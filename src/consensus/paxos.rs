use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use log::debug;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::sync::{Mutex, broadcast, mpsc};

use crate::{
    broadcast::best_effort::BestEffortBroadcast,
    common::{NodeId, marshal, unmarshal},
    consensus::paxos::PaxosMessage::{Accepted, Commit, Prepare, Promise},
    links::{LinkError, perfect_link::PerfectLink},
};

#[derive(Debug)]
pub struct Decided<T> {
    value: T,
}

#[derive(Clone, Serialize, Debug, Deserialize)]
struct Proposal<T> {
    pub id: usize,
    pub value: T,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum PaxosMessage<T> {
    Prepare {
        id: usize,
    },
    Promise {
        id: usize,
        accepted: Option<Proposal<T>>,
    },
    Commit(Proposal<T>),
    Accepted(Proposal<T>),
}

struct State<T> {
    promise: Option<Proposal<Option<T>>>,
    proposal: Option<Proposal<T>>,
    acks: HashMap<NodeId, Option<Proposal<T>>>,
    acceptances: HashMap<usize, (usize, Proposal<T>)>,
    decided: Option<Proposal<T>>,
}

pub struct Paxos<T> {
    local_id: NodeId,
    number_of_nodes: usize,
    state: Mutex<State<T>>,
    indications: Arc<Mutex<Option<mpsc::Receiver<Decided<T>>>>>,
    indications_tx: Mutex<Option<mpsc::Sender<Decided<T>>>>,
    broadcast: Arc<dyn BestEffortBroadcast<Value>>,
    link: Arc<dyn PerfectLink<Value>>,
}

fn get_id(node_id: usize, number_of_nodes: usize) -> usize {
    let ts = SystemTime::now();
    let duration = ts.duration_since(UNIX_EPOCH).expect("Time went backwards");
    let nano = duration.as_nanos();
    ((nano as usize) / number_of_nodes) * number_of_nodes + node_id
}

impl<T> Paxos<T>
where
    T: std::fmt::Debug + Serialize + Clone + DeserializeOwned + Send + 'static,
{
    pub fn new(
        local_id: NodeId,
        number_of_nodes: usize,
        broadcast: Arc<dyn BestEffortBroadcast<Value>>,
        link: Arc<dyn PerfectLink<Value>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(32);
        Self {
            local_id,
            number_of_nodes,
            state: Mutex::from(State {
                decided: None,
                acceptances: HashMap::new(),
                proposal: None,
                promise: None,
                acks: HashMap::new(),
            }),
            indications: Arc::from(Mutex::from(Some(rx))),
            indications_tx: Mutex::from(Some(tx)),
            broadcast,
            link,
        }
    }

    pub async fn take_indications(&self) -> Option<mpsc::Receiver<Decided<T>>> {
        self.indications.lock().await.take()
    }

    async fn do_propose(self: Arc<Self>, value: T) -> Result<(), LinkError> {
        let mut backoff = Duration::from_millis(300);
        let get_jitter = || Duration::from_millis(rand::random_range(0..=1000));
        loop {
            let broadcast = {
                let mut state = self.state.lock().await;
                if state.acks.len() > self.number_of_nodes / 2 {
                    return Ok(());
                }
                state.acks.clear();

                let new_proposal_id = get_id(self.local_id.get() as usize, self.number_of_nodes);
                state.proposal = Some(Proposal {
                    id: new_proposal_id,
                    value: value.clone(),
                });

                Prepare::<T> {
                    id: new_proposal_id,
                }
            };

            debug!("proposing: {:?}", broadcast);
            self.broadcast.broadcast(&marshal(broadcast)?).await?;
            tokio::time::sleep(backoff + get_jitter()).await;
            backoff = backoff * 2;
        }
    }

    pub async fn propose(self: Arc<Self>, value: T) -> Result<(), LinkError> {
        tokio::spawn(async { self.do_propose(value).await });
        //todo: save and join handle
        Ok(())
    }

    pub async fn start(&self) -> Result<(), LinkError> {
        tokio::try_join!(
            self.link.listen(),
            self.broadcast.listen(),
            self.handle_messages(),
            self.handle_broadcast()
        )?;
        Ok(())
    }

    async fn on_accepted(
        &self,
        sender: NodeId,
        tx: &mpsc::Sender<Decided<T>>,
        id: usize,
        value: T,
    ) -> Result<(), LinkError> {
        let mut state = self.state.lock().await;
        let entry = state.acceptances.entry(id).or_insert((
            0,
            Proposal {
                id: id,
                value: value.clone(),
            },
        ));

        entry.0 += 1;
        let entry = (entry.0, entry.1.clone());

        debug!("received accept from {sender}. Entry: {:?}", entry);
        if let None = state.decided
            && entry.0 > self.number_of_nodes / 2
        {
            state.decided = Some(entry.1.clone());
            tx.send(Decided {
                value: entry.1.value,
            })
            .await
            .map_err(LinkError::with_action("send decided indication"))?
        }
        Ok(())
    }

    async fn on_commit(&self, sender: NodeId, id: usize, value: T) -> Result<(), LinkError> {
        let response = {
            let mut state = self.state.lock().await;
            if let Some(promise) = state.promise.as_mut()
                && promise.id == id
            {
                debug!(
                    "accepting: ({:?}, {:?}) from {sender}. Promised: {:?}",
                    id, value, promise
                );
                promise.value = Some(value);
                Some(Accepted(promise.clone()))
            } else {
                debug!(
                    "ignoring: {:?} from {sender}. Promised: {:?}",
                    id, state.promise
                );
                None
            }
        };

        if let Some(response) = response {
            self.broadcast.broadcast(&marshal(response)?).await?
        }
        Ok(())
    }
    async fn on_promise(
        &self,
        sender: NodeId,
        id: usize,
        accepted: Option<Proposal<T>>,
    ) -> Result<(), LinkError> {
        {
            let mut state = self.state.lock().await;
            if let Some(proposal) = state.proposal.as_mut()
                && id == proposal.id
            {
                state.acks.insert(sender, accepted);
            } else {
                return Ok(());
            };
        }
        let proposal;
        let mut acceptors: HashSet<NodeId>;

        let value = {
            let state = self.state.lock().await;
            if state.acks.len() <= self.number_of_nodes / 2 {
                return Ok(());
            }

            proposal = state.proposal.clone();
            acceptors = state.acks.keys().copied().collect();
            state
                .acks
                .iter()
                .filter(|(_, p)| p.is_some())
                .max_by_key(|(_, p)| p.as_ref().clone().unwrap().id)
                .map(|(_, p)| p.clone().unwrap().value)
                .unwrap_or(proposal.unwrap().value)
        };

        let commit = marshal(&Commit(Proposal { id, value }))?;
        debug!(
            "received {} promises. sending: {:?}",
            acceptors.len(),
            commit
        );
        for id in acceptors.drain() {
            self.link.send(&commit, id).await?;
        }
        Ok(())
    }

    async fn on_prepare(&self, sender: NodeId, id: usize) -> Result<(), LinkError> {
        let promise: PaxosMessage<T> = {
            let mut state = self.state.lock().await;
            if let Some(promise) = state.promise.as_mut() {
                if promise.id > id {
                    debug!(
                        "ignoring: {:?} from {sender}. Promised: {id}",
                        state.promise
                    );
                    return Ok(());
                }

                let accepted = match &promise.value {
                    Some(v) => Some(Proposal {
                        id: promise.id,
                        value: v.clone(),
                    }),
                    None => None,
                };

                promise.id = id;
                Promise { id, accepted }
            } else {
                state.promise = Some(Proposal { id, value: None });
                Promise { id, accepted: None }
            }
        };

        debug!("promising: {:?} to {sender}. Promised: {id}", promise);
        let response = marshal(&promise)?;
        self.link.send(&response, sender).await
    }

    async fn handle_broadcast(&self) -> Result<(), LinkError> {
        let mut indications = match self.broadcast.take_indications().await {
            None => return Err(LinkError::InternalError("take bc indications".to_owned())),
            Some(rx) => rx,
        };

        let tx = match self.indications_tx.lock().await.take() {
            Some(tx) => tx,
            None => return Err(LinkError::InternalError("take indications tx".to_owned())),
        };

        while let Some(delivery) = indications.recv().await {
            let message: PaxosMessage<T> = match unmarshal(delivery.message) {
                Ok(m) => m,
                Err(e) => {
                    debug!("error unmarshaling message: {:?}", e);
                    continue;
                }
            };

            if let Prepare { id } = message {
                self.on_prepare(delivery.sender, id).await?;
            } else if let Accepted(proposal) = message {
                self.on_accepted(delivery.sender, &tx, proposal.id, proposal.value)
                    .await?;
            }
        }
        Ok(())
    }

    async fn handle_messages(&self) -> Result<(), LinkError> {
        let mut indications = match self.link.take_indications().await {
            None => return Err(LinkError::InternalError("take link indications".to_owned())),
            Some(rx) => rx,
        };

        while let Some(delivery) = indications.recv().await {
            let message: PaxosMessage<T> = match unmarshal(delivery.message) {
                Ok(m) => m,
                Err(e) => {
                    debug!("error unmarshaling message: {:?}", e);
                    continue;
                }
            };

            if let Commit(Proposal { id, value }) = message {
                self.on_commit(delivery.sender, id, value).await?
            } else if let Promise { id, accepted } = message {
                self.on_promise(delivery.sender, id, accepted).await?
            }
        }
        Ok(())
    }
}
