use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use log::debug;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::sync::{Mutex, mpsc};

use crate::consensus::multi_paxos::PaxosMessage::Ack;
use crate::{
    broadcast::best_effort::BestEffortBroadcast,
    common::{NodeId, marshal, unmarshal},
    consensus::multi_paxos::PaxosMessage::{Accepted, Commit, Prepare},
    links::{LinkError, perfect_link::PerfectLink},
};

#[derive(Debug)]
pub struct Decided<T> {
    pub value: T,
}

#[derive(Clone, Serialize, Debug, Deserialize)]
struct OpenProposal<T> {
    id: u128,
    acks: HashSet<NodeId>,
    acceptances: HashSet<NodeId>,
    proposal: Proposal<T>,
}

#[derive(Clone, Serialize, Debug, Deserialize)]
struct Proposal<T> {
    pub slot: usize,
    pub value: T,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum PaxosMessage<T> {
    Prepare(u128),
    Ack {
        id: u128,
        decisions: HashMap<usize, T>,
    },
    Commit {
        id: u128,
        proposal: Proposal<T>,
    },
    Accepted {
        id: u128,
        proposal: Proposal<T>,
    },
}

struct State<T> {
    current_slot: usize,
    decisions: HashMap<usize, T>,
    open_decision: HashMap<usize, (T, usize)>,
    proposal: Option<OpenProposal<T>>,
    promised_id: Option<(NodeId, u128)>,
    id: u128,
}

pub struct MultiPaxos<T> {
    local_id: NodeId,
    number_of_nodes: usize,
    state: Mutex<State<T>>,
    indications: Arc<Mutex<Option<mpsc::Receiver<Decided<T>>>>>,
    indications_tx: Mutex<Option<mpsc::Sender<Decided<T>>>>,
    broadcast: Arc<dyn BestEffortBroadcast<Value>>,
    link: Arc<dyn PerfectLink<Value>>,
}

fn get_id(node_id: usize, number_of_nodes: usize) -> u128 {
    let ts = SystemTime::now();
    let duration = ts.duration_since(UNIX_EPOCH).expect("Time went backwards");
    let nano = duration.as_nanos();
    (nano / (number_of_nodes as u128)) * (number_of_nodes as u128) + (node_id as u128)
}

fn get_jitter() -> Duration {
    Duration::from_millis(rand::random_range(0..=1000))
}

impl<T> MultiPaxos<T>
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
                current_slot: 0,
                decisions: HashMap::new(),
                open_decision: HashMap::new(),
                promised_id: None,
                id: 0,
                proposal: None,
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
        let mut backoff = Duration::from_millis(3000);
        let slot = {
            let mut state = self.state.lock().await;
            state.current_slot += 1;
            if state.proposal.is_some() {
                return Err(LinkError::InternalError(
                    "Cannot propose with another active proposal".to_owned(),
                ));
            }
            state.current_slot
        };

        loop {
            let broadcast = {
                let mut state = self.state.lock().await;
                let proposal_id = get_id(self.number_of_nodes, self.local_id.get() as usize);
                state.id = proposal_id;
                state.proposal = Some(OpenProposal {
                    id: proposal_id,
                    acks: HashSet::new(),
                    acceptances: HashSet::new(),
                    proposal: Proposal {
                        slot: state.current_slot,
                        value: value.clone(),
                    },
                });
                Prepare::<T>(proposal_id)
            };

            debug!("proposing: {:?}", broadcast);
            self.broadcast.broadcast(&marshal(broadcast)?).await?;
            let time = backoff + get_jitter();
            debug!("sleeping: {:?}", time);
            tokio::time::sleep(time).await;

            backoff = backoff * 2;
            let state = self.state.lock().await;
            if state.proposal.is_none() || state.current_slot > slot {
                return Ok(());
            }
        }
    }

    pub async fn propose(self: Arc<Self>, value: T) -> Result<(), LinkError> {
        let broadcast = {
            let mut state = self.state.lock().await;
            if let Some((promised_to, _)) = state.promised_id
                && promised_to == self.local_id
            {
                state.current_slot += 1;
                Some(Commit {
                    id: state.id,
                    proposal: Proposal {
                        slot: state.current_slot,
                        value: value.clone(),
                    },
                })
            } else {
                None
            }
        };
        if let Some(message) = broadcast {
            debug!("sending commit directly (is_leader): {:?}", message);
            self.broadcast.broadcast(&marshal(message)?).await
        } else {
            //TODO: save and join handle
            tokio::spawn(async { self.do_propose(value).await });
            Ok(())
        }
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

    async fn handle_accepted(
        &self,
        sender: NodeId,
        tx: &mpsc::Sender<Decided<T>>,
        accepted: &PaxosMessage<T>,
    ) -> Result<(), LinkError> {
        if let Accepted { id, proposal } = accepted {
            let mut state = self.state.lock().await;
            if let Some(promised_id) = state.promised_id
                && promised_id.1 == *id
            {
                if state.decisions.contains_key(&proposal.slot) {
                    debug!(
                        "decision already settled. Ignoring accept from {sender}: {:?}",
                        accepted
                    );
                    return Ok(());
                }

                state.current_slot = proposal.slot;

                let entry = state
                    .open_decision
                    .entry(proposal.slot)
                    .or_insert((proposal.value.clone(), 0));

                entry.1 += 1;

                debug!(
                    "received accept ({}/{}) for slot {} from {sender}: {:?}",
                    entry.1, self.number_of_nodes, proposal.slot, accepted
                );

                if entry.1 > self.number_of_nodes / 2 {
                    debug!("committing id: {}", id);
                    state.proposal = None;
                    state
                        .decisions
                        .insert(proposal.slot, proposal.value.clone());
                    state.open_decision.remove(&proposal.slot);
                    tx.send(Decided {
                        value: proposal.value.clone(),
                    })
                    .await
                    .map_err(LinkError::with_action("send decided indication"))?
                }
            } else {
                debug!("wrong id. Ignoring accept from {sender}: {:?}.", accepted);
            }
        }
        Ok(())
    }

    async fn handle_commit(
        &self,
        sender: NodeId,
        commit: &PaxosMessage<T>,
    ) -> Result<(), LinkError> {
        if let Commit { id, proposal } = commit {
            let response = {
                let state = self.state.lock().await;
                match state.promised_id {
                    Some(x) if x.1 == *id => {}
                    _ => {
                        debug!(
                            "received commit with wrong id. expected {:?}, got {}",
                            state.promised_id, id
                        );
                        return Ok(());
                    }
                };

                if state.decisions.contains_key(&proposal.slot) {
                    debug!("received duplicate commit from {sender}: {:?}", proposal);
                    None
                } else {
                    debug!("received new commit: {:?} from {sender}", proposal);
                    Some(Accepted {
                        id: *id,
                        proposal: proposal.clone(),
                    })
                }
            };

            if let Some(response) = response {
                debug!("broadcasting: {:?}", response);
                self.broadcast.broadcast(&marshal(response)?).await?
            }
        };
        Ok(())
    }
    async fn handle_ack(&self, sender: NodeId, promise: &PaxosMessage<T>) -> Result<(), LinkError> {
        if let Ack { id, decisions } = promise {
            let mut acceptors: Option<HashSet<NodeId>> = None;
            let mut broadcast: Option<PaxosMessage<T>> = None;
            {
                let mut state = self.state.lock().await;
                {
                    if let Some(open_proposal) = state.proposal.as_mut()
                        && open_proposal.id == *id
                    {
                        open_proposal.acks.insert(sender);
                    } else {
                        return Ok(());
                    }
                };

                let open_proposal = state.proposal.as_ref().unwrap().clone();
                debug!("Replicating log from {}: {:?}", sender, decisions);
                for (slot, decision) in decisions.iter() {
                    state.decisions.insert(*slot, decision.clone());
                }

                debug!(
                    "acks {}/ {:?}",
                    open_proposal.acks.len(),
                    self.number_of_nodes
                );
                if open_proposal.acks.len() > self.number_of_nodes / 2 {
                    broadcast = Some(Commit {
                        id: *id,
                        proposal: open_proposal.proposal,
                    });
                    acceptors = Some(open_proposal.acks.clone());
                }
            }

            if let Some(mut acceptors) = acceptors {
                let broadcast = broadcast.unwrap();
                debug!("sending: {:?}", broadcast);
                let broadcast = marshal(broadcast)?;
                for id in acceptors.drain() {
                    self.link.send(&broadcast, id).await?;
                }
            }
        }
        Ok(())
    }

    async fn handle_prepare(
        &self,
        sender: NodeId,
        prepare: &PaxosMessage<T>,
    ) -> Result<(), LinkError> {
        if let Prepare(received_id) = prepare {
            let response: PaxosMessage<T> = {
                let mut state = self.state.lock().await;
                if let Some(promised_id) = &state.promised_id
                    && promised_id.1 >= *received_id
                {
                    debug!(
                        "ignoring: {} from {sender}. previous promise: {:?}",
                        received_id, state.promised_id
                    );
                    return Ok(());
                }

                state.promised_id = Some((sender, *received_id));
                debug!(
                    "promising {} to {sender}. previous promise: {:?}",
                    received_id, state.promised_id
                );

                PaxosMessage::Ack {
                    id: *received_id,
                    decisions: state.decisions.clone(),
                }
            };

            self.link.send(&marshal(response)?, sender).await?
        };
        Ok(())
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

            if matches!(message, Prepare { .. }) {
                self.handle_prepare(delivery.sender, &message).await?;
            } else if matches!(message, Accepted { .. }) {
                self.handle_accepted(delivery.sender, &tx, &message).await?;
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

            if matches!(message, Commit { .. }) {
                self.handle_commit(delivery.sender, &message).await?
            } else if matches!(message, Ack { .. }) {
                self.handle_ack(delivery.sender, &message).await?
            }
        }
        Ok(())
    }
}
