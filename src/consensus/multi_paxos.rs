use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use log::debug;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::sync::{Mutex, mpsc};

use crate::consensus::multi_paxos::PaxosMessage::{Accepted, Ack, Commit, Prepare};
use crate::links::LinkError::InternalError;
use crate::{
    broadcast::best_effort::BestEffortBroadcast,
    common::{NodeId, marshal, unmarshal},
    links::{LinkError, perfect_link::PerfectLink},
};

#[derive(Debug)]
pub struct Decided<T> {
    pub value: T,
}

#[derive(Clone, Serialize, Debug, Deserialize)]
struct Proposal<T> {
    id: u128,
    acks: HashSet<NodeId>,
    acceptances: HashSet<NodeId>,
    slot_value: SlotValue<T>,
}

#[derive(Clone, Serialize, Debug, Deserialize)]
struct SlotValue<T> {
    pub slot: usize,
    pub value: T,
}

struct LearnedValue<T> {
    pub value: Arc<T>,
    pub accepted_by: HashSet<NodeId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum PaxosMessage<T> {
    Prepare {
        id: u128,
    },
    Ack {
        id: u128,
        decisions: HashMap<usize, T>,
    },
    Commit {
        id: u128,
        proposal: SlotValue<T>,
    },
    Accepted {
        id: u128,
        proposal: SlotValue<T>,
    },
}

struct State<T> {
    current_slot: usize,
    sent_proposal: Option<Proposal<Arc<T>>>,
    open_decision: HashMap<usize, LearnedValue<T>>,
    settled_decisions: HashMap<usize, Arc<T>>,
    promised_id: Option<(NodeId, u128)>,
    proposed_id: u128,
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
                settled_decisions: HashMap::new(),
                open_decision: HashMap::new(),
                promised_id: None,
                proposed_id: 0,
                sent_proposal: None,
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

    async fn new_proposal(&self, value: Arc<T>, state: &mut State<T>) -> PaxosMessage<Arc<T>> {
        let new_id = get_id(self.number_of_nodes, self.local_id.get() as usize);
        state.proposed_id = new_id;
        state.sent_proposal = Some(Proposal {
            id: new_id,
            acks: HashSet::new(),
            acceptances: HashSet::new(),
            slot_value: SlotValue {
                slot: state.current_slot,
                value,
            },
        });
        Prepare { id: new_id }
    }

    pub async fn propose_retry(&self, value: Arc<T>) -> Result<(), LinkError> {
        let mut backoff = Duration::from_millis(3000);
        loop {
            tokio::time::sleep(backoff + get_jitter()).await;
            let broadcast = {
                let mut state = self.state.lock().await;
                let sent_proposal = state.sent_proposal.as_ref();

                if sent_proposal.is_none() {
                    debug!("Proposal is none. Cancelling retry");
                    state.sent_proposal = None;
                    return Ok(());
                }
                self.new_proposal(Arc::clone(&value), &mut state).await
            };
            self.broadcast.broadcast(&marshal(broadcast)?).await?;
            backoff = backoff * 2;
        }
    }

    pub async fn propose(&self, value: T) -> Result<(), LinkError> {
        let value = Arc::from(value);

        let broadcast: PaxosMessage<Arc<T>> = {
            let mut state = self.state.lock().await;

            if state.sent_proposal.is_some() {
                return Err(InternalError("proposal is still open".to_owned()));
            }

            state.current_slot += 1;

            if let Some((promised_to, id)) = state.promised_id
                && promised_to == self.local_id
            {
                // Node is already leader
                let message = Commit {
                    id: state.proposed_id,
                    proposal: SlotValue {
                        slot: state.current_slot,
                        value: Arc::clone(&value),
                    },
                };

                let new_proposal = Proposal {
                    id,
                    acks: HashSet::new(),
                    acceptances: HashSet::new(),
                    slot_value: SlotValue {
                        slot: state.current_slot,
                        value: Arc::clone(&value),
                    },
                };

                state.sent_proposal = Some(new_proposal);
                debug!("sending commit directly (is_leader): {:?}", message);
                message
            } else {
                // Node needs to become leader
                let prepare = self.new_proposal(Arc::clone(&value), &mut state).await;
                debug!("proposing {:?}", state.sent_proposal);
                prepare
            }
        };

        self.broadcast.broadcast(&marshal(broadcast)?).await?;
        self.propose_retry(value).await
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
        accepted_id: u128,
        accepted_proposal: SlotValue<T>,
    ) -> Result<(), LinkError> {
        let mut state = self.state.lock().await;
        if let Some(promised_id) = state.promised_id
            && promised_id.1 == accepted_id
        {
            if state
                .settled_decisions
                .contains_key(&accepted_proposal.slot)
            {
                debug!(
                    "decision already settled. Ignoring accept from {sender}: {:?}",
                    accepted_proposal
                );
                return Ok(());
            }

            let value = Arc::from(accepted_proposal.value);
            let slot = accepted_proposal.slot;

            state.current_slot = accepted_proposal.slot;

            let committed_value =
                state
                    .open_decision
                    .entry(accepted_proposal.slot)
                    .or_insert(LearnedValue {
                        value: Arc::clone(&value),
                        accepted_by: HashSet::new(),
                    });

            committed_value.accepted_by.insert(sender);

            debug!(
                "received accept ({}/{}) from {sender}: {:?}",
                committed_value.accepted_by.len(),
                self.number_of_nodes,
                (slot, &value)
            );

            if committed_value.accepted_by.len() > self.number_of_nodes / 2 {
                let settled_decision = state.open_decision.remove(&accepted_proposal.slot).unwrap();
                debug!("committing id: {}", accepted_id);
                state.sent_proposal = None;
                state
                    .settled_decisions
                    .insert(accepted_proposal.slot, Arc::from(settled_decision.value));

                state.open_decision.remove(&accepted_proposal.slot);
                tx.send(Decided {
                    value: (*value).clone(),
                })
                .await
                .map_err(LinkError::with_action("send decided indication"))?
            }
        } else {
            debug!(
                "wrong id. Ignoring accept from {sender}: {:?}.",
                accepted_proposal
            );
        }
        Ok(())
    }

    async fn on_commit(
        &self,
        sender: NodeId,
        committed_id: u128,
        committed_value: SlotValue<T>,
    ) -> Result<(), LinkError> {
        let response = {
            let state = self.state.lock().await;
            match state.promised_id {
                Some(x) if x.1 == committed_id => {}
                _ => {
                    debug!(
                        "received commit with wrong id. expected {:?}, got {}",
                        state.promised_id, committed_id
                    );
                    return Ok(());
                }
            };

            if state.settled_decisions.contains_key(&committed_value.slot) {
                debug!(
                    "received duplicate commit from {sender}: {:?}",
                    committed_value
                );
                None
            } else {
                debug!("received new commit: {:?} from {sender}", committed_value);
                Some(Accepted {
                    id: committed_id,
                    proposal: committed_value.clone(),
                })
            }
        };

        if let Some(response) = response {
            debug!("broadcasting: {:?}", response);
            self.broadcast.broadcast(&marshal(response)?).await?
        }
        Ok(())
    }
    async fn on_ack(
        &self,
        sender: NodeId,
        ack_id: u128,
        settled_decisions: HashMap<usize, T>,
    ) -> Result<(), LinkError> {
        let mut acceptors: Option<HashSet<NodeId>> = None;
        let mut broadcast: Option<Value> = None;
        {
            let mut state = self.state.lock().await;

            if state.sent_proposal.is_none() {
                return Ok(());
            }

            let learned_decisions: Vec<(usize, T)> = settled_decisions
                .into_iter()
                .filter(|(s, _)| !state.settled_decisions.contains_key(&s))
                .collect();

            debug!(
                "Acks from {sender}. learned_decisions: {:?}",
                learned_decisions
            );

            if let Some(last_slot) = learned_decisions.iter().map(|(s, _)| *s).max() {
                if state.current_slot < last_slot {
                    debug!("cancelling old proposal and moving to {last_slot}");
                    state.current_slot = last_slot;
                    state.sent_proposal = None;
                    return Ok(());
                }
            }

            for (slot, decision) in learned_decisions.into_iter() {
                state.settled_decisions.insert(slot, Arc::from(decision));
            }

            let open_proposal = state.sent_proposal.as_mut().unwrap();
            open_proposal.acks.insert(sender);

            if open_proposal.acks.len() > self.number_of_nodes / 2 {
                let proposal_ref = Commit {
                    id: ack_id,
                    proposal: SlotValue {
                        slot: open_proposal.slot_value.slot,
                        value: &open_proposal.slot_value.value,
                    },
                };
                broadcast = Some(marshal(proposal_ref)?);
                acceptors = Some(open_proposal.acks.clone());
            }
        }

        if let Some(mut acceptors) = acceptors {
            let broadcast = broadcast.unwrap();
            debug!("sending: {:?}", broadcast);
            for id in acceptors.drain() {
                self.link.send(&broadcast, id).await?;
            }
        }
        Ok(())
    }

    async fn on_prepare(&self, sender: NodeId, received_id: u128) -> Result<(), LinkError> {
        let response = {
            let mut state = self.state.lock().await;
            if let Some(promised_id) = &state.promised_id
                && promised_id.1 >= received_id
            {
                debug!(
                    "ignoring: {} from {sender}. previous promise: {:?}",
                    received_id, state.promised_id
                );
                return Ok(());
            }

            state.promised_id = Some((sender, received_id));
            debug!(
                "promising {} to {sender}. previous promise: {:?}",
                received_id, state.promised_id
            );

            let ack = PaxosMessage::<Arc<T>>::Ack {
                id: received_id,
                decisions: state.settled_decisions.clone(),
            };
            marshal(ack)?
        };

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
            } else if let Accepted { id, proposal } = message {
                self.on_accepted(delivery.sender, &tx, id, proposal).await?;
            } else if let Commit { id, proposal } = message {
                self.on_commit(delivery.sender, id, proposal).await?
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

            if let Commit { id, proposal } = message {
                self.on_commit(delivery.sender, id, proposal).await?
            } else if let Ack { id, decisions } = message {
                self.on_ack(delivery.sender, id, decisions).await?
            }
        }
        Ok(())
    }
}
