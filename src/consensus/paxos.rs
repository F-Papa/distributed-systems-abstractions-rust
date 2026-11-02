use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use log::debug;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, broadcast, mpsc};

use crate::{
    broadcast::best_effort::BestEffortBroadcast,
    common::{NodeId, marshal, unmarshal},
    consensus::paxos::PaxosMessage::{Accepted, Commit, Prepare, Promise},
    links::{LinkError, perfect_link::PerfectLink},
};

pub struct Decided<T> {
    value: T,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum PaxosMessage<T> {
    Prepare { id: usize },
    Promise { id: usize, accepted: Option<T> },
    Commit { id: usize, value: T },
    Accepted { id: usize, value: T },
}

impl<T> PaxosMessage<T>
where
    T: Clone,
{
    pub fn promised_value(&self) -> Option<T> {
        match self {
            Promise { id, accepted } => accepted.clone(),
            _ => None,
        }
    }
}

struct Proposal<T> {
    is: usize,
    value: T
}

struct State<T> {
    promise: Option<PaxosMessage<T>>,
    proposal: Option<(usize, T)>,
    acks: HashSet<NodeId>,
}

pub struct Paxos<T> {
    number_of_nodes: usize,
    state: Mutex<State<T>>,
    indications: Arc<Mutex<Option<mpsc::Receiver<Decided<T>>>>>,
    indications_tx: Mutex<Option<mpsc::Sender<Decided<T>>>>,
    broadcast: Arc<dyn BestEffortBroadcast<Value>>,
    link: Arc<dyn PerfectLink<Value>>,
}

impl<T> Paxos<T>
where
    T: std::fmt::Debug + Serialize + Clone,
{
    async fn handle_accepted(
        &self,
        sender: NodeId,
        commit: &PaxosMessage<T>,
    ) -> Result<(), LinkError> {
        if let Accepted { id, value } = commit {}
        Ok(())
    }
    async fn handle_commit(
        &self,
        sender: NodeId,
        commit: &PaxosMessage<T>,
    ) -> Result<(), LinkError> {
        if let Commit { id, value } = commit {
            let response = {
                let state = self.state.lock().await;
                match state.promise.as_ref() {
                    Some(Promise {
                        id: promised_id,
                        accepted,
                    }) if promised_id == id => {
                        debug!(
                            "accepting: {:?} from {sender}. Promised: {:?}",
                            commit, state.promise
                        );
                        Some(marshal(&Accepted { id: *id, value })?)
                    }
                    _ => {
                        debug!(
                            "ignoring: {:?} from {sender}. Promised: {:?}",
                            commit, state.promise
                        );
                        None
                    }
                }
            };

            if let Some(response) = response {
                self.broadcast.broadcast(&response).await?
            }
        };
        Ok(())
    }
    async fn handle_promise(
        &self,
        sender: NodeId,
        prepare: &PaxosMessage<T>,
    ) -> Result<(), LinkError> {
        if let Promise { id, accepted } = prepare {
            let mut acceptors: Option<&HashSet<NodeId>> = None;
            let mut broadcast: Option<PaxosMessage<T>> = None;
            {
                let mut state = self.state.lock().await;
                if let Some(proposal) = &state.proposal
                    && *id == proposal.0
                {
                    let value = accepted.as_ref().unwrap_or(&proposal.1).clone();
                    state.proposal = Some((*id, value));
                    state.acks.insert(sender);
                    if state.acks.len() > self.number_of_nodes / 2 {
                        broadcast = Some(Commit {
                            id: *id,
                            value: (&(state.proposal).as_ref().unwrap().1).clone(),
                        });
                    }

                    acceptors = Some(&state.acks);
                };
            }

            if let Some(acceptors) = acceptors {
                debug!(
                    "received {} promises. sending: {:?}",
                    acceptors.len(),
                    broadcast.unwrap()
                );
                let broadcast = marshal(broadcast.unwrap())?;
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
        if let Prepare { id } = prepare {
            let response: Option<&PaxosMessage<T>> = {
                let mut state = self.state.lock().await;
                match state.promise {
                    Some(Promise {
                        id: promised_id,
                        accepted,
                    }) if promised_id > *id => {
                        // Promised higher id, ignore
                        debug!(
                            "ignoring: {:?} from {sender}. Promised: {:?}",
                            prepare, state.promise
                        );
                        None
                    }
                    _ => {
                        // Save promise
                        debug!(
                            "promising: {:?} to {sender}. Promised: {:?}",
                            prepare, state.promise
                        );
                        state.promise = Some(Promise {
                            id: *id,
                            accepted: state.promise.map(PaxosMessage::promised_value),
                        });
                        state.promise.as_ref()
                    }
                }
            };

            if let Some(response) = response {
                self.link.send(&marshal(response)?, sender).await?
            }
        };
        Ok(())
    }

    async fn handle_broadcast(&self) -> Result<(), LinkError> {
        let mut indications = match self.link.take_indications().await {
            None => return Err(LinkError::LinkClosed()),
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

            if matches!(message, Prepare { .. }) {
                self.handle_prepare(delivery.sender, &message).await?;
            }
        }
        Ok(())
    }

    async fn handle_messages(&self) -> Result<(), LinkError> {
        let mut indications = match self.link.take_indications().await {
            None => return Err(LinkError::LinkClosed()),
            Some(rx) => rx,
        };

        let tx = match self.indications_tx.lock().await.take() {
            Some(tx) => tx,
            None => return Err(LinkError::LinkClosed()),
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
            } else if matches!(message, Promise { .. }) {
                self.handle_promise(delivery.sender, &message).await?
            }
        }
        Ok(())
    }
}
