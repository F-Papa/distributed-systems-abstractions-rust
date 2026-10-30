use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use log::debug;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, mpsc};

use crate::{
    common::NodeId,
    leader_election::Leader,
    links::{
        LinkError::{self, InternalError, LinkClosed},
        auth_perfect_link::AuthenticatedPerfectLink,
    },
};

pub struct Trust {
    pub node_id: NodeId,
}

#[derive(Debug, Serialize, Deserialize)]
enum Protocol {
    RotatingElection,
}

#[derive(Debug, Serialize, Deserialize)]
struct Envelope {
    protocol: Protocol,
    message: Complaint,
}

#[derive(Serialize, Deserialize, Debug)]
struct Complaint(usize);

#[async_trait]
pub trait ByzantineLeaderElection {
    async fn start(&self) -> Result<(), LinkError>;
    async fn stop(&self);
    async fn take_indications(&self) -> Option<mpsc::Receiver<Leader>>;
    async fn complain(&self) -> Result<(), LinkError>;
}

struct State {
    complained: bool,
    round: usize,
}

pub struct RotatingElection {
    state: Mutex<State>,
    link: Arc<dyn AuthenticatedPerfectLink<Value>>,
    max_faulty_nodes: usize,
    indications_tx: Mutex<Option<mpsc::Sender<Leader>>>,
    indications: Arc<Mutex<Option<mpsc::Receiver<Leader>>>>,
    peers: HashSet<NodeId>,
    nodes: Vec<NodeId>,
}

#[async_trait]
impl ByzantineLeaderElection for RotatingElection {
    async fn start(&self) -> Result<(), LinkError> {
        println!("{:?}", self.nodes);
        tokio::try_join!(self.handle_link_indications(), self.link.listen())?;
        Ok(())
    }
    async fn stop(&self) {
        self.link.close().await
    }
    async fn take_indications(&self) -> Option<mpsc::Receiver<Leader>> {
        self.indications.lock().await.take()
    }
    async fn complain(&self) -> Result<(), LinkError> {
        let mut state = self.state.lock().await;
        state.complained = true;
        let complaint = Complaint(state.round);
        let envelope = Envelope {
            protocol: Protocol::RotatingElection,
            message: complaint,
        };

        let value = serde_json::to_value(&envelope).map_err(|e| InternalError(e.to_string()))?;
        for p in self.peers.iter() {
            self.link.send(&value, *p).await?;
        }

        Ok(())
    }
}

impl RotatingElection {
    pub fn new(
        peers: HashSet<NodeId>,
        nodes: Vec<NodeId>,
        link: Arc<dyn AuthenticatedPerfectLink<Value>>,
        max_faulty_nodes: usize,
    ) -> Self {
        let (indications_tx, indications) = mpsc::channel(32);

        Self {
            link,
            peers,
            nodes,
            max_faulty_nodes,
            state: Mutex::from(State {
                round: 1,
                complained: false,
            }),
            indications: Arc::from(Mutex::from(Some(indications))),
            indications_tx: Mutex::from(Some(indications_tx)),
        }
    }

    fn round_leader(&self, state: &State) -> NodeId {
        let idx = state.round % self.nodes.len();
        *self.nodes.get(idx).unwrap()
    }

    async fn handle_link_indications(&self) -> Result<(), LinkError> {
        let indications_tx = match self.indications_tx.lock().await.take() {
            None => return Err(LinkClosed()),
            Some(i) => i,
        };

        let mut link_indications = match self.link.take_indications().await {
            None => return Err(LinkClosed()),
            Some(i) => i,
        };

        let mut complain_list: HashSet<NodeId> = HashSet::new();
        while let Some(indication) = link_indications.recv().await {
            let envelope: Envelope = match serde_json::from_value(indication.message) {
                Ok(c) => c,
                Err(e) => {
                    debug!("could not deserialize complaint: {}", e);
                    continue;
                }
            };

            debug!(
                "complaint received from node {} for round {}",
                indication.sender, envelope.message.0
            );
            let mut state = self.state.lock().await;
            if state.round != envelope.message.0 || complain_list.contains(&indication.sender) {
                continue;
            }

            complain_list.insert(indication.sender);

            if complain_list.len() > self.max_faulty_nodes && !state.complained {
                debug!("Joining complaints");
                self.complain().await?;
            } else if complain_list.len() > 2 * self.max_faulty_nodes {
                // len > 2f: the previous condition doesn't guarantee that every node will complain
                // (especially if most complainers are byzantine). Once len > 2f, then it will
                // eventually propagate.
                state.round += 1;
                complain_list.clear();
                state.complained = false;
                let leader = self.round_leader(&state);
                indications_tx
                    .send(Leader { node_id: leader })
                    .await
                    .map_err(|e| InternalError(e.to_string()))?;
            }
        }

        Ok(())
    }
}
