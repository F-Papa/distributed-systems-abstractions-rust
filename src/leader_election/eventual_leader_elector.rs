use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::{Mutex, mpsc};

use crate::{
    common::NodeId,
    failure_detector::{
        eventually_perfect_failure_detector::{
            EpfdMessageIndication, EventuallyPerfectFailureDetector,
        },
    },
    leader_election::{Leader, LeaderElection},
    links::LinkError::{self, InternalError},
};

#[async_trait]
pub trait EventualLeaderElection {
    async fn start(&self) -> Result<(), LinkError>;
    async fn stop(&self) -> Result<(), LinkError>;
    async fn take_indications(&self) -> Option<mpsc::Receiver<Leader>>;
}

pub struct MonarchicalEventualLeaderElector {
    nodes: HashSet<NodeId>,
    failure_detector: Arc<dyn EventuallyPerfectFailureDetector>,
    indications: Mutex<Option<mpsc::Receiver<Leader>>>,
    indications_tx: Arc<Mutex<Option<mpsc::Sender<Leader>>>>,
}

impl MonarchicalEventualLeaderElector {
    pub fn new(
        nodes: HashSet<NodeId>,
        failure_detector: Arc<dyn EventuallyPerfectFailureDetector>,
    ) -> Self {
        let (indications_tx, indications) = mpsc::channel(32);
        Self {
            nodes,
            failure_detector,
            indications: Mutex::from(Some(indications)),
            indications_tx: Arc::from(Mutex::from(Some(indications_tx))),
        }
    }

    async fn do_election(
        &self,
        old_leader: Option<NodeId>,
        indications_tx: &mpsc::Sender<Leader>,
        suspected: &HashSet<NodeId>,
    ) -> Result<NodeId, LinkError> {
        let new_leader = self.get_highest_alive_rank(suspected)?;
        if old_leader != Some(new_leader) {
            indications_tx
                .send(Leader {
                    node_id: new_leader,
                })
                .await
                .map_err(|e| InternalError(e.to_string()))?;
        }
        Ok(new_leader)
    }

    async fn handle_epfd_indications(&self) -> Result<(), LinkError> {
        let indications_tx = match self.indications_tx.lock().await.take() {
            None => return Err(LinkError::LinkClosed()),
            Some(i) => i,
        };

        if let Some(mut epfd_indications) = self.failure_detector.take_indications().await {
            let mut suspected: HashSet<NodeId> = HashSet::new();
            let mut leader = self.do_election(None, &indications_tx, &suspected).await?;
            while let Some(indication) = epfd_indications.recv().await {
                match indication {
                    EpfdMessageIndication::Suspect(node_id) => {
                        suspected.insert(node_id);
                    }
                    EpfdMessageIndication::Restore(node_id) => {
                        suspected.remove(&node_id);
                    }
                }

                leader = self
                    .do_election(Some(leader), &indications_tx, &suspected)
                    .await?;
            }
        }

        Ok(())
    }

    fn get_highest_alive_rank(&self, suspected: &HashSet<NodeId>) -> Result<NodeId, LinkError> {
        let highest_id = self
            .nodes
            .iter()
            .filter(|id| !suspected.contains(id))
            .reduce(|a, b| if a.get() > b.get() { a } else { b }).copied();

        match highest_id {
            Some(id) => Ok(id),
            None => Err(InternalError("No leader could be elected".to_owned())),
        }
    }
}

#[async_trait]
impl EventualLeaderElection for MonarchicalEventualLeaderElector {
    async fn start(&self) -> Result<(), LinkError> {
        tokio::try_join!(
            self.failure_detector.start(),
            self.handle_epfd_indications()
        )?;
        Ok(())
    }
    async fn stop(&self) -> Result<(), LinkError> {
        self.failure_detector.stop().await
    }
    async fn take_indications(&self) -> Option<mpsc::Receiver<Leader>> {
        self.indications.lock().await.take()
    }
}
