use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use tokio::sync::{Mutex, mpsc};

pub mod byzantine_leader_election;
pub mod eventual_leader_elector;

use crate::{
    common::NodeId,
    failure_detector::perfect_failure_detector::PerfectFailureDetector,
    links::LinkError::{self, InternalError},
};

pub struct Leader {
    pub node_id: NodeId,
}

#[async_trait]
pub trait LeaderElection: Send + Sync {
    async fn start(&self) -> Result<(), LinkError>;
    async fn stop(&self) -> Result<(), LinkError>;
    async fn take_indications(&self) -> Option<mpsc::Receiver<Leader>>;
}

pub struct MonarchicalLeaderElection {
    nodes: HashSet<NodeId>,
    failure_detector: Arc<dyn PerfectFailureDetector>,
    indications: Mutex<Option<mpsc::Receiver<Leader>>>,
    indications_tx: Arc<Mutex<Option<mpsc::Sender<Leader>>>>,
}

#[async_trait]
impl LeaderElection for MonarchicalLeaderElection {
    async fn start(&self) -> Result<(), LinkError> {
        tokio::try_join!(self.handle_fd_indications(), self.failure_detector.start())?;
        Ok(())
    }
    async fn stop(&self) -> Result<(), LinkError> {
        self.failure_detector.stop().await
    }
    async fn take_indications(&self) -> Option<mpsc::Receiver<Leader>> {
        self.indications.lock().await.take()
    }
}

impl MonarchicalLeaderElection {
    pub fn new(nodes: HashSet<NodeId>, failure_detector: Arc<dyn PerfectFailureDetector>) -> Self {
        let (indications_tx, indications) = mpsc::channel(32);
        MonarchicalLeaderElection {
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

    async fn handle_fd_indications(&self) -> Result<(), LinkError> {
        let mut suspected = HashSet::new();
        let indications_tx = match self.indications_tx.lock().await.take() {
            Some(i) => i,
            None => return Err(LinkError::LinkClosed()),
        };

        if let Some(mut indications) = self.failure_detector.take_indications().await {
            let mut leader = self.do_election(None, &indications_tx, &suspected).await?;
            while let Some(crash) = indications.recv().await {
                suspected.insert(crash.node_id);
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
            .reduce(|a, b| if a.get() > b.get() { a } else { b })
            .copied();

        match highest_id {
            Some(id) => Ok(id),
            None => Err(InternalError("No leader could be elected".to_owned())),
        }
    }
}
