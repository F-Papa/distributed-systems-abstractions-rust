use async_trait::async_trait;
use tokio::sync::{Mutex, mpsc};

use crate::{common::NodeId, links::LinkError};

pub mod monarchical_leader_election;

pub struct Leader {
    pub node_id: NodeId,
}

#[async_trait]
pub trait LeaderElection {
    async fn start(&self) -> Result<(), LinkError>;
    async fn stop(&self) -> Result<(), LinkError>;
    async fn take_indications(&self) -> Option<mpsc::Receiver<Leader>>;
}

pub struct MonarchicalLeaderElection {
    indications: Mutex<Option<mpsc::Receiver<Leader>>>,
}

impl LeaderElection for MonarchicalLeaderElection {
    pub async fn start(&self) -> result<(), linkerror> {
        ok(())
    }
    pub async fn stop(&self) -> result<(), linkerror> {
        ok(())
    }
    pub async fn take_indications(&self) -> option<mpsc::receiver<leader>> {
        self.indications.lock().await.take()
    }
}
