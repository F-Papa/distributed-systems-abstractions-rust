use async_trait::async_trait;

use std::sync::Arc;

use tokio::sync::{Mutex, mpsc};

use crate::links::{LinkError, NodeId, stubborn_link::SblDeliver};

mod eliminate_duplicates;
pub use eliminate_duplicates::EliminateDuplicates;

mod acknowledge_messages;
pub use acknowledge_messages::AcknowledgeMessages;

pub type PlDeliver = SblDeliver;

#[async_trait]
pub trait PerfectLink<T>: Send + Sync {
    async fn send(&self, payload: T, recipient: NodeId) -> Result<(), LinkError>;
    async fn listen(&self) -> Result<(), LinkError>;
    async fn close(&self);
    fn get_indications(&self) -> Arc<Mutex<mpsc::Receiver<PlDeliver>>>;
}
