use async_trait::async_trait;

use tokio::sync::mpsc;

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
    /// Transfers the receiver to its consumer. Returns `None` after the first take.
    async fn take_indications(&self) -> Option<mpsc::Receiver<PlDeliver>>;
}
