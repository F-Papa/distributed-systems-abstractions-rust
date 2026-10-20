use std::sync::Arc;

use tokio::sync::{Mutex, mpsc};

use crate::{
    common::BoxFuture,
    links::{LinkError, NodeId, stubborn_link::SblDeliver},
};

mod eliminate_duplicates;
pub use eliminate_duplicates::EliminateDuplicates;

mod acknowledge_messages;
pub use acknowledge_messages::AcknowledgeMessages;

pub type PlDeliver = SblDeliver;

pub trait PerfectLink<T>: Send + Sync {
    fn send(&self, payload: T, recipient: NodeId) -> BoxFuture<'_, Result<(), LinkError>>;
    fn listen(&self) -> BoxFuture<'_, Result<(), LinkError>>;
    fn close(&self) -> BoxFuture<'_, ()>;
    fn get_indications(&self) -> Arc<Mutex<mpsc::Receiver<PlDeliver>>>;
}
