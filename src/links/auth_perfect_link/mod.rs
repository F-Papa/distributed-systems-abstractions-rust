use async_trait::async_trait;

use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use uuid::Uuid;

use crate::links::stubborn_link::SblDeliver;
use crate::links::{LinkError, NodeId};

pub type AplDeliver = SblDeliver;

mod authenticate_and_filter;
pub use authenticate_and_filter::AuthenticateAndFilter;

mod sign_and_acknowledge;
pub use sign_and_acknowledge::SignAndAcknowledge;

#[async_trait]
pub trait AuthenticatedPerfectLink<T>: Send + Sync {
    async fn send(&self, payload: T, recipient: NodeId) -> Result<(), LinkError>;
    async fn listen(&self) -> Result<(), LinkError>;
    async fn close(&self);
    fn get_indications(&self) -> Arc<Mutex<mpsc::Receiver<AplDeliver>>>;
}

pub(crate) fn bytes_to_sign(id: &Uuid, recipient: NodeId, payload: &[u8]) -> Vec<u8> {
    let id_bytes = id.as_bytes();
    let mut buf = Vec::with_capacity(id_bytes.len() + payload.len() + NodeId::ENCODED_LEN);

    buf.extend_from_slice(&recipient.to_be_bytes()[..]);
    buf.extend_from_slice(id_bytes);
    buf.extend_from_slice(payload);
    buf
}
