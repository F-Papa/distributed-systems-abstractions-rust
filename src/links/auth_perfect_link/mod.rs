use async_trait::async_trait;

use serde::Serialize;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::links::stubborn_link::SblDeliver;
use crate::links::{LinkError, NodeId};

pub type AplDeliver = SblDeliver;

mod authenticate_and_filter;
pub use authenticate_and_filter::AuthenticateAndFilter;

mod sign_and_acknowledge;
pub use sign_and_acknowledge::SignAndAcknowledge;

#[async_trait]
pub trait AuthenticatedPerfectLink<T>: Send + Sync
where
    T: Sync + Serialize,
{
    async fn send<'a>(&'a self, payload: &'a T, recipient: NodeId) -> Result<(), LinkError>;
    async fn listen(&self) -> Result<(), LinkError>;
    async fn close(&self);
    /// Transfers the receiver to its consumer. Returns `None` after the first take.
    async fn take_indications(&self) -> Option<mpsc::Receiver<AplDeliver>>;
}

pub(crate) fn bytes_to_sign(
    protocol: &str,
    id: &Uuid,
    recipient: NodeId,
    payload: &[u8],
) -> Vec<u8> {
    let id_bytes = id.as_bytes();
    let mut buf = Vec::with_capacity(
        protocol.len() + 1 + id_bytes.len() + payload.len() + NodeId::ENCODED_LEN,
    );

    buf.extend_from_slice(protocol.as_bytes());
    buf.push(0);
    buf.extend_from_slice(&recipient.to_be_bytes()[..]);
    buf.extend_from_slice(id_bytes);
    buf.extend_from_slice(payload);
    buf
}
