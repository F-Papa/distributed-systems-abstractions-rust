use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use uuid::Uuid;

use crate::common::BoxFuture;
use crate::links::stubborn_link::SblDeliver;
use crate::links::{LinkError, NodeId};

pub type AplDeliver = SblDeliver;

mod authenticate_and_filter;
pub use authenticate_and_filter::AuthenticateAndFilter;

mod sign_and_acknowledge;
pub use sign_and_acknowledge::SignAndAcknowledge;

pub trait AuthenticatedPerfectLink<T>: Send + Sync {
    fn send(&self, payload: T, recipient: NodeId) -> BoxFuture<'_, Result<(), LinkError>>;
    fn listen(&self) -> BoxFuture<'_, Result<(), LinkError>>;
    fn close(&self) -> BoxFuture<'_, ()>;
    fn get_indications(&self) -> Arc<Mutex<mpsc::Receiver<AplDeliver>>>;
}

pub(crate) struct Authenticator {
}

pub(crate) fn bytes_to_sign(id: &Uuid, recipient: NodeId, payload: &[u8]) -> Vec<u8> {
    let id_bytes = id.as_bytes();
    let mut buf = Vec::with_capacity(id_bytes.len() + payload.len() + NodeId::ENCODED_LEN);

    buf.extend_from_slice(&recipient.to_be_bytes()[..]);
    buf.extend_from_slice(id_bytes);
    buf.extend_from_slice(payload);
    buf
}
