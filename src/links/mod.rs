use std::sync::Arc;

use tokio::signal::unix::{Signal, SignalKind, signal};

pub mod fair_loss_link;
pub mod perfect_link;
pub mod stubborn_link;

const MAX_LENGTH: usize = 1000;

pub type Callback = Arc<dyn Fn(&[u8]) -> Result<(), LinkError> + Send + Sync>;

#[derive(Debug)]
pub enum LinkError {
    SocketError(String),
    MessageError(String),
    InternalError(String),
    LinkClosed(),
}

pub fn signals() -> (Signal, Signal) {
    let sigtterm = signal(SignalKind::interrupt()).expect("creating sigint receiver");
    let sigtint = signal(SignalKind::interrupt()).expect("creating sigint receiver");
    (sigtint, sigtterm)
}

pub trait Resolver<TPeer, TAddress>: Send + Sync {
    fn resolve(&self, peer: TPeer) -> TAddress;
}

pub struct LocalhostResolver {
    pub base_port: u16,
}

impl Resolver<u16, String> for LocalhostResolver {
    fn resolve(&self, peer: u16) -> String {
        let peer_port = self.base_port + peer;
        format!("localhost:{peer_port}")
    }
}
