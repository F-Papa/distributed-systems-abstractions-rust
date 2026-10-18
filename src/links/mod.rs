use std::{fmt, sync::Arc};

use tokio::signal::unix::{Signal, SignalKind, signal};

pub mod auth_perfect_link;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(u16);

impl NodeId {
    pub const ENCODED_LEN: usize = 2;

    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u16 {
        self.0
    }

    pub const fn to_be_bytes(self) -> [u8; Self::ENCODED_LEN] {
        self.0.to_be_bytes()
    }

    pub const fn from_be_bytes(bytes: [u8; Self::ENCODED_LEN]) -> Self {
        Self(u16::from_be_bytes(bytes))
    }
}

impl From<u16> for NodeId {
    fn from(value: u16) -> Self {
        Self::new(value)
    }
}

impl From<NodeId> for u16 {
    fn from(node_id: NodeId) -> Self {
        node_id.get()
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

pub fn signals() -> (Signal, Signal) {
    let sigtterm = signal(SignalKind::terminate()).expect("creating sigterm receiver");
    let sigtint = signal(SignalKind::interrupt()).expect("creating sigint receiver");
    (sigtint, sigtterm)
}

pub trait Resolver: Send + Sync {
    fn resolve(&self, peer: NodeId) -> String;
}

pub struct LocalhostResolver {
    pub base_port: u16,
}

impl Resolver for LocalhostResolver {
    fn resolve(&self, peer: NodeId) -> String {
        let peer_port = self.base_port + peer.get();
        format!("localhost:{peer_port}")
    }
}
