use tokio::signal::unix::{Signal, SignalKind, signal};

use crate::{common::NodeId, links::LinkError::InternalError};

pub mod auth_perfect_link;
pub mod fair_loss_link;
pub mod logged_perfect_link;
pub mod perfect_link;
pub mod stubborn_link;

const MAX_LENGTH: usize = 1000;

#[derive(Debug)]
pub enum LinkError {
    SocketError(String),
    MessageError(String),
    InternalError(String),
    LinkClosed(),
}

impl LinkError {
    pub fn from_error<E>(e: E) -> Self
    where
        E: std::error::Error,
    {
        InternalError(e.to_string())
    }

    pub fn with_action<E>(action: &str) -> impl Fn(E) -> Self
    where
        E: std::error::Error,
    {
        move |e: E| InternalError(format!("action:{}, error: {}", action, e.to_string()))
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
