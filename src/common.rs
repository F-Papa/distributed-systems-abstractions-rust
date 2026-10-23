use core::fmt;

use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, watch};

pub struct ShutdownHandler {
    sender: watch::Sender<bool>,
    receiver: Mutex<watch::Receiver<bool>>,
}

impl ShutdownHandler {
    pub fn new() -> Self {
        let (sender, receiver) = watch::channel(false);
        Self {
            sender,
            receiver: Mutex::from(receiver),
        }
    }
    pub async fn shutdown(&self) {
        self.sender.send_replace(true);
    }

    pub async fn wait(&self) {
        self.receiver
            .lock()
            .await
            .wait_for(|is_shutting_down| *is_shutting_down)
            .await
            .expect("could not signal shutdown");
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
