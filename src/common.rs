use core::fmt;
use std::{io::stdin, sync::Arc, thread};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::sync::{Mutex, mpsc, watch};

use crate::links::LinkError::{self, InternalError};

pub struct ShutdownHandler {
    sender: watch::Sender<bool>,
    receiver: Mutex<watch::Receiver<bool>>,
}

impl Default for ShutdownHandler {
    fn default() -> Self {
        Self::new()
    }
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

pub fn marshal<T>(value: T) -> Result<Value, LinkError> where T: Serialize{
        serde_json::to_value(value).map_err(|e| InternalError(format!("Failed to marshal value: {}", e.to_string())))
}

pub fn unmarshal<T>(value: Value) -> Result<T, LinkError> where T: DeserializeOwned {
    serde_json::from_value(value).map_err(|e| InternalError(format!("Failed to unmarshal value: {}", e.to_string())))
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

pub struct InputHandler {
    tx: std::sync::Mutex<Option<mpsc::Sender<String>>>,
    rx: std::sync::Mutex<Option<mpsc::Receiver<String>>>,
}

impl Default for InputHandler {
    fn default() -> Self {
        Self::new()
    }
}

// type Callback<I, O> = AsyncFnOnce(I) -> O;
// type Callback<I, O> = Box<dyn Fn(I) -> Pin<Box<dyn Future<Output = O>>>>;

impl InputHandler {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(32);
        Self {
            tx: std::sync::Mutex::from(Some(tx)),
            rx: std::sync::Mutex::from(Some(rx)),
        }
    }

    pub fn start(self: Arc<Self>) {
        thread::spawn(move || {
            let tx = match self.tx.lock().unwrap().take() {
                Some(tx) => tx,
                None => return,
            };

            loop {
                let mut input = String::new();
                stdin().read_line(&mut input).unwrap();
                tx.blocking_send(input).unwrap();
            }
        });
    }

    pub async fn handle_input<C, E>(&self, callback: C) -> Result<(), E>
    where
        C: AsyncFn(String) -> Result<(), E>,
    {
        let mut rx = self.take_rx().unwrap();
        while let Some(input) = rx.recv().await {
            callback(input).await?;
        }
        Ok::<(), E>(())
    }

    pub fn take_rx(&self) -> Option<mpsc::Receiver<String>> {
        self.rx.lock().unwrap().take()
    }
}
