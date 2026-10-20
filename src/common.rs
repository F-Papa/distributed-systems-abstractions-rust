use std::{future::Future, pin::Pin};

use tokio::sync::{Mutex, watch};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

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
        self.sender.send_replace(false);
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
