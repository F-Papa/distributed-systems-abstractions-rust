use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use log::debug;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    sync::{Mutex, mpsc},
    try_join,
};
use uuid::Uuid;

use crate::{
    broadcast::best_effort::{BeDeliver, BestEffortBroadcast},
    common::NodeId,
    links::{LinkError, fair_loss_link::FairLossLink, stubborn_link::StubbornLink},
};
pub type SbDeliver = BeDeliver;

#[async_trait]
pub trait StubbornBroadcast<T>: Sync + Send {
    async fn broadcast<'a>(&'a self, payload: &'a T) -> Result<(), LinkError>;
    async fn listen(&self) -> Result<(), LinkError>;
    async fn close(&self);
    /// Transfers the receiver to its consumer. Returns `None` after the first take.
    async fn take_indications(&self) -> Option<mpsc::Receiver<SbDeliver>>;
}

pub struct BasicStubbonBroadcast<T> {
    link: Arc<dyn StubbornLink<T>>,
    indications: Arc<Mutex<Option<mpsc::Receiver<BeDeliver>>>>,
    indications_tx: Mutex<Option<mpsc::Sender<BeDeliver>>>,
    local_id: NodeId,
    peers: HashSet<NodeId>,
}

#[async_trait]
impl<T> StubbornBroadcast<T> for BasicStubbonBroadcast<T>
where
    T: Sync + Serialize,
{
    async fn broadcast<'a>(&'a self, payload: &'a T) -> Result<(), LinkError> {
        for p in self.peers.iter() {
            self.link.send(payload, *p).await?;
        }
        Ok(())
    }

    async fn listen(&self) -> Result<(), LinkError> {
        try_join!(self.handle_link_indications(), self.link.listen())?;
        Ok(())
    }

    async fn close(&self) {
        self.link.close().await
    }
    /// Transfers the receiver to its consumer. Returns `None` after the first take.
    async fn take_indications(&self) -> Option<mpsc::Receiver<BeDeliver>> {
        self.indications.lock().await.take()
    }
}

impl<T> BasicStubbonBroadcast<T>
where
    T: Sync + Serialize,
{
    pub fn new(local_id: NodeId, peers: HashSet<NodeId>, link: Arc<dyn StubbornLink<T>>) -> Self {
        let (indications_tx, indications) = mpsc::channel(32);
        Self {
            link,
            indications: Arc::from(Mutex::from(Some(indications))),
            indications_tx: Mutex::from(Some(indications_tx)),
            local_id,
            peers,
        }
    }
    async fn handle_link_indications(&self) -> Result<(), LinkError> {
        let mut link_indications = match self.link.take_indications().await {
            None => return Err(LinkError::LinkClosed()),
            Some(i) => i,
        };

        let indications_tx = match self.indications_tx.lock().await.take() {
            None => return Err(LinkError::LinkClosed()),
            Some(i) => i,
        };

        while let Some(i) = link_indications.recv().await {
            indications_tx
                .send(i)
                .await
                .map_err(LinkError::with_action("receive indication"))?;
        }

        Ok(())
    }
}
