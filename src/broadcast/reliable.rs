use std::{
    collections::HashMap,
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
    links::
        LinkError::{self, InternalError}
        
    ,
};

pub type RbDeliver = BeDeliver;

#[async_trait]
pub trait ReliableBroadcast<T>: Sync + Send {
    async fn broadcast<'a>(&'a self, payload: &'a T) -> Result<(), LinkError>;
    async fn listen(&self) -> Result<(), LinkError>;
    async fn close(&self);
    /// Transfers the receiver to its consumer. Returns `None` after the first take.
    async fn take_indications(&self) -> Option<mpsc::Receiver<BeDeliver>>;
}

pub struct EagerReliableBroadcast {
    best_effort_broadcast: Arc<dyn BestEffortBroadcast<Value>>,
    indications: Arc<Mutex<Option<mpsc::Receiver<BeDeliver>>>>,
    indications_tx: Mutex<Option<mpsc::Sender<BeDeliver>>>,
    local_id: NodeId,
}

#[derive(Serialize, Deserialize, Debug)]
struct Envelope<T> {
    id: uuid::Uuid,
    original_sender: NodeId,
    payload: T,
}

impl EagerReliableBroadcast {
    pub fn new(
        local_id: NodeId,
        best_effort_broadcast: Arc<dyn BestEffortBroadcast<Value>>,
    ) -> Self {
        let (indications_tx, indications) = mpsc::channel(32);
        Self {
            local_id,
            best_effort_broadcast,
            indications_tx: Mutex::from(Some(indications_tx)),
            indications: Arc::from(Mutex::from(Some(indications))),
        }
    }
    async fn handle_link_indications(&self) -> Result<(), LinkError> {
        let mut inbox: HashMap<(NodeId, Uuid), Value> = HashMap::new();
        let mut link_indications = match self.best_effort_broadcast.take_indications().await {
            None => return Err(LinkError::LinkClosed()),
            Some(i) => i,
        };

        let indications_tx = match self.indications_tx.lock().await.take() {
            None => return Err(LinkError::LinkClosed()),
            Some(i) => i,
        };

        while let Some(i) = link_indications.recv().await {
            let envelope: Envelope<Value> = match serde_json::from_value(i.message.clone()) {
                Ok(e) => e,
                Err(e) => {
                    debug!("error deserializing envelope: {:?}", e);
                    continue;
                }
            };

            if inbox.contains_key(&(envelope.original_sender, envelope.id)) {
                continue;
            }

            inbox.insert(
                (envelope.original_sender, envelope.id),
                envelope.payload.clone(),
            );

            let delivery = RbDeliver {
                message: envelope.payload,
                sender: envelope.original_sender,
            };

            indications_tx
                .send(delivery)
                .await
                .map_err(|e| InternalError(e.to_string()))?;

            self.best_effort_broadcast.broadcast(&i.message).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl<T> ReliableBroadcast<T> for EagerReliableBroadcast
where
    T: Sync + Serialize,
{
    async fn broadcast<'a>(&'a self, payload: &'a T) -> Result<(), LinkError>
    where
        T: 'a,
    {
        let envelope = Envelope {
            payload,
            id: Uuid::new_v4(),
            original_sender: self.local_id,
        };

        let value = serde_json::to_value(&envelope).map_err(|e| InternalError(e.to_string()))?;
        self.best_effort_broadcast.broadcast(&value).await
    }

    async fn listen(&self) -> Result<(), LinkError> {
        try_join!(
            self.handle_link_indications(),
            self.best_effort_broadcast.listen()
        )?;
        Ok(())
    }

    async fn close(&self) {
        self.best_effort_broadcast.close().await
    }
    /// Transfers the receiver to its consumer. Returns `None` after the first take.
    async fn take_indications(&self) -> Option<mpsc::Receiver<BeDeliver>> {
        self.indications.lock().await.take()
    }
}
