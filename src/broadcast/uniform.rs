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
    links::LinkError::{self, InternalError},
};

pub type UbDeliver = BeDeliver;

#[async_trait]
pub trait UniformBroadcast<T>: Sync + Send {
    async fn broadcast<'a>(&'a self, payload: &'a T) -> Result<(), LinkError>;
    async fn listen(&self) -> Result<(), LinkError>;
    async fn close(&self);
    /// Transfers the receiver to its consumer. Returns `None` after the first take.
    async fn take_indications(&self) -> Option<mpsc::Receiver<UbDeliver>>;
}

pub struct MajorityAckBroadcast {
    best_effort_broadcast: Arc<dyn BestEffortBroadcast<Value>>,
    indications: Arc<Mutex<Option<mpsc::Receiver<BeDeliver>>>>,
    indications_tx: Mutex<Option<mpsc::Sender<BeDeliver>>>,
    local_id: NodeId,
    number_of_nodes: u16,
}

#[derive(Debug, Serialize, Deserialize)]
struct Envelope<T> {
    id: Uuid,
    payload: T,
    original_sender: NodeId,
}

#[async_trait]
impl<T> UniformBroadcast<T> for MajorityAckBroadcast
where
    T: Sync + Serialize,
{
    async fn broadcast<'a>(&'a self, payload: &'a T) -> Result<(), LinkError>
    where
        T: 'a,
    {
        let envelope = Envelope {
            original_sender: self.local_id,
            id: Uuid::new_v4(),
            payload,
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

struct PendingDelivery {
    delivered: bool,
    received_from: HashSet<NodeId>,
}

impl MajorityAckBroadcast {
    pub fn new(
        local_id: NodeId,
        number_of_nodes: u16,
        best_effort_broadcast: Arc<dyn BestEffortBroadcast<Value>>,
    ) -> Self {
        let (indications_tx, indications) = mpsc::channel(32);
        Self {
            local_id,
            number_of_nodes,
            best_effort_broadcast,
            indications_tx: Mutex::from(Some(indications_tx)),
            indications: Arc::from(Mutex::from(Some(indications))),
        }
    }
    async fn handle_delivery(
        &self,
        delivery: BeDeliver,
        inbox: &mut HashMap<Uuid, PendingDelivery>,
        indications_tx: &mpsc::Sender<UbDeliver>,
    ) -> Result<(), LinkError> {
        let envelope: Envelope<Value> = match serde_json::from_value(delivery.message.clone()) {
            Ok(e) => e,
            Err(e) => {
                debug!("error deserializing envelope: {:?}", e);
                return Ok(());
            }
        };

        if delivery.sender != self.local_id && !inbox.contains_key(&envelope.id) {
            self.best_effort_broadcast
                .broadcast(&delivery.message)
                .await?;
        }

        let message = inbox.entry(envelope.id).or_insert(PendingDelivery {
            delivered: false,
            received_from: HashSet::with_capacity(self.number_of_nodes as usize),
        });

        message.received_from.insert(delivery.sender);
        let majority = (1 + self.number_of_nodes / 2) as usize;

        println!(
            "message_id: {} #received_from: {} majority: {}",
            envelope.id,
            message.received_from.len(),
            majority
        );

        if !message.delivered && (message.received_from.len()) >= majority {
            let indication = UbDeliver {
                sender: envelope.original_sender,
                message: envelope.payload,
            };

            indications_tx
                .send(indication)
                .await
                .map_err(|e| InternalError(e.to_string()))?;

            message.delivered = true;
        }

        if message.received_from.len() == message.received_from.capacity() {
            inbox.remove(&envelope.id);
        }

        Ok(())
    }

    async fn handle_link_indications(&self) -> Result<(), LinkError> {
        let mut link_indications = match self.best_effort_broadcast.take_indications().await {
            None => return Err(LinkError::LinkClosed()),
            Some(i) => i,
        };

        let indications_tx = match self.indications_tx.lock().await.take() {
            None => return Err(LinkError::LinkClosed()),
            Some(i) => i,
        };

        let mut inbox = HashMap::new();

        while let Some(i) = link_indications.recv().await {
            self.handle_delivery(i, &mut inbox, &indications_tx).await?;
        }
        Ok(())
    }
}
