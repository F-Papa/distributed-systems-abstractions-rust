use async_trait::async_trait;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use log::debug;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    select,
    sync::{
        self, Mutex,
        mpsc::{self, Receiver},
    },
    time, try_join,
};
use uuid::Uuid;

use crate::{
    common::ShutdownHandler,
    links::{
        self,
        LinkError::{self, InternalError, MessageError},
        NodeId,
        fair_loss_link::FairLossLink,
        perfect_link::{PerfectLink, PlDeliver},
    },
};

pub struct AcknowledgeMessages {
    fair_loss_link: Arc<FairLossLink>,
    outbox: Arc<Mutex<HashMap<(NodeId, Uuid), Arc<Value>>>>,
    inbox: Arc<Mutex<HashSet<(NodeId, Uuid)>>>,
    indications: Arc<Mutex<mpsc::Receiver<PlDeliver>>>,
    indications_tx: Arc<mpsc::Sender<PlDeliver>>,
    retransmission_period: Duration,
    shutdown_handler: ShutdownHandler,
}

#[derive(Serialize, Debug, Deserialize)]
struct Envelope<T> {
    id: Uuid,
    recipient: NodeId,
    payload: T,
}

impl<T> Envelope<T> {
    fn key(&self) -> (NodeId, Uuid) {
        (self.recipient, self.id)
    }
}

#[derive(Deserialize, Serialize, Debug)]
enum ProtocolMessage {
    Ack(Uuid),
    Data(Value),
}

type OutboxEntry = ((NodeId, Uuid), Arc<Value>);

impl AcknowledgeMessages {
    pub fn new(fair_loss_link: FairLossLink, retransmission_override: Option<Duration>) -> Self {
        const DEFAULT_RETRANSMISSION_SECS: u64 = 5;
        let (indications_tx, indications) = mpsc::channel::<PlDeliver>(32);

        Self {
            fair_loss_link: Arc::from(fair_loss_link),
            outbox: Arc::from(Mutex::from(HashMap::new())),
            inbox: Arc::from(Mutex::from(HashSet::new())),
            indications: Arc::from(Mutex::from(indications)),
            indications_tx: Arc::from(indications_tx),
            retransmission_period: retransmission_override
                .unwrap_or(Duration::from_secs(DEFAULT_RETRANSMISSION_SECS)),
            shutdown_handler: ShutdownHandler::new(),
        }
    }
    async fn ack(&self, to: NodeId, id: Uuid) -> Result<(), LinkError> {
        let ack = ProtocolMessage::Ack(id);
        let encoded = serde_json::to_value(&ack).map_err(|e| MessageError(e.to_string()))?;
        self.fair_loss_link.send(&encoded, to).await
    }

    async fn retransmit(&self) -> Result<(), LinkError> {
        loop {
            select! {
                _ = time::sleep(self.retransmission_period) => {}
                _ = self.shutdown_handler.wait() => { break; }
            }

            let to_send: Vec<OutboxEntry> = {
                let outbox = self.outbox.lock().await;
                outbox.iter().map(|(a, b)| (*a, Arc::clone(b))).collect()
            };

            for ((recipient, _), message) in to_send {
                self.fair_loss_link.send(&message, recipient).await?;
            }
        }

        Ok(())
    }

    async fn handle_fll_indications(&self) -> Result<(), LinkError> {
        let fll_indications = self.fair_loss_link.get_indications();

        let mut fll_indications = fll_indications.lock().await;
        let mut received_keys = self.inbox.lock().await;

        while let Some(fll_deliver) = fll_indications.recv().await {
            let message: ProtocolMessage = match serde_json::from_value(fll_deliver.message) {
                Ok(e) => e,
                Err(e) => {
                    debug!(
                        "Invalid message from {}: {}",
                        fll_deliver.sender,
                        e.to_string()
                    );
                    continue;
                }
            };

            let value = match message {
                ProtocolMessage::Ack(id) => {
                    self.outbox.lock().await.remove(&(fll_deliver.sender, id));
                    continue;
                }
                ProtocolMessage::Data(value) => value,
            };

            let envelope: Envelope<Value> = match serde_json::from_value(value) {
                Ok(e) => e,
                Err(e) => {
                    debug!("could not deserialize envelope: {}", e.to_string());
                    continue;
                }
            };

            let dedup_key = envelope.key();
            self.indications_tx
                .send(PlDeliver {
                    sender: fll_deliver.sender,
                    message: envelope.payload,
                })
                .await
                .map_err(|e| InternalError(e.to_string()))?;

            received_keys.insert(dedup_key);
            self.ack(fll_deliver.sender, envelope.id).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl PerfectLink<Value> for AcknowledgeMessages {
    async fn send(&self, payload: Value, recipient: links::NodeId) -> Result<(), LinkError> {
        let envelope = Envelope {
            payload,
            recipient,
            id: Uuid::new_v4(),
        };

        let encoded_envelope =
            serde_json::to_value(&envelope).map_err(|e| MessageError(e.to_string()))?;

        let encoded_message = serde_json::to_value(&ProtocolMessage::Data(encoded_envelope))
            .map_err(|e| MessageError(e.to_string()))?;

        let key = (recipient, envelope.id);
        let encoded_message = Arc::from(encoded_message);

        self.outbox
            .lock()
            .await
            .insert(key, Arc::clone(&encoded_message));

        if let Err(error) = self.fair_loss_link.send(&encoded_message, recipient).await {
            self.outbox.lock().await.remove(&key);
            return Err(error);
        }

        Ok(())
    }

    async fn listen(&self) -> Result<(), LinkError> {
        try_join!(
            self.fair_loss_link.listen(),
            self.handle_fll_indications(),
            self.retransmit()
        )?;
        Ok(())
    }

    async fn close(&self) {
        self.shutdown_handler.shutdown().await;
        self.fair_loss_link.close().await;
    }

    fn get_indications(&self) -> Arc<sync::Mutex<Receiver<super::PlDeliver>>> {
        Arc::clone(&self.indications)
    }
}
