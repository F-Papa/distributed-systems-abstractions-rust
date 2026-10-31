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
        Mutex,
        mpsc::{self, Receiver},
    },
    time, try_join,
};
use uuid::Uuid;

use crate::{
    common::{ShutdownHandler, marshal, unmarshal},
    links::{
        self, LinkError, NodeId,
        fair_loss_link::FairLossLink,
        perfect_link::{PerfectLink, PlDeliver},
    },
};

pub struct AcknowledgeMessages {
    fair_loss_link: Arc<FairLossLink>,
    outbox: Arc<Mutex<HashMap<(NodeId, Uuid), Arc<Value>>>>,
    inbox: Arc<Mutex<HashSet<(NodeId, Uuid)>>>,
    indications: Mutex<Option<mpsc::Receiver<PlDeliver>>>,
    indications_tx: Arc<mpsc::Sender<PlDeliver>>,
    retransmission_period: Duration,
    shutdown_handler: ShutdownHandler,
}

#[derive(Debug, Serialize, Deserialize)]
enum Protocol {
    AcknowledgeMessages,
}

#[derive(Serialize, Debug, Deserialize)]
struct Envelope<T> {
    protocol: Protocol,
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
#[serde(untagged)]
enum ProtocolMessage<T> {
    Ack {
        protocol: Protocol,
        #[serde(rename = "Ack")]
        id: Uuid,
    },
    Data {
        #[serde(rename = "Data")]
        envelope: Envelope<T>,
    },
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
            indications: Mutex::new(Some(indications)),
            indications_tx: Arc::from(indications_tx),
            retransmission_period: retransmission_override
                .unwrap_or(Duration::from_secs(DEFAULT_RETRANSMISSION_SECS)),
            shutdown_handler: ShutdownHandler::new(),
        }
    }
    async fn ack(&self, to: NodeId, id: Uuid) -> Result<(), LinkError> {
        let ack: ProtocolMessage<Value> = ProtocolMessage::Ack {
            protocol: Protocol::AcknowledgeMessages,
            id,
        };
        let encoded = marshal(&ack)?;
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
        let mut fll_indications = self
            .fair_loss_link
            .take_indications()
            .await
            .ok_or_else(|| LinkError::InternalError("indications receiver already taken".into()))?;
        let mut received_keys = self.inbox.lock().await;

        while let Some(fll_deliver) = fll_indications.recv().await {
            let message: ProtocolMessage<Value> = match unmarshal(fll_deliver.message) {
                Ok(e) => e,
                Err(e) => {
                    debug!("Invalid message from {}: {:?}", fll_deliver.sender, e);
                    continue;
                }
            };

            let envelope = match message {
                ProtocolMessage::Ack { id, .. } => {
                    self.outbox.lock().await.remove(&(fll_deliver.sender, id));
                    continue;
                }
                ProtocolMessage::Data { envelope } => envelope,
            };

            let dedup_key = envelope.key();
            self.indications_tx
                .send(PlDeliver {
                    sender: fll_deliver.sender,
                    message: envelope.payload,
                })
                .await
                .map_err(LinkError::with_action("send indication"))?;

            received_keys.insert(dedup_key);
            self.ack(fll_deliver.sender, envelope.id).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl<T> PerfectLink<T> for AcknowledgeMessages
where
    T: Sync + Serialize,
{
    async fn send<'a>(&'a self, payload: &'a T, recipient: links::NodeId) -> Result<(), LinkError> {
        let envelope = Envelope {
            protocol: Protocol::AcknowledgeMessages,
            payload,
            recipient,
            id: Uuid::new_v4(),
        };

        let key = (recipient, envelope.id);
        let encoded_message = marshal(ProtocolMessage::Data { envelope })?;

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

    async fn take_indications(&self) -> Option<Receiver<super::PlDeliver>> {
        self.indications.lock().await.take()
    }
}
