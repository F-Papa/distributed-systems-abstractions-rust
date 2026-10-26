use async_trait::async_trait;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use log::debug;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    select,
    sync::{Mutex, mpsc},
    time, try_join,
};
use uuid::Uuid;

use crate::{
    common::ShutdownHandler,
    links::{
        LinkError::{self, InternalError, MessageError},
        NodeId,
        auth_perfect_link::{AplDeliver, AuthenticatedPerfectLink, bytes_to_sign},
        fair_loss_link::FairLossLink,
    },
};

pub struct SignAndAcknowledge {
    local_id: NodeId,
    fair_loss_link: Arc<FairLossLink>,
    outbox: Arc<Mutex<HashMap<(NodeId, Uuid), Arc<Value>>>>,
    inbox: Arc<Mutex<HashSet<(NodeId, Uuid)>>>,
    indications: Mutex<Option<mpsc::Receiver<AplDeliver>>>,
    indications_tx: Arc<mpsc::Sender<AplDeliver>>,
    retransmission_period: Duration,
    shutdown_handler: ShutdownHandler,
    signing_key: SigningKey,
    verifiers: HashMap<NodeId, Arc<VerifyingKey>>,
}

#[derive(Debug, Serialize, Deserialize)]
enum Protocol {
    SignAndAcknowledge,
}

#[derive(Serialize, Debug, Deserialize)]
struct Envelope<T, S> {
    protocol: Protocol,
    id: Uuid,
    recipient: NodeId,
    payload: T,
    signature: S,
}

#[derive(Deserialize, Serialize, Debug)]
enum ProtocolMessage<T> {
    Ack(Uuid),
    Data(T),
}

type OutboxEntry = ((NodeId, Uuid), Arc<Value>);

impl SignAndAcknowledge {
    pub fn new(
        local_id: NodeId,
        signing_key: SigningKey,
        verifiers: HashMap<NodeId, Arc<VerifyingKey>>,
        fair_loss_link: FairLossLink,
        retransmission_override: Option<Duration>,
    ) -> Self {
        const DEFAULT_RETRANSMISSION_SECS: u64 = 5;
        let (indications_tx, indications) = mpsc::channel::<AplDeliver>(32);

        Self {
            local_id,
            fair_loss_link: Arc::from(fair_loss_link),
            outbox: Arc::from(Mutex::from(HashMap::new())),
            inbox: Arc::from(Mutex::from(HashSet::new())),
            indications: Mutex::new(Some(indications)),
            indications_tx: Arc::from(indications_tx),
            retransmission_period: retransmission_override
                .unwrap_or(Duration::from_secs(DEFAULT_RETRANSMISSION_SECS)),
            shutdown_handler: ShutdownHandler::new(),
            signing_key,
            verifiers,
        }
    }

    fn encode_message<T>(
        &self,
        payload: ProtocolMessage<T>,
        recipient: NodeId,
    ) -> Result<(Uuid, Value), LinkError>
    where
        T: Serialize,
    {
        let id = Uuid::new_v4();
        let payload_bytes =
            serde_json::to_vec(&payload).map_err(|e| MessageError(e.to_string()))?;
        let to_sign = bytes_to_sign("SignAndAcknowledge", &id, recipient, &payload_bytes);
        let signature = self.signing_key.sign(&to_sign).to_bytes();

        let envelope = Envelope {
            protocol: Protocol::SignAndAcknowledge,
            id,
            recipient,
            payload,
            signature: &signature[..],
        };
        let encoded = serde_json::to_value(envelope).map_err(|e| MessageError(e.to_string()))?;
        Ok((id, encoded))
    }

    fn verify_signature<T>(
        &self,
        sender: NodeId,
        envelope: &Envelope<ProtocolMessage<T>, Vec<u8>>,
    ) -> bool
    where
        T: Serialize,
    {
        if envelope.recipient != self.local_id {
            debug!("Wrong recipient: {}", envelope.recipient);
            return false;
        }

        let verifier = match self.verifiers.get(&sender) {
            Some(verifier) => verifier,
            None => {
                debug!("No verifier for {}", sender);
                return false;
            }
        };

        let payload_bytes = match serde_json::to_vec(&envelope.payload) {
            Ok(bytes) => bytes,
            Err(e) => {
                debug!("Error serializing signed payload: {:?}", e);
                return false;
            }
        };

        let bytes_to_verify = bytes_to_sign(
            "SignAndAcknowledge",
            &envelope.id,
            self.local_id,
            &payload_bytes,
        );
        let signature = match Signature::from_slice(&envelope.signature) {
            Ok(signature) => signature,
            Err(e) => {
                debug!("Could not parse signature: {:?}", e);
                return false;
            }
        };

        verifier.verify(&bytes_to_verify, &signature).is_ok()
    }

    async fn ack(&self, to: NodeId, id: Uuid) -> Result<(), LinkError> {
        let (_, encoded) = self.encode_message::<Value>(ProtocolMessage::Ack(id), to)?;
        self.fair_loss_link.send(&encoded, to).await
    }

    async fn retransmit(&self) -> Result<(), LinkError> {
        loop {
            // listen cancels this loop on shutdown, including blocked sends.
            time::sleep(self.retransmission_period).await;

            let to_send: Vec<OutboxEntry> = {
                let outbox = self.outbox.lock().await;
                outbox.iter().map(|(a, b)| (*a, Arc::clone(b))).collect()
            };

            for ((recipient, _), message) in to_send {
                self.fair_loss_link.send(&message, recipient).await?;
            }
        }
    }

    async fn handle_fll_indications(&self) -> Result<(), LinkError> {
        let mut fll_indications = self
            .fair_loss_link
            .take_indications()
            .await
            .ok_or_else(|| LinkError::InternalError("indications receiver already taken".into()))?;
        let mut received_keys = self.inbox.lock().await;

        while let Some(fll_deliver) = fll_indications.recv().await {
            let envelope: Envelope<ProtocolMessage<Value>, Vec<u8>> =
                match serde_json::from_value(fll_deliver.message) {
                    Ok(envelope) => envelope,
                    Err(e) => {
                        debug!("Invalid message from {}: {}", fll_deliver.sender, e);
                        continue;
                    }
                };

            if !self.verify_signature(fll_deliver.sender, &envelope) {
                debug!("Invalid signature for sender {}", fll_deliver.sender);
                continue;
            }

            let payload = match envelope.payload {
                ProtocolMessage::Ack(id) => {
                    self.outbox.lock().await.remove(&(fll_deliver.sender, id));
                    continue;
                }
                ProtocolMessage::Data(payload) => payload,
            };

            let dedup_key = (fll_deliver.sender, envelope.id);
            if !received_keys.contains(&dedup_key) {
                self.indications_tx
                    .send(AplDeliver {
                        sender: fll_deliver.sender,
                        message: payload,
                    })
                    .await
                    .map_err(|e| InternalError(e.to_string()))?;

                received_keys.insert(dedup_key);
            }

            self.ack(fll_deliver.sender, envelope.id).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl<T> AuthenticatedPerfectLink<T> for SignAndAcknowledge
where
    T: Sync + Serialize,
{
    async fn send<'a>(&'a self, payload: &'a T, recipient: NodeId) -> Result<(), LinkError> {
        let (id, encoded_message) =
            self.encode_message(ProtocolMessage::Data(payload), recipient)?;
        let key = (recipient, id);
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
        select! {
            _ = self.shutdown_handler.wait() => {}
            result = async {
                try_join!(
                    self.fair_loss_link.listen(),
                    self.handle_fll_indications(),
                    self.retransmit()
                )
            } => { result?; }
        }
        Ok(())
    }

    async fn close(&self) {
        self.shutdown_handler.shutdown().await;
        self.fair_loss_link.close().await;
    }

    async fn take_indications(&self) -> Option<mpsc::Receiver<AplDeliver>> {
        self.indications.lock().await.take()
    }
}
