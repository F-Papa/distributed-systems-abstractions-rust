use ed25519_dalek::{self, Signature, Signer, Verifier, VerifyingKey};
use log::debug;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::sync::{Mutex, mpsc};
use uuid::{self, Uuid};

use crate::links::LinkError::MessageError;
use crate::links::stubborn_link::{SblDeliver, RetransmitForever};
use crate::links::{LinkError, NodeId, Resolver};

pub type AplDeliver = SblDeliver;
pub struct AuthPerfectLink {
    local_id: NodeId,
    pub stubborn_link: Arc<RetransmitForever>,
    pub indications: Arc<Mutex<mpsc::Receiver<AplDeliver>>>,
    indications_tx: Mutex<Option<mpsc::Sender<AplDeliver>>>,
    sbl_indications: Arc<Mutex<mpsc::Receiver<SblDeliver>>>,
    inbox: Arc<Mutex<HashSet<Uuid>>>,
    signing_key: ed25519_dalek::SigningKey,
    verifiers: HashMap<NodeId, Arc<VerifyingKey>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Envelope<T, S> {
    id: Uuid,
    payload: T,
    signature: S,
}

impl AuthPerfectLink {
    pub async fn new(
        local_id: NodeId,
        resolver: Box<dyn Resolver>,
        retransmission_override: Option<Duration>,
        signing_key: ed25519_dalek::SigningKey,
        verifiers: HashMap<NodeId, Arc<VerifyingKey>>,
    ) -> Result<Self, LinkError> {
        let stubborn_link = RetransmitForever::new(local_id, resolver, retransmission_override).await?;
        let sbl_indications = Arc::clone(&stubborn_link.indications);

        let (tx, rx) = mpsc::channel::<AplDeliver>(32);

        let inbox: Arc<Mutex<HashSet<Uuid>>> = Arc::new(Mutex::new(HashSet::new()));

        let perfect_link = AuthPerfectLink {
            local_id,
            stubborn_link: Arc::new(stubborn_link),
            indications: Arc::from(Mutex::from(rx)),
            indications_tx: Mutex::from(Some(tx)),
            sbl_indications,
            inbox,
            signing_key,
            verifiers,
        };

        Ok(perfect_link)
    }

    pub async fn send(&self, msg: Value, recipient: NodeId) -> Result<(), LinkError> {
        let id = uuid::Uuid::new_v4();
        let payload = serde_json::to_vec(&msg).map_err(|e| MessageError(e.to_string()))?;

        let to_sign = self.bytes_to_sign(&id, recipient, &payload);
        let signature = self.signing_key.sign(&to_sign).to_bytes();

        let envelope = Envelope {
            id,
            signature: &signature[..],
            payload: msg,
        };

        let value = serde_json::to_value(envelope).map_err(|e| MessageError(e.to_string()))?;

        self.stubborn_link.send(Arc::from(value), recipient).await?;
        Ok(())
    }

    fn bytes_to_sign(&self, id: &Uuid, recipient: NodeId, payload: &[u8]) -> Vec<u8> {
        let id_bytes = id.as_bytes();
        let mut buf = Vec::with_capacity(id_bytes.len() + payload.len() + NodeId::ENCODED_LEN);

        buf.extend_from_slice(&recipient.to_be_bytes()[..]);
        buf.extend_from_slice(id_bytes);
        buf.extend_from_slice(payload);
        buf
    }

    pub async fn listen(&self) -> Result<(), LinkError> {
        tokio::try_join!(self.stubborn_link.listen(), self.handle_indications())?;
        Ok(())
    }

    async fn handle_indications(&self) -> Result<(), LinkError> {
        let indications_tx = match self.indications_tx.lock().await.take() {
            None => return Err(LinkError::LinkClosed()),
            Some(tx) => tx,
        };

        let mut inbox = self.inbox.lock().await;
        let mut sbl_indications = self.sbl_indications.lock().await;

        loop {
            let sbl_deliver = match sbl_indications.recv().await {
                Some(sbl_deliver) => sbl_deliver,
                None => break,
            };

            let envelope: Envelope<Value, Vec<u8>> =
                match serde_json::from_value(sbl_deliver.message) {
                    Ok(e) => e,
                    Err(e) => {
                        debug!("Error decoding envelope: {:?}", e);
                        continue;
                    }
                };

            if !self.verify_signature(sbl_deliver.sender, &envelope) {
                debug!("Invalid signature for sender {}", sbl_deliver.sender);
                continue;
            }

            if inbox.contains(&envelope.id) {
                debug!("Duplicate from {}: {}", sbl_deliver.sender, &envelope.id);
                continue;
            }

            if indications_tx
                .send(AplDeliver {
                    sender: sbl_deliver.sender,
                    message: envelope.payload,
                })
                .await
                .is_err()
            {
                break;
            }

            inbox.insert(envelope.id);
        }

        Ok(())
    }

    fn verify_signature(&self, sender: NodeId, envelope: &Envelope<Value, Vec<u8>>) -> bool {
        let verifier = match self.verifiers.get(&sender) {
            Some(verifier) => verifier,
            None => {
                debug!("No verifier for {}", sender);
                return false;
            }
        };

        let payload_bytes = match serde_json::to_vec(&envelope.payload) {
            Ok(b) => b,
            Err(e) => {
                debug!("error getting payload_bytes: {:?}", e);
                return false;
            }
        };

        let bytes_to_verify = self.bytes_to_sign(&envelope.id, self.local_id, &payload_bytes);
        let signature = match Signature::from_slice(&envelope.signature) {
            Ok(s) => s,
            Err(e) => {
                debug!("could not parse signature: {:?}", e);
                return false;
            }
        };

        verifier.verify(&bytes_to_verify, &signature).is_ok()
    }

    pub async fn close(&self) {
        self.stubborn_link.close().await
    }
}
