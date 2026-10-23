use async_trait::async_trait;

use ed25519_dalek::{self, Signature, Signer, Verifier, VerifyingKey};
use log::debug;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::{collections::HashSet, sync::Arc};
use tokio::sync::{Mutex, mpsc};
use uuid::{self, Uuid};

use crate::links::LinkError::MessageError;
use crate::links::auth_perfect_link::{AplDeliver, AuthenticatedPerfectLink, bytes_to_sign};
use crate::links::stubborn_link::StubbornLink;
use crate::links::{LinkError, NodeId};

#[async_trait]
impl AuthenticatedPerfectLink<Value> for AuthenticateAndFilter {
    async fn send(&self, payload: Value, recipient: NodeId) -> Result<(), LinkError> {
        let id = uuid::Uuid::new_v4();
        let payload_bytes =
            serde_json::to_vec(&payload).map_err(|e| MessageError(e.to_string()))?;

        let to_sign = bytes_to_sign(&id, recipient, &payload_bytes);
        let signature = self.signing_key.sign(&to_sign).to_bytes();

        let envelope = Envelope {
            id,
            signature: &signature[..],
            payload,
        };

        let value = serde_json::to_value(envelope).map_err(|e| MessageError(e.to_string()))?;
        self.stubborn_link.send(value, recipient).await?;
        Ok(())
    }

    async fn listen(&self) -> Result<(), LinkError> {
        tokio::try_join!(self.stubborn_link.listen(), self.handle_indications())?;
        Ok(())
    }

    async fn close(&self) {
        self.stubborn_link.close().await
    }

    fn get_indications(&self) -> Arc<Mutex<mpsc::Receiver<AplDeliver>>> {
        Arc::clone(&self.indications)
    }
}

pub struct AuthenticateAndFilter {
    local_id: NodeId,
    stubborn_link: Arc<dyn StubbornLink<Value>>,
    indications: Arc<Mutex<mpsc::Receiver<AplDeliver>>>,
    indications_tx: Mutex<Option<mpsc::Sender<AplDeliver>>>,
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

impl AuthenticateAndFilter {
    pub async fn new(
        local_id: NodeId,
        signing_key: ed25519_dalek::SigningKey,
        verifiers: HashMap<NodeId, Arc<VerifyingKey>>,
        stubborn_link: Arc<dyn StubbornLink<Value>>,
    ) -> Result<Self, LinkError> {
        let (tx, rx) = mpsc::channel::<AplDeliver>(32);
        let inbox: Arc<Mutex<HashSet<Uuid>>> = Arc::new(Mutex::new(HashSet::new()));

        let perfect_link = AuthenticateAndFilter {
            local_id,
            stubborn_link,
            indications: Arc::from(Mutex::from(rx)),
            indications_tx: Mutex::from(Some(tx)),
            inbox,
            signing_key,
            verifiers,
        };

        Ok(perfect_link)
    }

    async fn handle_indications(&self) -> Result<(), LinkError> {
        let indications_tx = match self.indications_tx.lock().await.take() {
            None => return Err(LinkError::LinkClosed()),
            Some(tx) => tx,
        };

        let sbl_indications = self.stubborn_link.get_indications();

        let mut sbl_indications = sbl_indications.lock().await;
        let mut inbox = self.inbox.lock().await;

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
                debug!("Duplicate from {}: {}", sbl_deliver.sender, envelope.id);
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

        let bytes_to_verify = bytes_to_sign(&envelope.id, self.local_id, &payload_bytes);
        let signature = match Signature::from_slice(&envelope.signature) {
            Ok(s) => s,
            Err(e) => {
                debug!("could not parse signature: {:?}", e);
                return false;
            }
        };

        verifier.verify(&bytes_to_verify, &signature).is_ok()
    }
}
