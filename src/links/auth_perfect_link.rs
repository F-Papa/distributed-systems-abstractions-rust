use ed25519_dalek::{self, Signature, Signer, Verifier, VerifyingKey};
use std::collections::HashMap;
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::sync::{Mutex, mpsc};
use uuid::{self, Uuid};

use crate::links::stubborn_link::{SblDeliver, StubbornLink};
use crate::links::{LinkError, NodeId, Resolver};

const UUID_LEN: usize = 16;
const SIGNATURE_LEN: usize = 64;

pub type AplDeliver = SblDeliver;
pub struct AuthPerfectLink {
    pub stubborn_link: Arc<StubbornLink>,
    pub indications: Arc<Mutex<mpsc::Receiver<AplDeliver>>>,
    indications_tx: Mutex<Option<mpsc::Sender<AplDeliver>>>,
    sbl_indications: Arc<Mutex<mpsc::Receiver<SblDeliver>>>,
    inbox: Arc<Mutex<HashSet<Uuid>>>,
    signing_key: ed25519_dalek::SigningKey,
    verifiers: HashMap<NodeId, Arc<VerifyingKey>>,
}

impl AuthPerfectLink {
    pub async fn new(
        local_id: NodeId,
        resolver: Box<dyn Resolver>,
        retransmission_override: Option<Duration>,
        signing_key: ed25519_dalek::SigningKey,
        verifiers: HashMap<NodeId, Arc<VerifyingKey>>,
    ) -> Result<Self, LinkError> {
        let stubborn_link = StubbornLink::new(local_id, resolver, retransmission_override).await?;
        let sbl_indications = Arc::clone(&stubborn_link.indications);

        let (tx, rx) = mpsc::channel::<AplDeliver>(32);

        let inbox: Arc<Mutex<HashSet<Uuid>>> = Arc::new(Mutex::new(HashSet::new()));

        let perfect_link = AuthPerfectLink {
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

    pub async fn send(&self, msg: &[u8], recipient: NodeId) -> Result<(), LinkError> {
        self.stubborn_link
            .send(&add_envelope(msg, &self.signing_key), recipient)
            .await
    }

    pub async fn listen(&self) -> Result<(), LinkError> {
        let stubborn_link_cpy = Arc::clone(&self.stubborn_link);
        let sbl_handle = tokio::spawn(async move { stubborn_link_cpy.listen().await });

        let mut inbox = self.inbox.lock().await;
        let mut sbl_indications = self.sbl_indications.lock().await;
        let mut indications_tx = self.indications_tx.lock().await;
        loop {
            match sbl_indications.recv().await {
                None => break,
                Some(sbl_deliver) => {
                    let (id, signature, payload) = take_envelope(&sbl_deliver.message)?;
                    if inbox.contains(&id) {
                        continue;
                    }

                    let verifier = self.verifiers.get(&sbl_deliver.sender);
                    let to_verify = [&id.as_bytes()[..], &payload].concat();

                    match verifier {
                        None => continue,
                        Some(verifier) => {
                            if verifier.verify(&to_verify, &signature).is_err() {
                                continue;
                            }
                        }
                    };

                    let indication = AplDeliver {
                        message: Arc::from(payload),
                        sender: sbl_deliver.sender,
                    };

                    if indications_tx
                        .as_ref()
                        .expect("get indications_tx")
                        .send(indication)
                        .await
                        .is_err()
                    {
                        break;
                    }
                    inbox.insert(id);
                }
            }
        }
        *indications_tx = None;

        sbl_handle.await.expect("join sbl_handle")
    }

    pub async fn close(&self) {
        self.stubborn_link.close().await
    }
}

fn take_envelope(msg: &[u8]) -> Result<(uuid::Uuid, ed25519_dalek::Signature, &[u8]), LinkError> {
    let (id_bytes, rest) = msg.split_at(UUID_LEN);
    let id_bytes: [u8; 16] = id_bytes
        .try_into()
        .map_err(|e| LinkError::MessageError(format!("Could not obtain message id: {:?}", e)))?;

    let (signature, payload) = rest.split_at(SIGNATURE_LEN);
    let signature_bytes: [u8; 64] = signature
        .try_into()
        .map_err(|e| LinkError::MessageError(format!("Could not obtain signature: {:?}", e)))?;

    let id = uuid::Uuid::from_bytes(id_bytes);
    let signature = Signature::from_bytes(&signature_bytes);

    Ok((id, signature, payload))
}

fn add_envelope(msg: &[u8], key: &ed25519_dalek::SigningKey) -> Vec<u8> {
    let id = uuid::Uuid::new_v4().into_bytes();

    let mut payload = Vec::with_capacity(id.len() + msg.len() + SIGNATURE_LEN);
    payload.extend_from_slice(&id);
    payload.extend_from_slice(msg);

    let signature = key.sign(&payload).to_bytes();

    payload.clear();
    payload.extend_from_slice(&id);
    payload.extend_from_slice(&signature);
    payload.extend_from_slice(msg);
    payload
}
