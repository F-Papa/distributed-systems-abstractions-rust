use log::debug;
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::sync::{Mutex, mpsc};
use uuid::{self, Uuid};

use crate::links::stubborn_link::{SblDeliver, StubbornLink};
use crate::links::{LinkError, NodeId, Resolver};

const UUID_LEN: usize = 16;

pub type PlDeliver = SblDeliver;

pub struct PerfectLink {
    pub stubborn_link: Arc<StubbornLink>,
    pub indications: Arc<Mutex<mpsc::Receiver<PlDeliver>>>,
    indications_tx: Mutex<Option<mpsc::Sender<PlDeliver>>>,
    sbl_indications: Arc<Mutex<mpsc::Receiver<SblDeliver>>>,
    inbox: Arc<Mutex<HashSet<Uuid>>>,
}

impl PerfectLink {
    pub async fn new(
        local_id: NodeId,
        resolver: Box<dyn Resolver>,
        retransmission_override: Option<Duration>,
    ) -> Result<Self, LinkError> {
        let stubborn_link = StubbornLink::new(local_id, resolver, retransmission_override).await?;
        let sbl_indications = Arc::clone(&stubborn_link.indications);

        let (tx, rx) = mpsc::channel::<PlDeliver>(32);

        let inbox: Arc<Mutex<HashSet<Uuid>>> = Arc::new(Mutex::new(HashSet::new()));

        let perfect_link = PerfectLink {
            stubborn_link: Arc::new(stubborn_link),
            indications: Arc::from(Mutex::from(rx)),
            indications_tx: Mutex::from(Some(tx)),
            sbl_indications,
            inbox,
        };

        Ok(perfect_link)
    }

    pub async fn send(&self, msg: &[u8], recipient: NodeId) -> Result<(), LinkError> {
        self.stubborn_link.send(&add_envelope(msg), recipient).await
    }
    pub async fn listen(&self) -> Result<(), LinkError> {
        let stubborn_link_cpy = Arc::clone(&self.stubborn_link);
        tokio::try_join!(stubborn_link_cpy.listen(), self.handle_indications())?;
        debug!("finished");
        Ok(())
    }

    async fn handle_indications(&self) -> Result<(), LinkError> {
        let mut inbox = self.inbox.lock().await;
        let mut sbl_indications = self.sbl_indications.lock().await;
        let mut indications_tx = self.indications_tx.lock().await;
        loop {
            match sbl_indications.recv().await {
                None => break,
                Some(sbl_deliver) => {
                    let (id, payload) = take_envelope(&sbl_deliver.message)?;
                    if inbox.contains(&id) {
                        continue;
                    }

                    let indication = PlDeliver {
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
        Ok(())
    }

    pub async fn close(&self) {
        self.stubborn_link.close().await
    }
}

fn take_envelope(msg: &[u8]) -> Result<(uuid::Uuid, &[u8]), LinkError> {
    let (id_bytes, payload) = msg.split_at(UUID_LEN);
    let id_bytes: [u8; 16] = id_bytes
        .try_into()
        .map_err(|e| LinkError::MessageError(format!("Could not obtain message id: {:?}", e)))?;

    let id = uuid::Uuid::from_bytes(id_bytes);
    Ok((id, payload))
}

fn add_envelope(msg: &[u8]) -> Vec<u8> {
    let id = uuid::Uuid::new_v4().into_bytes();
    let mut result = Vec::with_capacity(id.len() + msg.len());
    result.extend_from_slice(&id);
    result.extend_from_slice(msg);
    result
}
