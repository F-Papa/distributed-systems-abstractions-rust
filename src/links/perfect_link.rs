use log::debug;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::sync::{Mutex, mpsc};
use uuid::{self, Uuid};

use crate::links::LinkError::MessageError;
use crate::links::stubborn_link::{SblDeliver, RetransmitForever};
use crate::links::{LinkError, NodeId, Resolver};

pub type PlDeliver = SblDeliver;

pub struct EliminateDuplicates {
    pub stubborn_link: Arc<RetransmitForever>,
    pub indications: Arc<Mutex<mpsc::Receiver<PlDeliver>>>,
    indications_tx: Mutex<Option<mpsc::Sender<PlDeliver>>>,
    sbl_indications: Arc<Mutex<mpsc::Receiver<SblDeliver>>>,
    inbox: Arc<Mutex<HashSet<Uuid>>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Envelope<T> {
    id: Uuid,
    payload: T,
}

impl EliminateDuplicates {
    pub async fn new(
        local_id: NodeId,
        resolver: Box<dyn Resolver>,
        retransmission_override: Option<Duration>,
    ) -> Result<Self, LinkError> {
        let stubborn_link = RetransmitForever::new(local_id, resolver, retransmission_override).await?;
        let sbl_indications = Arc::clone(&stubborn_link.indications);

        let (tx, rx) = mpsc::channel::<PlDeliver>(32);

        let inbox: Arc<Mutex<HashSet<Uuid>>> = Arc::new(Mutex::new(HashSet::new()));

        let perfect_link = EliminateDuplicates {
            stubborn_link: Arc::new(stubborn_link),
            indications: Arc::from(Mutex::from(rx)),
            indications_tx: Mutex::from(Some(tx)),
            sbl_indications,
            inbox,
        };

        Ok(perfect_link)
    }

    pub async fn send(&self, msg: &Value, recipient: NodeId) -> Result<(), LinkError> {
        let envelope = Envelope {
            id: Uuid::new_v4(),
            payload: msg,
        };

        let bytes = serde_json::to_value(&envelope).map_err(|e| MessageError(e.to_string()))?;
        self.stubborn_link.send(Arc::from(bytes), recipient).await
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
            let indication = match sbl_indications.recv().await {
                None => break,
                Some(sbl_deliver) => sbl_deliver,
            };

            let envelope: Envelope<Value> = match serde_json::from_value(indication.message) {
                Err(e) => {
                    debug!("invalid message ({e})");
                    continue;
                }
                Ok(e) => e,
            };

            if inbox.contains(&envelope.id) {
                continue;
            }

            let indication = PlDeliver {
                message: envelope.payload,
                sender: indication.sender,
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
            inbox.insert(envelope.id);
        }
        *indications_tx = None;
        Ok(())
    }

    pub async fn close(&self) {
        self.stubborn_link.close().await
    }
}
