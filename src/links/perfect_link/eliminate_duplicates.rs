use async_trait::async_trait;

use log::debug;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashSet, sync::Arc};
use tokio::sync::{Mutex, mpsc};
use uuid::{self, Uuid};

use crate::links::LinkError::MessageError;
use crate::links::perfect_link::{PerfectLink, PlDeliver};
use crate::links::stubborn_link::StubbornLink;
use crate::links::{LinkError, NodeId};

pub struct EliminateDuplicates {
    stubborn_link: Arc<dyn StubbornLink<Value>>,
    indications: Mutex<Option<mpsc::Receiver<PlDeliver>>>,
    indications_tx: Mutex<Option<mpsc::Sender<PlDeliver>>>,
    inbox: Arc<Mutex<HashSet<Uuid>>>,
}

#[derive(Debug, Serialize, Deserialize)]
enum Protocol {
    EliminateDuplicates,
}

#[derive(Debug, Serialize, Deserialize)]
struct Envelope<T> {
    protocol: Protocol,
    id: Uuid,
    payload: T,
}

#[async_trait]
impl<T> PerfectLink<T> for EliminateDuplicates
where
    T: Sync + Serialize,
{
    async fn send<'a>(&'a self, payload: &'a T, recipient: NodeId) -> Result<(), LinkError> {
        let envelope = Envelope {
            protocol: Protocol::EliminateDuplicates,
            id: Uuid::new_v4(),
            payload,
        };

        let bytes = serde_json::to_value(&envelope).map_err(|e| MessageError(e.to_string()))?;
        self.stubborn_link.send(&bytes, recipient).await
    }

    async fn listen(&self) -> Result<(), LinkError> {
        let stubborn_link_cpy = Arc::clone(&self.stubborn_link);
        tokio::try_join!(stubborn_link_cpy.listen(), self.handle_indications())?;
        debug!("finished");
        Ok(())
    }

    async fn close(&self) {
        self.stubborn_link.close().await
    }

    async fn take_indications(&self) -> Option<mpsc::Receiver<PlDeliver>> {
        self.indications.lock().await.take()
    }
}

impl EliminateDuplicates {
    pub fn new(stubborn_link: Arc<dyn StubbornLink<Value>>) -> Self {
        let (indications_tx, indications) = mpsc::channel(32);
        let inbox = Arc::from(Mutex::from(HashSet::new()));

        Self {
            stubborn_link,
            indications: Mutex::new(Some(indications)),
            indications_tx: Mutex::from(Some(indications_tx)),
            inbox,
        }
    }

    async fn handle_indications(&self) -> Result<(), LinkError> {
        let mut sbl_indications =
            self.stubborn_link.take_indications().await.ok_or_else(|| {
                LinkError::InternalError("indications receiver already taken".into())
            })?;
        let mut indications_tx = self.indications_tx.lock().await;
        let mut inbox = self.inbox.lock().await;
        loop {
            let sbl_deliver = match sbl_indications.recv().await {
                None => break,
                Some(sbl_deliver) => sbl_deliver,
            };

            let envelope: Envelope<Value> = match serde_json::from_value(sbl_deliver.message) {
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
            inbox.insert(envelope.id);
        }
        *indications_tx = None;
        Ok(())
    }
}
