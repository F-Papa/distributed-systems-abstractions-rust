use log::debug;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashSet, sync::Arc};
use tokio::sync::{Mutex, mpsc};
use uuid::{self, Uuid};

use crate::common::BoxFuture;
use crate::links::LinkError::MessageError;
use crate::links::stubborn_link::{SblDeliver, StubbornLink};
use crate::links::{LinkError, NodeId};

pub type PlDeliver = SblDeliver;

pub trait PerfectLink<T>: Send + Sync {
    fn send(&self, payload: T, recipient: NodeId) -> BoxFuture<'_, Result<(), LinkError>>;
    fn listen(&self) -> BoxFuture<'_, Result<(), LinkError>>;
    fn close(&self) -> BoxFuture<'_, ()>;
    fn get_indications(&self) -> Arc<Mutex<mpsc::Receiver<PlDeliver>>>;
}

pub struct EliminateDuplicates {
    stubborn_link: Arc<dyn StubbornLink<Value>>,
    indications: Arc<Mutex<mpsc::Receiver<PlDeliver>>>,
    indications_tx: Mutex<Option<mpsc::Sender<PlDeliver>>>,
    inbox: Arc<Mutex<HashSet<Uuid>>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Envelope<T> {
    id: Uuid,
    payload: T,
}

impl PerfectLink<Value> for EliminateDuplicates {
    fn send(&self, payload: Value, recipient: NodeId) -> BoxFuture<'_, Result<(), LinkError>> {
        Box::pin(async move {
            let envelope = Envelope {
                id: Uuid::new_v4(),
                payload,
            };

            let bytes = serde_json::to_value(&envelope).map_err(|e| MessageError(e.to_string()))?;
            self.stubborn_link.send(bytes, recipient).await
        })
    }

    fn listen(&self) -> BoxFuture<'_, Result<(), LinkError>> {
        Box::pin(async move {
            let stubborn_link_cpy = Arc::clone(&self.stubborn_link);
            tokio::try_join!(stubborn_link_cpy.listen(), self.handle_indications())?;
            debug!("finished");
            Ok(())
        })
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move { self.stubborn_link.close().await })
    }

    fn get_indications(&self) -> Arc<Mutex<mpsc::Receiver<PlDeliver>>> {
        Arc::clone(&self.indications)
    }
}

impl EliminateDuplicates {
    pub fn new(stubborn_link: Arc<dyn StubbornLink<Value>>) -> Self {
        let (indications_tx, indications) = mpsc::channel(32);
        let inbox = Arc::from(Mutex::from(HashSet::new()));

        Self {
            stubborn_link,
            indications: Arc::from(Mutex::from(indications)),
            indications_tx: Mutex::from(Some(indications_tx)),
            inbox,
        }
    }

    async fn handle_indications(&self) -> Result<(), LinkError> {
        let stubborn_link_indications = Arc::clone(&self.stubborn_link.get_indications());

        let mut sbl_indications = stubborn_link_indications.lock().await;
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
