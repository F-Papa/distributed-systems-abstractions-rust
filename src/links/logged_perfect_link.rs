use std::{fmt::format, path::Path, sync::Arc};

use async_trait::async_trait;

use log::debug;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    fs::{self, create_dir_all, rename},
    io::{AsyncReadExt, AsyncWriteExt},
    net::unix::pipe::Sender,
    sync::{Mutex, mpsc},
};
use uuid::Uuid;

use crate::links::{
    LinkError::{self, InternalError},
    NodeId,
    perfect_link::{PerfectLink, PlDeliver},
};

pub struct LplDeliver {
    pub delivered: Vec<PlDeliver>,
}

#[derive(Deserialize, Serialize, Debug)]
struct Envelope<T> {
    id: Uuid,
    payload: T,
}

#[async_trait]
pub trait LoggedPerfectLink<T> {
    async fn send(&self, payload: T, recipient: NodeId) -> Result<(), LinkError>;
    async fn listen(&self) -> Result<(), LinkError>;
    async fn close(&self);
    /// Transfers the receiver to its consumer. Returns `None` after the first take.
    async fn take_indications(&self) -> Option<mpsc::Receiver<LplDeliver>>;
}

pub struct CommitAndAcknowledge {
    log_filepath: Box<Path>,
    indications_tx: Arc<Mutex<Option<mpsc::Sender<LplDeliver>>>>,
    indications: Arc<Mutex<Option<mpsc::Receiver<LplDeliver>>>>,
    perfect_link: Arc<dyn PerfectLink<Value>>,
}

impl CommitAndAcknowledge {
    pub fn new(
        id: NodeId,
        perfect_link: Arc<dyn PerfectLink<Value>>,
        log_filepath_override: Option<String>,
    ) -> Self {
        let (indications_tx, indications) = mpsc::channel(32);
        let filepath = log_filepath_override.unwrap_or(format!("./data/{}.log", id.get()));
        Self {
            log_filepath: Box::from(Path::new(&filepath)),
            indications_tx: Arc::from(Mutex::from(Some(indications_tx))),
            indications: Arc::from(Mutex::from(Some(indications))),
            perfect_link,
        }
    }

    async fn retrieve(&self) -> Result<Vec<Envelope<Value>>, LinkError> {
        if !self.log_filepath.exists() {
            return Ok(vec![]);
        }

        let mut file = fs::File::open(&self.log_filepath)
            .await
            .map_err(|e| InternalError(e.to_string()))?;

        let mut content = String::new();
        file.read_to_string(&mut content)
            .await
            .map_err(|e| InternalError(e.to_string()))?;
        let delivered = serde_json::from_str(&content).map_err(|e| InternalError(e.to_string()))?;
        Ok(delivered)
    }

    async fn store(&self, delivered: &Vec<Envelope<Value>>) -> Result<(), LinkError> {
        if let Some(parent_dir) = self.log_filepath.parent()
            && !parent_dir.exists()
        {
            create_dir_all(parent_dir)
                .await
                .map_err(|e| InternalError(e.to_string()))?;
        }

        let content = serde_json::to_string(delivered).map_err(|e| InternalError(e.to_string()))?;
        let tmp_filepath = (self.log_filepath.to_string_lossy() + ".tmp").to_string();
        let mut file = fs::File::create(&tmp_filepath)
            .await
            .map_err(|e| InternalError(e.to_string()))?;

        file.write_all(content.as_bytes())
            .await
            .map_err(|e| InternalError(e.to_string()))?;

        rename(tmp_filepath, &self.log_filepath).await.map_err(|e| InternalError(e.to_string()))?;
        Ok(())
    }

    async fn handle_pl_indications(&self, mut delivered: Vec<Envelope<Value>>) -> Result<(), LinkError> {
        let tx = match self.indications_tx.lock().await.take() {
            Some(tx) => tx,
            None => return Err(LinkError::LinkClosed()),
        };

        if let Some(mut pl_indications) = self.perfect_link.take_indications().await.take() {
            while let Some(pl_delivery) = pl_indications.recv().await {
                let envelope: Envelope<Value> = match serde_json::from_value(pl_delivery.message) {
                    Ok(e) => e,
                    Err(e) => {
                        debug!("could not deserialize envelope: {}", e);
                        continue;
                    }
                };

                let is_dup = delivered.iter().find(|d| d.id == envelope.id).is_some();
                if is_dup {
                    continue;
                }

                delivered.push(envelope);
                tx.send(LplDeliver {
                    delivered: delivered
                        .iter()
                        .map(|d| PlDeliver {
                            sender: pl_delivery.sender,
                            message: d.payload.clone(),
                        })
                        .collect(),
                })
                .await
                .map_err(|e| InternalError(e.to_string()))?;
                self.store(&delivered).await?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl LoggedPerfectLink<Value> for CommitAndAcknowledge {
    async fn send(&self, payload: Value, recipient: NodeId) -> Result<(), LinkError> {
        let envelope = Envelope {
            id: Uuid::new_v4(),
            payload,
        };

        let value = serde_json::to_value(envelope).map_err(|e| InternalError(e.to_string()))?;
        self.perfect_link.send(value, recipient).await
    }
    async fn listen(&self) -> Result<(), LinkError> {
        let delivered = self.retrieve().await?;
        tokio::try_join!(self.handle_pl_indications(delivered), self.perfect_link.listen())?;
        Ok(())
    }
    async fn close(&self) {
        self.perfect_link.close().await
    }
    /// Transfers the receiver to its consumer. Returns `None` after the first take.
    async fn take_indications(&self) -> Option<mpsc::Receiver<LplDeliver>> {
        self.indications.lock().await.take()
    }
}
