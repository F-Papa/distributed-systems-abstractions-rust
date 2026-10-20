use std::{collections::HashSet, sync::Arc, time::Duration};

use log::debug;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    select,
    sync::{
        Mutex,
        mpsc::{self, channel},
    },
};

use crate::{
    common::{BoxFuture, ShutdownHandler},
    links::{LinkError, perfect_link::PerfectLink},
};

use crate::{
    failure_detector::perfect_failure_detector::PfdMessage::{HeartReply, HeartbeatRequest},
    links::{LinkError::MessageError, NodeId},
};

pub trait PerfectFailureDetector: Sync + Send {
    fn start(self: &Arc<Self>) -> BoxFuture<'_, Result<(), LinkError>>;
    fn stop(self: &Arc<Self>) -> BoxFuture<'_, Result<(), LinkError>>;
    fn get_indications(&self) -> Arc<Mutex<mpsc::Receiver<Crash>>>;
}

pub struct Crash {
    pub node_id: NodeId,
}

pub struct ExcludeOnTimeout {
    peers: HashSet<NodeId>,
    alive: Mutex<HashSet<NodeId>>,
    detected: Mutex<HashSet<NodeId>>,
    perfect_link: Arc<dyn PerfectLink<Value>>,
    indications: Arc<Mutex<mpsc::Receiver<Crash>>>,
    indications_tx: Mutex<Option<mpsc::Sender<Crash>>>,
    shutdown_handler: ShutdownHandler,
}

#[derive(Debug, Deserialize, Serialize)]
enum PfdMessage {
    HeartbeatRequest,
    HeartReply,
}

impl PerfectFailureDetector for ExcludeOnTimeout {
    fn start(self: &Arc<Self>) -> BoxFuture<'_, Result<(), LinkError>> {
        Box::pin(async move {
            tokio::try_join!(
                self.perfect_link.listen(),
                self.handle_timeout(),
                self.handle_link_indications()
            )?;
            Ok(())
        })
    }

    fn stop(self: &Arc<Self>) -> BoxFuture<'_, Result<(), LinkError>> {
        Box::pin(async move {
            self.shutdown_handler.shutdown().await;
            self.perfect_link.close().await;
            Ok(())
        })
    }

    fn get_indications(&self) -> Arc<Mutex<mpsc::Receiver<Crash>>> {
        Arc::clone(&self.indications)
    }
}

impl ExcludeOnTimeout {
    pub fn new(peers: HashSet<NodeId>, perfect_link: Arc<dyn PerfectLink<Value>>) -> Self {
        let (indications_tx, indications_rx) = channel::<Crash>(32);

        ExcludeOnTimeout {
            peers: peers.clone(),
            alive: Mutex::new(peers.clone()),
            detected: Mutex::new(HashSet::new()),
            perfect_link,
            indications: Arc::from(Mutex::from(indications_rx)),
            indications_tx: Mutex::new(Some(indications_tx)),
            shutdown_handler: ShutdownHandler::new(),
        }
    }

    async fn handle_link_indications(&self) -> Result<(), LinkError> {
        let pl_indications = self.perfect_link.get_indications();
        let mut indications = pl_indications.lock().await;

        while let Some(deliver) = indications.recv().await {
            match serde_json::from_value(deliver.message) {
                Err(_) => continue,
                Ok(HeartReply) => {
                    debug!("{:?} from {}", HeartReply, deliver.sender);
                    self.alive.lock().await.insert(deliver.sender);
                }
                Ok(HeartbeatRequest) => {
                    debug!("{:?} from {}", HeartbeatRequest, deliver.sender);

                    let payload = serde_json::to_value(PfdMessage::HeartReply)
                        .map_err(|e| MessageError(e.to_string()))?;

                    self.perfect_link.send(payload, deliver.sender).await?;
                    debug!("{:?} sent to {}", HeartReply, deliver.sender);
                }
            };
        }

        Ok(())
    }

    async fn handle_timeout(&self) -> Result<(), LinkError> {
        let mut tx = self.indications_tx.lock().await;

        'outer: loop {
            select! {
            _ = self.shutdown_handler.wait() => {
                break 'outer;
            }

            _ = tokio::time::sleep(Duration::from_secs(3))=> {}
            }

            let alive = &mut *self.alive.lock().await;
            let detected = &mut *self.detected.lock().await;

            for p in self.peers.iter() {
                if !alive.contains(p) && !detected.contains(p) {
                    detected.insert(*p);

                    match tx.as_mut() {
                        None => return Err(LinkError::LinkClosed()),
                        Some(tx) => {
                            if tx.send(Crash { node_id: *p }).await.is_err() {
                                break 'outer;
                            }
                        }
                    };
                }

                let payload = match serde_json::to_value(HeartbeatRequest) {
                    Ok(p) => p,
                    Err(e) => {
                        debug!("Error encoding HeartbeatRequest: {:?}", e);
                        continue;
                    }
                };

                self.perfect_link.send(payload, *p).await?;
                debug!("{:?} sent to {}", HeartbeatRequest, p);
            }

            alive.clear();
        }

        Ok(())
    }
}
