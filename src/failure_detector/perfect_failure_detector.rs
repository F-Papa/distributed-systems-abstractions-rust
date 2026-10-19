use std::{collections::HashSet, sync::Arc, time::Duration};

use log::debug;
use serde::{Deserialize, Serialize};
use tokio::{
    select,
    sync::{
        Mutex,
        mpsc::{self, Receiver, channel},
        watch,
    },
};

use crate::{
    failure_detector::perfect_failure_detector::PfdMessage::{HeartReply, HeartbeatRequest},
    links::{
        LinkError::{self, MessageError},
        NodeId, Resolver,
        perfect_link::{EliminateDuplicates, PlDeliver},
    },
};

pub struct Crash {
    pub node_id: NodeId,
}

pub struct ExcludeOnTimeout {
    peers: HashSet<NodeId>,
    alive: Mutex<HashSet<NodeId>>,
    detected: Mutex<HashSet<NodeId>>,
    perfect_link: Arc<EliminateDuplicates>,
    pub indications: Mutex<mpsc::Receiver<Crash>>,
    indications_tx: Mutex<Option<mpsc::Sender<Crash>>>,
    pl_indications: Arc<Mutex<Receiver<PlDeliver>>>,
    shutdown_tx: watch::Sender<bool>,
    shutdown: Mutex<watch::Receiver<bool>>,
}

#[derive(Debug, Deserialize, Serialize)]
enum PfdMessage {
    HeartbeatRequest,
    HeartReply,
}

impl ExcludeOnTimeout {
    pub async fn new(
        node_id: NodeId,
        resolver: Box<dyn Resolver>,
        peers: HashSet<NodeId>,
    ) -> Result<Self, LinkError> {
        let perfect_link = Arc::new(EliminateDuplicates::new(node_id, resolver, None).await?);
        let pl_indications = Arc::clone(&perfect_link.indications);

        let (indications_tx, indications_rx) = channel::<Crash>(32);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let detector = ExcludeOnTimeout {
            peers: peers.clone(),
            alive: Mutex::new(peers.clone()),
            detected: Mutex::new(HashSet::new()),
            perfect_link,
            indications: Mutex::new(indications_rx),
            indications_tx: Mutex::new(Some(indications_tx)),
            pl_indications,
            shutdown: Mutex::new(shutdown_rx),
            shutdown_tx,
        };

        return Ok(detector);
    }

    pub async fn stop(self: &Arc<Self>) {
        self.shutdown_tx.send_replace(true);
        self.perfect_link.close().await;
    }

    pub async fn start(self: &Arc<Self>) -> Result<(), LinkError> {
        tokio::try_join!(
            self.perfect_link.listen(),
            self.handle_timeout(),
            self.handle_link_indications()
        )?;
        debug!("finished");

        Ok(())
    }

    async fn handle_link_indications(self: &Arc<Self>) -> Result<(), LinkError> {
        let mut indications = self.pl_indications.lock().await;
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

                    self.perfect_link.send(&payload, deliver.sender).await?;
                    debug!("{HEARTBEAT_REPLY} sent to {}", deliver.sender);
                }
            };
        }

        Ok(())
    }

    async fn handle_timeout(self: &Arc<Self>) -> Result<(), LinkError> {
        let mut tx = self.indications_tx.lock().await;
        let mut shutdown = self.shutdown.lock().await;

        'outer: loop {
            select! {
            _ = shutdown.wait_for(|value| *value) => {
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

                self.perfect_link.send(&payload, *p).await?;
                debug!("{:?} sent to {}", HeartbeatRequest, p);
            }

            alive.clear();
        }

        Ok(())
    }
}
