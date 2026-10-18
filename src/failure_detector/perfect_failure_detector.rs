use std::{collections::HashSet, sync::Arc, time::Duration};

use log::debug;
use tokio::{
    select,
    sync::{
        Mutex,
        mpsc::{self, Receiver, channel},
        watch,
    },
};

use crate::links::{
    LinkError::{self},
    NodeId, Resolver,
    perfect_link::{PerfectLink, PlDeliver},
};

pub struct Crash {
    pub node_id: NodeId,
}

pub struct PerfectFailureDetector {
    peers: HashSet<NodeId>,
    alive: Mutex<HashSet<NodeId>>,
    detected: Mutex<HashSet<NodeId>>,
    perfect_link: Arc<PerfectLink>,
    pub indications: Mutex<mpsc::Receiver<Crash>>,
    indications_tx: Mutex<Option<mpsc::Sender<Crash>>>,
    pl_indications: Arc<Mutex<Receiver<PlDeliver>>>,
    shutdown_tx: watch::Sender<bool>,
    shutdown: Mutex<watch::Receiver<bool>>,
}

const HEARTBEAT_REQUEST: &str = "HEARTBEAT_REQUEST";
const HEARTBEAT_REPLY: &str = "HEARTBEAT_REPLY";

impl PerfectFailureDetector {
    pub async fn new(
        node_id: NodeId,
        resolver: Box<dyn Resolver>,
        peers: HashSet<NodeId>,
    ) -> Result<Self, LinkError> {
        let perfect_link = Arc::new(PerfectLink::new(node_id, resolver, None).await?);
        let pl_indications = Arc::clone(&perfect_link.indications);

        let (indications_tx, indications_rx) = channel::<Crash>(32);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let detector = PerfectFailureDetector {
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
            match std::str::from_utf8(&deliver.message) {
                Err(_) => continue,
                Ok(text) => {
                    if text == HEARTBEAT_REPLY {
                        debug!("{HEARTBEAT_REPLY} from {}", deliver.sender);
                        self.alive.lock().await.insert(deliver.sender);
                    } else if text == HEARTBEAT_REQUEST {
                        debug!("{HEARTBEAT_REQUEST} from {}", deliver.sender);
                        self.perfect_link
                            .send(HEARTBEAT_REPLY.as_bytes(), deliver.sender)
                            .await?;
                        debug!("{HEARTBEAT_REPLY} sent to {}", deliver.sender);
                    }
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

                self.perfect_link
                    .send(HEARTBEAT_REQUEST.as_bytes(), *p)
                    .await?;
                debug!("{HEARTBEAT_REQUEST} sent to {}", p);
            }

            alive.clear();
        }

        Ok(())
    }
}
