use async_trait::async_trait;

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
    common::{NodeId, ShutdownHandler},
    links::{LinkError, perfect_link::PerfectLink},
};

use crate::links::LinkError::MessageError;

#[async_trait]
pub trait EventuallyPerfectFailureDetector: Sync + Send {
    async fn start(&self) -> Result<(), LinkError>;
    async fn stop(&self) -> Result<(), LinkError>;
    /// Transfers the receiver to its consumer. Returns `None` after the first take.
    async fn take_indications(&self) -> Option<mpsc::Receiver<EpfdMessageIndication>>;
}

pub enum EpfdMessageIndication {
    Suspect(NodeId),
    Restore(NodeId),
}

pub struct IncreasingTimeout {
    peers: HashSet<NodeId>,
    timeout: Arc<Mutex<Option<Duration>>>,
    alive: Mutex<HashSet<NodeId>>,
    detected: Mutex<HashSet<NodeId>>,
    perfect_link: Arc<dyn PerfectLink<Value>>,
    indications: Mutex<Option<mpsc::Receiver<EpfdMessageIndication>>>,
    indications_tx: Mutex<Option<mpsc::Sender<EpfdMessageIndication>>>,
    shutdown_handler: ShutdownHandler,
}

#[derive(Debug, Serialize, Deserialize)]
enum Protocol {
    IncreasingTimeout,
}

#[derive(Debug, Serialize, Deserialize)]
struct Heartbeat {
    protocol: Protocol,
    #[serde(flatten)]
    message: EpfdMessage,
}

impl Heartbeat {
    fn new(message: EpfdMessage) -> Self {
        Self {
            protocol: Protocol::IncreasingTimeout,
            message,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
enum EpfdMessage {
    HeartbeatRequest,
    HeartReply,
}

#[async_trait]
impl EventuallyPerfectFailureDetector for IncreasingTimeout {
    async fn start(&self) -> Result<(), LinkError> {
        tokio::try_join!(
            self.perfect_link.listen(),
            self.handle_timeout(),
            self.handle_link_indications()
        )?;
        Ok(())
    }

    async fn stop(&self) -> Result<(), LinkError> {
        self.shutdown_handler.shutdown().await;
        self.perfect_link.close().await;
        Ok(())
    }

    async fn take_indications(&self) -> Option<mpsc::Receiver<EpfdMessageIndication>> {
        self.indications.lock().await.take()
    }
}

impl IncreasingTimeout {
    pub fn new(peers: HashSet<NodeId>, perfect_link: Arc<dyn PerfectLink<Value>>) -> Self {
        let (indications_tx, indications_rx) = channel::<EpfdMessageIndication>(32);
        const INITIAL_TIMEOUT: u64 = 3;

        IncreasingTimeout {
            peers: peers.clone(),
            alive: Mutex::new(peers.clone()),
            detected: Mutex::new(HashSet::new()),
            perfect_link,
            indications: Mutex::new(Some(indications_rx)),
            indications_tx: Mutex::new(Some(indications_tx)),
            shutdown_handler: ShutdownHandler::new(),
            timeout: Arc::from(Mutex::from(Some(Duration::from_secs(INITIAL_TIMEOUT)))),
        }
    }

    async fn handle_link_indications(&self) -> Result<(), LinkError> {
        let mut indications =
            self.perfect_link.take_indications().await.ok_or_else(|| {
                LinkError::InternalError("indications receiver already taken".into())
            })?;

        while let Some(deliver) = indications.recv().await {
            match serde_json::from_value::<Heartbeat>(deliver.message)
                .map(|heartbeat| heartbeat.message)
            {
                Err(_) => continue,
                Ok(EpfdMessage::HeartReply) => {
                    debug!("{:?} from {}", EpfdMessage::HeartReply, deliver.sender);
                    self.alive.lock().await.insert(deliver.sender);
                }
                Ok(EpfdMessage::HeartbeatRequest) => {
                    debug!(
                        "{:?} from {}",
                        EpfdMessage::HeartbeatRequest,
                        deliver.sender
                    );

                    let payload = serde_json::to_value(Heartbeat::new(EpfdMessage::HeartReply))
                        .map_err(|e| MessageError(e.to_string()))?;

                    self.perfect_link.send(&payload, deliver.sender).await?;
                    debug!("{:?} sent to {}", EpfdMessage::HeartReply, deliver.sender);
                }
            };
        }

        Ok(())
    }

    async fn handle_timeout(&self) -> Result<(), LinkError> {
        const TIMEOUT_INCREASE: u64 = 2;

        let (tx, mut timeout) = match (
            self.indications_tx.lock().await.take(),
            self.timeout.lock().await.take(),
        ) {
            (Some(tx), Some(to)) => (tx, to),
            _ => return Err(LinkError::LinkClosed()),
        };

        'outer: loop {
            select! {
            _ = self.shutdown_handler.wait() => {
                break 'outer;
            }

            _ = tokio::time::sleep(timeout)=> {}
            }

            let alive = &mut *self.alive.lock().await;
            let suspected = &mut *self.detected.lock().await;

            if alive.iter().find(|n| suspected.contains(n)).is_some() {
                timeout += Duration::from_secs(TIMEOUT_INCREASE)
            }

            for p in self.peers.iter() {
                if !alive.contains(p) && !suspected.contains(p) {
                    suspected.insert(*p);
                    if tx.send(EpfdMessageIndication::Suspect(*p)).await.is_err() {
                        break 'outer;
                    }
                } else if alive.contains(p) && suspected.contains(p) {
                    suspected.remove(p);
                    if tx.send(EpfdMessageIndication::Restore(*p)).await.is_err() {
                        break 'outer;
                    }
                }

                let payload =
                    match serde_json::to_value(Heartbeat::new(EpfdMessage::HeartbeatRequest)) {
                        Ok(p) => p,
                        Err(e) => {
                            debug!("Error encoding HeartbeatRequest: {:?}", e);
                            continue;
                        }
                    };

                self.perfect_link.send(&payload, *p).await?;
                debug!("{:?} sent to {}", EpfdMessage::HeartbeatRequest, p);
            }

            alive.clear();
        }

        Ok(())
    }
}
