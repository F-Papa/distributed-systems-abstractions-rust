use async_trait::async_trait;

use std::{sync::Arc, time::Duration};

use serde_json::Value;
use tokio::{
    select,
    sync::{
        Mutex,
        mpsc::{self, Receiver, Sender},
    },
};

use crate::{
    common::{NodeId, ShutdownHandler}, links::{
        LinkError::{self, InternalError},
        fair_loss_link::{FairLossLink, FllDeliver},
    },
};

type OutboxEntry = (NodeId, Arc<Value>);

pub type SblDeliver = FllDeliver;

#[async_trait]
pub trait StubbornLink<T>: Send + Sync {
    async fn send(&self, payload: T, recipient: NodeId) -> Result<(), LinkError>;
    async fn listen(&self) -> Result<(), LinkError>;
    async fn close(&self);
    fn get_indications(&self) -> Arc<Mutex<Receiver<SblDeliver>>>;
}

pub struct RetransmitForever {
    indications: Arc<Mutex<Receiver<SblDeliver>>>,
    fair_loss_link: Arc<FairLossLink>,
    indications_tx: Mutex<Option<Sender<SblDeliver>>>,
    outbox: Arc<Mutex<Vec<OutboxEntry>>>,
    retransmission_period: Duration,
    shutdown_handler: ShutdownHandler,
}

#[async_trait]
impl StubbornLink<Value> for RetransmitForever {
    async fn send(&self, payload: Value, recipient: NodeId) -> Result<(), LinkError> {
        self.fair_loss_link.send(&payload, recipient).await?;
        self.outbox
            .lock()
            .await
            .push((recipient, Arc::from(payload)));
        Ok(())
    }

    async fn listen(&self) -> Result<(), LinkError> {
        tokio::try_join!(
            self.retransmit(),
            self.fair_loss_link.listen(),
            self.handle_indications()
        )?;
        Ok(())
    }

    async fn close(&self) {
        self.shutdown_handler.shutdown().await;
        self.fair_loss_link.close().await;
    }

    fn get_indications(&self) -> Arc<Mutex<Receiver<SblDeliver>>> {
        Arc::clone(&self.indications)
    }
}

impl RetransmitForever {
    pub fn new(retransmission_override: Option<Duration>, fair_loss_link: FairLossLink) -> Self {
        const DEFAULT_RETRANSMISSION_SECS: u64 = 5;
        const CHANNEL_BUFFER_SIZE: usize = 32;
        const OUTBOX_INITIAL_CAPACITY: usize = 1024;

        let shutdown_handler = ShutdownHandler::new();
        let (indications_tx, indications) = mpsc::channel::<SblDeliver>(CHANNEL_BUFFER_SIZE);

        RetransmitForever {
            fair_loss_link: Arc::from(fair_loss_link),
            outbox: Arc::new(Mutex::new(Vec::with_capacity(OUTBOX_INITIAL_CAPACITY))),
            indications: Arc::from(Mutex::new(indications)),
            indications_tx: Mutex::from(Some(indications_tx)),
            shutdown_handler,
            retransmission_period: retransmission_override
                .unwrap_or(Duration::from_secs(DEFAULT_RETRANSMISSION_SECS)),
        }
    }

    async fn retransmit(&self) -> Result<(), LinkError> {
        loop {
            select! {
                _ = self.shutdown_handler.wait() => { break },
                _ = tokio::time::sleep(self.retransmission_period) => {}
            }

            let messages: Vec<OutboxEntry>;
            {
                messages = self
                    .outbox
                    .lock()
                    .await
                    .iter()
                    .map(|(a, b)| (*a, Arc::clone(b)))
                    .collect();
            }

            for (peer_rank, msg) in messages.iter() {
                self.fair_loss_link.send(msg, *peer_rank).await?;
            }
        }
        Ok(())
    }

    async fn handle_indications(&self) -> Result<(), LinkError> {
        let indications_tx = self.indications_tx.lock().await.take();
        let fll_indications = self.fair_loss_link.get_indications();
        let mut fll_indications = fll_indications.lock().await;

        while let (Some(fll_indication), Some(tx)) =
            (fll_indications.recv().await, indications_tx.as_ref())
        {
            tx.send(fll_indication)
                .await
                .map_err(|e| InternalError(e.to_string()))?;
        }
        Ok(())
    }
}
