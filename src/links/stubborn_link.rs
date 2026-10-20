use std::{collections::LinkedList, sync::Arc, time::Duration};

use serde_json::Value;
use tokio::{
    select,
    sync::{Mutex, mpsc, watch},
};

use crate::{
    common::BoxFuture,
    links::{
        LinkError::{self, InternalError},
        NodeId,
        fair_loss_link::{FairLossLink, FllDeliver},
    },
};

type OutboxEntry = (NodeId, Arc<Value>);

pub type SblDeliver = FllDeliver;

pub trait StubbornLink<T>: Send + Sync {
    fn send(&self, payload: T, recipient: NodeId) -> BoxFuture<'_, Result<(), LinkError>>;
    fn listen(&self) -> BoxFuture<'_, Result<(), LinkError>>;
    fn close(&self) -> BoxFuture<'_, ()>;
    fn get_indications(&self) -> Arc<Mutex<mpsc::Receiver<SblDeliver>>>;
}

pub struct RetransmitForever {
    indications: Arc<Mutex<mpsc::Receiver<SblDeliver>>>,
    fair_loss_link: Arc<FairLossLink>,
    indications_tx: Mutex<Option<mpsc::Sender<SblDeliver>>>,
    outbox: Arc<Mutex<LinkedList<OutboxEntry>>>,
    shutdown: Arc<Mutex<watch::Receiver<bool>>>,
    shutdown_tx: Arc<watch::Sender<bool>>,
    retransmission_period: Duration,
}

impl StubbornLink<Value> for RetransmitForever {
    fn send(&self, payload: Value, recipient: NodeId) -> BoxFuture<'_, Result<(), LinkError>> {
        Box::pin(async move {
            self.fair_loss_link.send(&payload, recipient).await?;
            self.outbox
                .lock()
                .await
                .push_back((recipient, Arc::from(payload)));
            Ok(())
        })
    }

    fn listen(&self) -> BoxFuture<'_, Result<(), LinkError>> {
        Box::pin(async move {
            tokio::try_join!(
                self.retransmit(),
                self.fair_loss_link.listen(),
                self.handle_indications()
            )?;
            Ok(())
        })
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.shutdown_tx.send_replace(true);
            self.fair_loss_link.close().await;
        })
    }

    fn get_indications(&self) -> Arc<Mutex<mpsc::Receiver<SblDeliver>>> {
        Arc::clone(&self.indications)
    }
}

impl RetransmitForever {
    pub fn new(retransmission_override: Option<Duration>, fair_loss_link: FairLossLink) -> Self {
        const DEFAULT_RETRANSMISSION_SECS: u64 = 5;

        let (shutdown_tx, shutdown) = watch::channel(false);
        let (indications_tx, indications) = mpsc::channel::<SblDeliver>(32);

        RetransmitForever {
            fair_loss_link: Arc::from(fair_loss_link),
            outbox: Arc::new(Mutex::new(LinkedList::new())),
            indications: Arc::from(Mutex::new(indications)),
            indications_tx: Mutex::from(Some(indications_tx)),
            shutdown: Arc::from(Mutex::from(shutdown)),
            shutdown_tx: Arc::from(shutdown_tx),
            retransmission_period: retransmission_override
                .unwrap_or(Duration::from_secs(DEFAULT_RETRANSMISSION_SECS)),
        }
    }

    async fn retransmit(&self) -> Result<(), LinkError> {
        let mut shutdown_watch = self.shutdown.lock().await;
        loop {
            select! {
                _ = shutdown_watch.wait_for(|v| *v) => { break },
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
        let mut fll_indications = self.fair_loss_link.indications.lock().await;

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
