use std::{collections::LinkedList, sync::Arc, time::Duration};

use log::{debug, error};
use serde_json::Value;
use tokio::{
    select,
    sync::{Mutex, Notify, mpsc},
    task::JoinHandle,
};

use crate::links::{
    LinkError, NodeId, Resolver,
    fair_loss_link::{FairLossLink, FllDeliver},
};

type OutboxEntry = (NodeId, Arc<Value>);

pub type SblDeliver = FllDeliver;

pub struct RetransmitForever {
    pub fair_loss_link: Arc<FairLossLink>,
    pub indications: Arc<Mutex<mpsc::Receiver<SblDeliver>>>,
    indications_tx: Mutex<Option<mpsc::Sender<SblDeliver>>>,
    fll_indications: Arc<Mutex<mpsc::Receiver<FllDeliver>>>,
    outbox: Arc<Mutex<LinkedList<OutboxEntry>>>,
    shutdown: Arc<Notify>,
    retransmission_handle: Mutex<Option<JoinHandle<()>>>,
}

impl RetransmitForever {
    pub async fn new(
        local_id: NodeId,
        resolver: Box<dyn Resolver>,
        retransmission_override: Option<Duration>,
    ) -> Result<Self, LinkError> {
        const DEFAULT_RETRANSMISSION_SECS: u64 = 5;

        let fair_loss_link = Arc::new(FairLossLink::new(local_id, resolver).await?);
        let fll_indications = Arc::clone(&fair_loss_link.indications);
        let outbox = Arc::new(Mutex::new(LinkedList::new()));
        let shutdown = Arc::new(Notify::new());

        let fll_copy = Arc::clone(&fair_loss_link);
        let outbox_copy = Arc::clone(&outbox);
        let shutdown_cpy = Arc::clone(&shutdown);

        let retransmission_handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = tokio::time::sleep(
                        retransmission_override.unwrap_or(
                            Duration::from_secs(DEFAULT_RETRANSMISSION_SECS)
                        )
                    ) => {}

                    _ = shutdown_cpy.notified() => {
                        break
                    },

                }

                let messages: Vec<OutboxEntry>;
                {
                    messages = outbox_copy
                        .lock()
                        .await
                        .iter()
                        .map(|(a, b)| (*a, Arc::clone(b)))
                        .collect();
                }

                for (peer_rank, msg) in messages.iter() {
                    if let Err(e) = fll_copy.send(msg, *peer_rank).await {
                        error!("Error sending message: {:?}", e);
                    }
                }
            }
        });

        let (tx, rx) = mpsc::channel::<SblDeliver>(32);

        let stubborn_link = RetransmitForever {
            fair_loss_link,
            outbox,
            indications: Arc::from(Mutex::new(rx)),
            indications_tx: Mutex::from(Some(tx)),
            fll_indications,
            shutdown,
            retransmission_handle: Mutex::new(Some(retransmission_handle)),
        };

        Ok(stubborn_link)
    }

    pub async fn send(&self, msg: Arc<Value>, recipient: NodeId) -> Result<(), LinkError> {
        self.fair_loss_link.send(msg.as_ref(), recipient).await?;

        self.outbox.lock().await.push_back((recipient, msg));
        Ok(())
    }

    pub async fn listen(&self) -> Result<(), LinkError> {
        let fair_loss_link_cpy = Arc::clone(&self.fair_loss_link);
        tokio::try_join!(fair_loss_link_cpy.listen(), self.handle_indications())?;
        debug!("finished");
        Ok(())
    }

    async fn handle_indications(&self) -> Result<(), LinkError> {
        let mut indications_tx = self.indications_tx.lock().await;
        let mut fll_indications = self.fll_indications.lock().await;
        loop {
            match fll_indications.recv().await {
                None => break,
                Some(indication) => {
                    debug!("fll deliver");
                    if indications_tx
                        .as_ref()
                        .expect("get indications_tx")
                        .send(indication)
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
        *indications_tx = None;
        Ok(())
    }

    pub async fn close(&self) {
        self.fair_loss_link.close().await;
        self.shutdown.notify_one();
        if let Some(handle) = self.retransmission_handle.lock().await.take() {
            handle.await.unwrap()
        };
    }
}
