use std::{collections::LinkedList, sync::Arc, time::Duration};

use tokio::sync::{Mutex, mpsc};

use crate::links::{
    LinkError, Resolver,
    fair_loss_link::{FairLossLink, FllDeliver},
};

type OutboxEntry = (u16, Arc<[u8]>);

pub type SblDeliver<T> = FllDeliver<T>;

pub struct StubbornLink<T> {
    pub fair_loss_link: Arc<FairLossLink<T>>,
    pub indications: Arc<Mutex<mpsc::Receiver<SblDeliver<T>>>>,
    indications_tx: Mutex<Option<mpsc::Sender<SblDeliver<T>>>>,
    fll_indications: Arc<Mutex<mpsc::Receiver<FllDeliver<T>>>>,
    outbox: Arc<Mutex<LinkedList<OutboxEntry>>>,
}

impl StubbornLink<u16> {
    pub async fn new(
        local_rank: u16,
        resolver: Box<dyn Resolver<u16, String>>,
        retransmission_override: Option<Duration>,
    ) -> Result<Self, LinkError> {
        const DEFAULT_RETRANSMISSION_SECS: u64 = 5;

        let fair_loss_link = FairLossLink::new(local_rank, resolver).await?;
        let fll_indications = Arc::clone(&fair_loss_link.indications);

        let (tx, rx) = mpsc::channel::<SblDeliver<u16>>(32);

        let stubborn_link = StubbornLink {
            fair_loss_link: Arc::new(fair_loss_link),
            outbox: Arc::new(Mutex::new(LinkedList::new())),
            indications: Arc::from(Mutex::new(rx)),
            indications_tx: Mutex::from(Some(tx)),
            fll_indications,
        };

        let fll_copy = Arc::clone(&stubborn_link.fair_loss_link);
        let outbox_copy = Arc::clone(&stubborn_link.outbox);

        _ = tokio::spawn(async move {
            loop {
                tokio::time::sleep(
                    retransmission_override
                        .unwrap_or(Duration::from_secs(DEFAULT_RETRANSMISSION_SECS)),
                )
                .await;

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
                        println!("Error sending message: {:?}", e);
                    }
                }
            }
        });

        Ok(stubborn_link)
    }

    pub async fn send(&self, msg: &[u8], recipient: u16) -> Result<(), LinkError> {
        self.fair_loss_link.send(msg, recipient).await?;
        self.outbox
            .lock()
            .await
            .push_back((recipient, Arc::from(msg)));

        Ok(())
    }

    pub async fn listen(&self) -> Result<(), LinkError> {
        let fair_loss_link_cpy = Arc::clone(&self.fair_loss_link);
        let fll_handle = tokio::spawn(async move { fair_loss_link_cpy.listen().await });

        let mut indications_tx = self.indications_tx.lock().await;
        let mut fll_indications = self.fll_indications.lock().await;
        loop {
            match fll_indications.recv().await {
                None => break,
                Some(indication) => {
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
        fll_handle.await.expect("join fll_handle")
    }

    pub async fn close(&self) {
        self.fair_loss_link.close().await;
    }
}
