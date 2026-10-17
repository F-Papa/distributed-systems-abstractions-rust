use std::{
    collections::LinkedList,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::links::{Callback, LinkError, Resolver, fair_loss_link::FairLossLink};

type OutboxEntry = (u16, Arc<[u8]>);

pub struct StubbornLink<T> {
    pub fair_loss_link: Arc<FairLossLink<T>>,
    outbox: Arc<Mutex<LinkedList<OutboxEntry>>>,
}

impl StubbornLink<u16> {
    pub async fn new(
        local_rank: u16,
        resolver: Box<dyn Resolver<u16, String>>,
        callback: Callback,
    ) -> Result<Self, LinkError> {
        let fair_loss_link = FairLossLink::new(local_rank, resolver, callback).await?;

        let stubborn_link = StubbornLink {
            fair_loss_link: Arc::new(fair_loss_link),
            outbox: Arc::new(Mutex::new(LinkedList::new())),
        };

        let fll_copy = Arc::clone(&stubborn_link.fair_loss_link);
        let outbox_copy = Arc::clone(&stubborn_link.outbox);

        _ = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let mut messages: Option<Vec<OutboxEntry>> = None;

                if let Ok(outbox) = &outbox_copy.lock() {
                    messages = Some(outbox.iter().map(|(a, b)| (*a, Arc::clone(&b))).collect());
                }

                match messages {
                    None => return,
                    Some(messages) => {
                        for (peer_rank, msg) in messages.iter() {
                            if let Err(e) = fll_copy.send(msg, *peer_rank).await {
                                println!("Error sending message: {:?}", e);
                            }
                        }
                    }
                }
            }
        });

        Ok(stubborn_link)
    }

    pub async fn send(&self, msg: &[u8], recipient: u16) -> Result<(), LinkError> {
        self.fair_loss_link.send(msg, recipient).await?;
        let mut outbox = self.outbox.lock().expect("Could not acquire outbox lock");
        outbox.push_back((recipient, Arc::from(msg)));
        Ok(())
    }

    pub async fn listen(&self) -> Result<(), LinkError> {
        self.fair_loss_link.listen().await
    }
}
