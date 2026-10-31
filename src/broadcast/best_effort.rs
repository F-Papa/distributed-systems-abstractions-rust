use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use serde::Serialize;
use tokio::{
    sync::{Mutex, mpsc},
    try_join,
};

use crate::{
    common::NodeId,
    links::{
        LinkError,
        perfect_link::{PerfectLink, PlDeliver},
    },
};

pub type BeDeliver = PlDeliver;

#[async_trait]
pub trait BestEffortBroadcast<T>: Send + Sync
where
    T: Sync + Serialize,
{
    async fn broadcast<'a>(&'a self, payload: &'a T) -> Result<(), LinkError>;
    async fn listen(&self) -> Result<(), LinkError>;
    async fn close(&self);
    /// Transfers the receiver to its consumer. Returns `None` after the first take.
    async fn take_indications(&self) -> Option<mpsc::Receiver<BeDeliver>>;
}

pub struct BasicBroadcast<T> {
    link: Arc<dyn PerfectLink<T>>,
    indications: Arc<Mutex<Option<mpsc::Receiver<BeDeliver>>>>,
    indications_tx: Mutex<Option<mpsc::Sender<BeDeliver>>>,
    nodes: HashSet<NodeId>,
}

#[async_trait]
impl<T> BestEffortBroadcast<T> for BasicBroadcast<T>
where
    T: Sync + Serialize,
{
    async fn broadcast<'a>(&'a self, payload: &'a T) -> Result<(), LinkError>
    where
        T: 'a,
    {
        for n in self.nodes.iter() {
            self.link.send(payload, *n).await?;
        }
        Ok(())
    }
    async fn listen(&self) -> Result<(), LinkError> {
        try_join!(self.handle_link_indications(), self.link.listen())?;
        Ok(())
    }

    async fn close(&self) {
        self.link.close().await
    }
    /// Transfers the receiver to its consumer. Returns `None` after the first take.
    async fn take_indications(&self) -> Option<mpsc::Receiver<BeDeliver>> {
        self.indications.lock().await.take()
    }
}

impl<T> BasicBroadcast<T>
where
    T: Sync + Serialize,
{
    pub fn new(link: Arc<dyn PerfectLink<T>>, nodes: HashSet<NodeId>) -> Self {
        let (indications_tx, indications) = mpsc::channel(32);

        Self {
            nodes,
            link,
            indications_tx: Mutex::from(Some(indications_tx)),
            indications: Arc::from(Mutex::from(Some(indications))),
        }
    }
    async fn handle_link_indications(&self) -> Result<(), LinkError> {
        let mut link_indications = match self.link.take_indications().await {
            None => return Err(LinkError::LinkClosed()),
            Some(i) => i,
        };

        let indications_tx = match self.indications_tx.lock().await.take() {
            None => return Err(LinkError::LinkClosed()),
            Some(i) => i,
        };

        while let Some(i) = link_indications.recv().await {
            indications_tx
                .send(i)
                .await
                .map_err(LinkError::with_action("send indication"))?;
        }

        Ok(())
    }
}
