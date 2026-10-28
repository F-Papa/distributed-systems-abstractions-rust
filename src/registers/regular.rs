use std::sync::{
    Arc,
    atomic::{AtomicU16, AtomicU32, Ordering},
};

use async_trait::async_trait;
use dashmap::{DashMap, mapref::one::Ref};
use log::debug;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::sync::{Mutex, mpsc};

use crate::{
    broadcast::best_effort::BestEffortBroadcast,
    common::NodeId,
    links::{
        LinkError::{self, InternalError},
        perfect_link::PerfectLink,
    },
    registers::RegisterIndication,
};

#[derive(Serialize, Deserialize, Debug)]
enum RegisterMessage<T> {
    Write { timestamp: u32, value: T },
    Ack { timestamp: u32 },
    Read { request_id: u32 },
    Value { request_id: u32, ts: u32, value: T },
}

#[async_trait]
pub trait OneManyRegularRegister<T>: Send + Sync
where
    T: Send + Sync + Serialize,
{
    async fn read(&self) -> Result<(), LinkError>;
    async fn write(&self, value: T) -> Result<(), LinkError>;
    async fn take_indications(&self) -> Option<mpsc::Receiver<RegisterIndication<T>>>;
    async fn start(&self) -> Result<(), LinkError>;
    async fn stop(&self);
}

pub struct WriteOneReadAll<T>
where
    T: Send + Sync + Serialize,
{
    majority: u16,
    timestamp: AtomicU32,
    val: Arc<Mutex<Option<T>>>,
    write_timestamp: AtomicU32,
    acks: AtomicU16,
    current_request_id: AtomicU32,
    readlist: DashMap<NodeId, (u32, T)>,
    link: Arc<dyn PerfectLink<Value>>,
    broadcast: Arc<dyn BestEffortBroadcast<Value>>,
    indications: Arc<Mutex<Option<mpsc::Receiver<RegisterIndication<T>>>>>,
    indications_tx: Mutex<Option<mpsc::Sender<RegisterIndication<T>>>>,
}

#[async_trait]
impl<T> OneManyRegularRegister<T> for WriteOneReadAll<T>
where
    T: Send + Sync + Serialize + DeserializeOwned + Clone,
{
    async fn take_indications(&self) -> Option<mpsc::Receiver<RegisterIndication<T>>> {
        self.indications.lock().await.take()
    }

    async fn start<'a>(&'a self) -> Result<(), LinkError> {
        tokio::try_join!(
            self.handle_messages(),
            self.handle_broadcasts(),
            self.broadcast.listen(),
            self.link.listen()
        )?;
        Ok(())
    }

    async fn stop(&self) {
        self.link.close().await;
        self.broadcast.close().await;
    }

    async fn read(&self) -> Result<(), LinkError> {
        let request_id =
            self.current_request_id
                .update(Ordering::Relaxed, Ordering::Relaxed, |rid| rid + 1)
                + 1;
        self.readlist.clear();
        let message: RegisterMessage<T> = RegisterMessage::Read { request_id };
        let encoded = serde_json::to_value(message).map_err(|e| InternalError(e.to_string()))?;
        self.broadcast.broadcast(&encoded).await
    }

    async fn write(&self, value: T) -> Result<(), LinkError> {
        debug!("writing");
        let timestamp = self
            .write_timestamp
            .update(Ordering::Relaxed, Ordering::Relaxed, |ts| ts + 1)
            + 1;
        self.acks.store(0, Ordering::Relaxed);
        debug!("writing with timestamp {timestamp}");
        let message: RegisterMessage<T> = RegisterMessage::Write { timestamp, value };
        let encoded = serde_json::to_value(message).map_err(|e| InternalError(e.to_string()))?;
        self.broadcast.broadcast(&encoded).await
    }
}

impl<T> WriteOneReadAll<T>
where
    T: Send + Sync + Serialize + DeserializeOwned + Clone,
{
    pub fn new(
        number_of_nodes: u16,
        link: Arc<dyn PerfectLink<Value>>,
        broadcast: Arc<dyn BestEffortBroadcast<Value>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(32);
        Self {
            majority: number_of_nodes / 2 + 1,
            timestamp: AtomicU32::new(0),
            val: Arc::from(Mutex::from(None)),
            write_timestamp: AtomicU32::new(0),
            acks: AtomicU16::new(0),
            current_request_id: AtomicU32::new(0),
            readlist: DashMap::with_capacity(number_of_nodes as usize),
            link,
            broadcast,
            indications: Arc::from(Mutex::from(Some(rx))),
            indications_tx: Mutex::from(Some(tx)),
        }
    }

    async fn handle_broadcasts(&self) -> Result<(), LinkError> {
        let mut indications = match self.broadcast.take_indications().await {
            None => return Err(LinkError::LinkClosed()),
            Some(rx) => rx,
        };

        while let Some(delivery) = indications.recv().await {
            let message: RegisterMessage<T> = match serde_json::from_value(delivery.message) {
                Ok(m) => m,
                Err(e) => {
                    debug!("error deserializing broadcast envelope: {e}");
                    continue;
                }
            };

            if let RegisterMessage::Write { timestamp, value } = message {
                if timestamp > self.timestamp.load(Ordering::Relaxed) {
                    let mut val = self.val.lock().await;
                    *val = Some(value);
                    self.timestamp.store(timestamp, Ordering::Relaxed);
                    let response = RegisterMessage::<T>::Ack { timestamp };
                    let encoded =
                        serde_json::to_value(response).map_err(|e| InternalError(e.to_string()))?;
                    self.link.send(&encoded, delivery.sender).await?;
                }
            } else if let RegisterMessage::Read { request_id } = message {
                let val = self.val.lock().await;
                let response = RegisterMessage::Value {
                    request_id,
                    ts: self.timestamp.load(Ordering::Relaxed),
                    value: &*val,
                };
                let encoded =
                    serde_json::to_value(&response).map_err(|e| InternalError(e.to_string()))?;
                self.link.send(&encoded, delivery.sender).await?;
            }
        }

        Ok(())
    }

    async fn handle_messages(&self) -> Result<(), LinkError> {
        let mut indications = match self.link.take_indications().await {
            None => return Err(LinkError::LinkClosed()),
            Some(rx) => rx,
        };

        let tx = match self.indications_tx.lock().await.take() {
            Some(tx) => tx,
            None => return Err(LinkError::LinkClosed()),
        };

        while let Some(delivery) = indications.recv().await {
            let message: RegisterMessage<T> = match serde_json::from_value(delivery.message.clone())
            {
                Ok(m) => m,
                Err(e) => {
                    debug!("error deserializing link envelope: {e}");
                    debug!("message: {:?}", delivery.message);
                    continue;
                }
            };

            if let RegisterMessage::Value {
                request_id,
                ts,
                value,
            } = message
            {
                if request_id == self.current_request_id.load(Ordering::Relaxed) {
                    self.readlist.insert(delivery.sender, (ts, value));
                }

                debug!(
                    "#readlist: {} majority: {}",
                    self.readlist.len(),
                    self.majority
                );
                if self.readlist.len() >= self.majority as usize {
                    let v = &self.highest_val(&self.readlist).unwrap().1;
                    let read_return = RegisterIndication::ReadReturn(v.clone());
                    tx.send(read_return)
                        .await
                        .map_err(|e| InternalError(e.to_string()))?;
                }
            } else if let RegisterMessage::Ack { timestamp } = message {
                if timestamp == self.write_timestamp.load(Ordering::Relaxed) {
                    let acks = self
                        .acks
                        .update(Ordering::Relaxed, Ordering::Relaxed, |acks| acks + 1)
                        + 1;

                    if acks >= self.majority {
                        self.acks.store(0, Ordering::Relaxed);
                        tx.send(RegisterIndication::WriteReturn)
                            .await
                            .map_err(|e| InternalError(e.to_string()))?;
                    }
                }
            }
        }
        Ok(())
    }

    fn highest_val<'a>(
        &'a self,
        readlist: &'a DashMap<NodeId, (u32, T)>,
    ) -> Option<Ref<'a, NodeId, (u32, T)>> {
        let highest_key = readlist
            .iter()
            .map(|entry| (*entry.key(), entry.value().0))
            .max_by_key(|(_, timestamp)| *timestamp)
            .map(|(key, _)| key)?;

        readlist.get(&highest_key)
    }
}
