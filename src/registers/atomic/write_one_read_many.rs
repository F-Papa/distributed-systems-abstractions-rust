use async_trait::async_trait;
use dashmap::{DashMap, mapref::one::Ref};
use log::debug;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU16, AtomicU32, Ordering},
};
use tokio::sync::{Mutex, mpsc};

use crate::{
    broadcast::{self, best_effort::BestEffortBroadcast},
    common::NodeId,
    links::{
        LinkError::{self, InternalError},
        perfect_link::PerfectLink,
    },
    registers::RegisterIndication,
};

#[derive(Serialize, Deserialize, Debug)]
enum RegisterMessage<T> {
    Write {
        request_id: u32,
        timestamp: u32,
        value: T,
    },
    Ack {
        request_id: u32,
    },
    Read {
        request_id: u32,
    },
    Value {
        request_id: u32,
        ts: u32,
        value: T,
    },
}

#[async_trait]
pub trait OneManyAtomicRegister<T>: Send + Sync
where
    T: Send + Sync + Serialize,
{
    async fn read(&self) -> Result<(), LinkError>;
    async fn write(&self, value: T) -> Result<(), LinkError>;
    async fn take_indications(&self) -> Option<mpsc::Receiver<RegisterIndication<T>>>;
    async fn start(&self) -> Result<(), LinkError>;
    async fn stop(&self);
}

pub struct WriteOneReadManyAtomic<T>
where
    T: Send + Sync + Serialize,
{
    last_timestamp: AtomicU32,
    val: Arc<Mutex<Option<T>>>,
    write_timestamp: AtomicU32,
    acks: AtomicU16,
    request_id: AtomicU32,
    readlist: DashMap<NodeId, (u32, Option<T>)>,
    majority: u16,
    // readval: Arc<Mutex<Option<T>>>,
    reading: AtomicBool,
    indications: Arc<Mutex<Option<mpsc::Receiver<RegisterIndication<T>>>>>,
    indications_tx: Mutex<Option<mpsc::Sender<RegisterIndication<T>>>>,
    broadcast: Arc<dyn BestEffortBroadcast<Value>>,
    link: Arc<dyn PerfectLink<Value>>,
}

#[async_trait]
impl<T> OneManyAtomicRegister<T> for WriteOneReadManyAtomic<T>
where
    T: Send + Sync + Serialize + DeserializeOwned + Clone,
{
    async fn read(&self) -> Result<(), LinkError> {
        let request_id = self
            .request_id
            .update(Ordering::Relaxed, Ordering::Relaxed, |id| id + 1)
            + 1;

        self.acks.store(0, Ordering::Relaxed);
        self.readlist.clear();
        self.reading.store(true, Ordering::Relaxed);

        let encoded = serde_json::to_value(RegisterMessage::<T>::Read { request_id })
            .map_err(|e| InternalError(e.to_string()))?;
        self.broadcast.broadcast(&encoded).await
    }

    async fn write(&self, value: T) -> Result<(), LinkError> {
        let request_id = self
            .request_id
            .update(Ordering::Relaxed, Ordering::Relaxed, |id| id + 1)
            + 1;
        let write_timestamp =
            self.write_timestamp
                .update(Ordering::Relaxed, Ordering::Relaxed, |ts| ts + 1)
                + 1;
        self.acks.store(0, Ordering::Relaxed);

        let msg = RegisterMessage::Write {
            request_id,
            timestamp: write_timestamp,
            value,
        };

        let encoded = serde_json::to_value(msg).map_err(|e| InternalError(e.to_string()))?;
        self.broadcast.broadcast(&encoded).await
    }
    async fn take_indications(&self) -> Option<mpsc::Receiver<RegisterIndication<T>>> {
        self.indications.lock().await.take()
    }
    async fn start(&self) -> Result<(), LinkError> {
        tokio::try_join!(
            self.handle_broadcast(),
            self.handle_message(),
            self.link.listen(),
            self.broadcast.listen()
        )?;
        Ok(())
    }
    async fn stop(&self) {
        self.broadcast.close().await;
        self.link.close().await;
    }
}

impl<T> WriteOneReadManyAtomic<T>
where
    T: Send + Sync + Serialize + Clone + DeserializeOwned,
{
    pub fn new(
        number_of_nodes: u16,
        broadcast: Arc<dyn BestEffortBroadcast<Value>>,
        link: Arc<dyn PerfectLink<Value>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(32);
        Self {
            last_timestamp: AtomicU32::from(0),
            val: Arc::from(Mutex::from(None)),
            write_timestamp: AtomicU32::from(0),
            acks: AtomicU16::from(0),
            request_id: AtomicU32::from(0),
            readlist: DashMap::new(),
            majority: number_of_nodes / 2 + 1,
            reading: AtomicBool::from(false),
            indications: Arc::from(Mutex::from(Some(rx))),
            indications_tx: Mutex::from(Some(tx)),
            broadcast,
            link,
        }
    }
    async fn handle_message(&self) -> Result<(), LinkError> {
        let mut readval: Option<T> = None;
        let mut indications = match self.link.take_indications().await {
            None => return Err(LinkError::LinkClosed()),
            Some(rx) => rx,
        };

        let tx = match self.indications_tx.lock().await.take() {
            Some(tx) => tx,
            None => return Err(LinkError::LinkClosed()),
        };

        while let Some(delivery) = indications.recv().await {
            let message: RegisterMessage<Option<T>> =
                match serde_json::from_value(delivery.message.clone()) {
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
                if request_id != self.request_id.load(Ordering::Relaxed) {
                    continue;
                }

                self.readlist.insert(delivery.sender, (ts, value));

                debug!(
                    "#readlist: {} majority: {}",
                    self.readlist.len(),
                    self.majority
                );
                if self.readlist.len() >= self.majority as usize {
                    let (maxts, value) = self.highest_val(&self.readlist).unwrap().clone();
                    readval = value;

                    self.readlist.clear();

                    let msg = RegisterMessage::Write {
                        request_id,
                        timestamp: maxts,
                        value: &readval,
                    };
                    let encoded =
                        serde_json::to_value(msg).map_err(|e| InternalError(e.to_string()))?;
                    self.broadcast.broadcast(&encoded).await?;
                }
            } else if let RegisterMessage::Ack { request_id } = message {
                if request_id != self.request_id.load(Ordering::Relaxed) {
                    continue;
                }
                let acks = self
                    .acks
                    .update(Ordering::Relaxed, Ordering::Relaxed, |acks| acks + 1)
                    + 1;

                if acks >= self.majority {
                    self.acks.store(0, Ordering::Relaxed);
                    let indication: RegisterIndication<T>;
                    if self
                        .reading
                        .update(Ordering::Relaxed, Ordering::Relaxed, |_| false)
                    {
                        indication = RegisterIndication::ReadReturn(readval.clone());
                    } else {
                        indication = RegisterIndication::WriteReturn;
                    }

                    tx.send(indication)
                        .await
                        .map_err(|e| InternalError(e.to_string()))?;
                }
            }
        }
        Ok(())
    }

    fn highest_val<'a>(
        &'a self,
        readlist: &'a DashMap<NodeId, (u32, Option<T>)>,
    ) -> Option<Ref<'a, NodeId, (u32, Option<T>)>> {
        let highest_key = readlist
            .iter()
            .map(|entry| (*entry.key(), entry.value().0))
            .max_by_key(|(_, timestamp)| *timestamp)
            .map(|(key, _)| key)?;

        readlist.get(&highest_key)
    }

    async fn handle_broadcast(&self) -> Result<(), LinkError> {
        let mut indications = match self.broadcast.take_indications().await {
            None => return Err(LinkError::LinkClosed()),
            Some(rx) => rx,
        };

        while let Some(delivery) = indications.recv().await {
            let message: RegisterMessage<Option<T>> = match serde_json::from_value(delivery.message)
            {
                Ok(m) => m,
                Err(e) => {
                    debug!("error deserializing broadcast envelope: {e}");
                    continue;
                }
            };

            if let RegisterMessage::Write {
                request_id,
                timestamp,
                value,
            } = message
            {
                if timestamp > self.last_timestamp.load(Ordering::Relaxed) {
                    debug!("write with new timestamp {timestamp} received");
                    let mut val = self.val.lock().await;
                    *val = value;
                    self.last_timestamp.store(timestamp, Ordering::Relaxed);
                }
                let response = RegisterMessage::<T>::Ack { request_id };
                let encoded =
                    serde_json::to_value(response).map_err(|e| InternalError(e.to_string()))?;
                self.link.send(&encoded, delivery.sender).await?;
            } else if let RegisterMessage::Read { request_id } = message {
                let (ts, value) = {
                    let value = self.val.lock().await.clone();
                    (self.last_timestamp.load(Ordering::Relaxed), value)
                };

                let response = RegisterMessage::Value {
                    request_id,
                    ts,
                    value,
                };

                let encoded =
                    serde_json::to_value(&response).map_err(|e| InternalError(e.to_string()))?;
                self.link.send(&encoded, delivery.sender).await?;
            }
        }
        Ok(())
    }
}
