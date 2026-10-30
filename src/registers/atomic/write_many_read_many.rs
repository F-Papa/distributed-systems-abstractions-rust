use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU16, AtomicU32, Ordering},
};

use async_trait::async_trait;
use dashmap::{DashMap, mapref::one::Ref};
use log::debug;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::sync::{Mutex, mpsc};
use uuid::timestamp;

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
    Write {
        request_id: u32,
        ts: u32,
        write_rank: u16,
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
        write_rank: u16,
        value: T,
    },
}

#[async_trait]
pub trait AtomicRegister<T>: Send + Sync
where
    T: Send + Sync + Serialize,
{
    async fn read(&self) -> Result<(), LinkError>;
    async fn write(&self, value: T) -> Result<(), LinkError>;
    async fn take_indications(&self) -> Option<mpsc::Receiver<RegisterIndication<T>>>;
    async fn start(&self) -> Result<(), LinkError>;
    async fn stop(&self);
}

pub struct ConsultMajorityRegister<T>
where
    T: Send + Sync + Serialize,
{
    timestamp: AtomicU32,
    write_rank: AtomicU16,
    value: Mutex<Option<T>>,
    request_id: AtomicU32,
    readlist: DashMap<NodeId, (u32, u16, Option<T>)>,
    readval: Mutex<Option<T>>,
    writeval: Mutex<Option<T>>,
    majority: u16,
    reading: AtomicBool,

    local_id: NodeId,
    acks: AtomicU16,
    indications: Arc<Mutex<Option<mpsc::Receiver<RegisterIndication<T>>>>>,
    indications_tx: Mutex<Option<mpsc::Sender<RegisterIndication<T>>>>,
    broadcast: Arc<dyn BestEffortBroadcast<Value>>,
    link: Arc<dyn PerfectLink<Value>>,
}

impl<T> ConsultMajorityRegister<T>
where
    T: Send + Sync + Serialize + DeserializeOwned + Clone,
{
    pub fn new(
        local_id: NodeId,
        number_of_nodes: u16,
        broadcast: Arc<dyn BestEffortBroadcast<Value>>,
        link: Arc<dyn PerfectLink<Value>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(32);
        Self {
            value: Mutex::from(None),
            majority: number_of_nodes / 2 + 1,
            local_id,
            timestamp: AtomicU32::from(0),
            write_rank: AtomicU16::from(0),
            acks: AtomicU16::from(0),
            request_id: AtomicU32::from(0),
            writeval: Mutex::from(None),
            readval: Mutex::from(None),
            readlist: DashMap::new(),
            reading: AtomicBool::from(false),
            indications: Arc::from(Mutex::from(Some(rx))),
            indications_tx: Mutex::from(Some(tx)),
            broadcast,
            link,
        }
    }
    fn highest_val<'a>(
        &'a self,
        readlist: &'a DashMap<NodeId, (u32, u16, Option<T>)>,
    ) -> Option<Ref<'a, NodeId, (u32, u16, Option<T>)>> {
        let highest_key = readlist
            .iter()
            .map(|entry| (*entry.key(), entry.value().1, entry.value().0))
            .max_by(|a, b| a.2.cmp(&b.2).then_with(|| a.1.cmp(&b.1)))
            .map(|(key, _, _)| key)?;

        readlist.get(&highest_key)
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
                write_rank,
                value,
            } = message
            {
                if request_id != self.request_id.load(Ordering::Relaxed) {
                    continue;
                }
                self.readlist
                    .insert(delivery.sender, (ts, write_rank, value));

                debug!(
                    "#readlist: {} majority: {}",
                    self.readlist.len(),
                    self.majority
                );
                if self.readlist.len() >= self.majority as usize {
                    let (maxts, rr, readval) = self.highest_val(&self.readlist).unwrap().clone();
                    *self.readval.lock().await = readval.clone();
                    self.readlist.clear();
                    debug!("new value!");

                    let msg: RegisterMessage<Option<T>>;
                    if self.reading.load(Ordering::Relaxed) {
                        msg = RegisterMessage::<Option<T>>::Write {
                            request_id: self.request_id.load(Ordering::Relaxed),
                            ts: maxts,
                            write_rank: rr,
                            value: readval,
                        };
                    } else {
                        msg = RegisterMessage::<Option<T>>::Write {
                            request_id: self.request_id.load(Ordering::Relaxed),
                            ts: maxts + 1,
                            write_rank: self.local_id.get(),
                            value: (*self.writeval.lock().await).clone(),
                        };
                    }
                    let encoded =
                        serde_json::to_value(msg).map_err(|e| InternalError(e.to_string()))?;
                    self.broadcast.broadcast(&encoded).await?;
                }
            } else if let RegisterMessage::Ack { request_id } = message {
                if request_id == self.request_id.load(Ordering::Relaxed) {
                    let acks = self
                        .acks
                        .update(Ordering::Relaxed, Ordering::Relaxed, |acks| acks + 1)
                        + 1;

                    let indication: RegisterIndication<T>;
                    if acks >= self.majority {
                        if self
                            .reading
                            .update(Ordering::Relaxed, Ordering::Relaxed, |_| false)
                        {
                            indication =
                                RegisterIndication::ReadReturn((*self.readval.lock().await).clone());
                        } else {
                            indication = RegisterIndication::WriteReturn;
                        }
                        tx.send(indication)
                            .await
                            .map_err(|e| InternalError(e.to_string()))?;
                    }
                }
            }
        }
        Ok(())
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
                ts,
                write_rank,
                value,
            } = message
            {
                let last_timestamp = self.timestamp.load(Ordering::Relaxed);
                if ts > last_timestamp
                    || ts == last_timestamp && write_rank > self.write_rank.load(Ordering::Relaxed)
                {
                    debug!("new write received: (ts={}, wr={})", ts, write_rank);
                    *self.value.lock().await = value;
                    self.timestamp.store(ts, Ordering::Relaxed);
                    self.write_rank.store(write_rank, Ordering::Relaxed);
                }
                let response = RegisterMessage::<T>::Ack { request_id };
                let encoded =
                    serde_json::to_value(response).map_err(|e| InternalError(e.to_string()))?;
                self.link.send(&encoded, delivery.sender).await?;
            } else if let RegisterMessage::Read { request_id } = message {
                let response = RegisterMessage::Value {
                    request_id,
                    ts: self.timestamp.load(Ordering::Relaxed),
                    write_rank: self.write_rank.load(Ordering::Relaxed),
                    value: &(*self.value.lock().await),
                };

                let encoded =
                    serde_json::to_value(&response).map_err(|e| InternalError(e.to_string()))?;

                self.link.send(&encoded, delivery.sender).await?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<T> AtomicRegister<T> for ConsultMajorityRegister<T>
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
        *self.writeval.lock().await = Some(value);
        self.acks.store(0, Ordering::Relaxed);
        self.readlist.clear();

        let encoded = serde_json::to_value(RegisterMessage::<T>::Read { request_id })
            .map_err(|e| InternalError(e.to_string()))?;
        self.broadcast.broadcast(&encoded).await
    }

    async fn take_indications(&self) -> Option<mpsc::Receiver<RegisterIndication<T>>> {
        self.indications.lock().await.take()
    }

    async fn start(&self) -> Result<(), LinkError> {
        tokio::try_join!(
            self.link.listen(),
            self.broadcast.listen(),
            self.handle_broadcast(),
            self.handle_messages()
        )?;
        Ok(())
    }

    async fn stop(&self) {
        self.broadcast.close().await;
        self.link.close().await;
    }
}
