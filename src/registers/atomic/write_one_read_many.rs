use async_trait::async_trait;
use log::debug;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};
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

struct State<T> {
    timestamp: u32,
    val: Option<T>,
    write_timestamp: u32,
    acks: u16,
    request_id: u32,
    readlist: HashMap<NodeId, (u32, Option<T>)>,
    reading: bool,
}

pub struct WriteOneReadManyAtomic<T>
where
    T: Send + Sync + Serialize,
{
    state: Mutex<State<T>>,
    majority: u16,
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
        let message = {
            let mut state = self.state.lock().await;
            state.request_id += 1;
            state.acks = 0;
            state.readlist.clear();
            state.reading = true;

            serde_json::to_value(RegisterMessage::<T>::Read {
                request_id: state.request_id,
            })
            .map_err(|e| InternalError(e.to_string()))?
        };
        self.broadcast.broadcast(&message).await
    }

    async fn write(&self, value: T) -> Result<(), LinkError> {
        let message = {
            let mut state = self.state.lock().await;
            state.request_id += 1;
            state.write_timestamp += 1;
            state.acks = 0;

            let msg = RegisterMessage::Write {
                request_id: state.request_id,
                timestamp: state.write_timestamp,
                value,
            };

            serde_json::to_value(msg).map_err(|e| InternalError(e.to_string()))?
        };
        self.broadcast.broadcast(&message).await
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
            state: Mutex::from(State {
                request_id: 0,
                timestamp: 0,
                acks: 0,
                write_timestamp: 0,
                reading: false,
                val: None,
                readlist: HashMap::new(),
            }),
            majority: number_of_nodes / 2 + 1,
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

            let mut response: Option<Value> = None;
            let mut indication = None;
            if let RegisterMessage::Value {
                request_id,
                ts,
                value,
            } = message
            {
                let mut state = self.state.lock().await;
                if request_id != state.request_id {
                    continue;
                }

                state.readlist.insert(delivery.sender, (ts, value));

                debug!(
                    "#readlist: {} majority: {}",
                    state.readlist.len(),
                    self.majority
                );
                if state.readlist.len() >= self.majority as usize {
                    let (maxts, value) = self.highest_val(&state.readlist).unwrap().clone();
                    readval = value;

                    state.readlist.clear();

                    let msg = RegisterMessage::Write {
                        request_id,
                        timestamp: maxts,
                        value: &readval,
                    };
                    response =
                        Some(serde_json::to_value(msg).map_err(|e| InternalError(e.to_string()))?);
                }
            } else if let RegisterMessage::Ack { request_id } = message {
                let mut state = self.state.lock().await;
                if request_id != state.request_id {
                    continue;
                }
                state.acks += 1;

                if state.acks >= self.majority {
                    state.acks = 0;
                    if state.reading {
                        state.reading = false;
                        indication = Some(RegisterIndication::ReadReturn(readval.clone()));
                    } else {
                        indication = Some(RegisterIndication::WriteReturn);
                    }
                }
            }

            if let Some(indication) = indication {
                tx.send(indication)
                    .await
                    .map_err(|e| InternalError(e.to_string()))?;
            }

            if let Some(r) = response {
                self.broadcast.broadcast(&r).await?;
            }
        }
        Ok(())
    }

    fn highest_val<'a>(
        &'a self,
        readlist: &'a HashMap<NodeId, (u32, Option<T>)>,
    ) -> Option<(u32, Option<T>)> {
        readlist
            .iter()
            .max_by_key(|(_, (timestamp, _))| *timestamp)
            .map(|(_, (ts, val))| (*ts, val.clone()))
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

            let mut response: Option<Value> = None;
            if let RegisterMessage::Write {
                request_id,
                timestamp,
                value,
            } = message
            {
                let mut state = self.state.lock().await;
                if timestamp > state.timestamp {
                    debug!("write with new timestamp {timestamp} received");
                    state.val = value;
                    state.timestamp = timestamp;
                }
                response = Some(
                    serde_json::to_value(RegisterMessage::<T>::Ack { request_id })
                        .map_err(|e| InternalError(e.to_string()))?,
                );
            } else if let RegisterMessage::Read { request_id } = message {
                let state = self.state.lock().await;
                let message = RegisterMessage::Value {
                    request_id,
                    ts: state.timestamp,
                    value: &state.val,
                };

                response =
                    Some(serde_json::to_value(&message).map_err(|e| InternalError(e.to_string()))?);
            }

            if let Some(r) = response {
                self.link.send(&r, delivery.sender).await?;
            }
        }
        Ok(())
    }
}
