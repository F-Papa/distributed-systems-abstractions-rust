use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
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

struct State<T> {
    timestamp: u32,
    write_timestamp: u32,
    val: Option<T>,
    acks: u32,
    request_id: u32,
    readlist: HashMap<NodeId, (u32, Option<T>)>,
}

pub struct WriteOneReadAll<T>
where
    T: Send + Sync + Serialize,
{
    state: Mutex<State<T>>,
    majority: u16,
    link: Arc<dyn PerfectLink<Value>>,
    broadcast: Arc<dyn BestEffortBroadcast<Value>>,
    indications: Arc<Mutex<Option<mpsc::Receiver<RegisterIndication<T>>>>>,
    indications_tx: Mutex<Option<mpsc::Sender<RegisterIndication<T>>>>,
}

#[derive(Serialize, Deserialize, Debug)]
enum RegisterMessage<T> {
    Write { timestamp: u32, value: T },
    Ack { timestamp: u32 },
    Read { request_id: u32 },
    Value { request_id: u32, ts: u32, value: T },
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
        let mut state = self.state.lock().await;
        state.request_id += 1;
        state.readlist.clear();
        let message: RegisterMessage<T> = RegisterMessage::Read {
            request_id: state.request_id,
        };
        let encoded = serde_json::to_value(message).map_err(|e| InternalError(e.to_string()))?;
        self.broadcast.broadcast(&encoded).await
    }

    async fn write(&self, value: T) -> Result<(), LinkError> {
        debug!("writing");
        let mut state = self.state.lock().await;
        state.write_timestamp += 1;
        state.acks = 0;
        debug!("writing with timestamp {}", state.timestamp);
        let message: RegisterMessage<T> = RegisterMessage::Write {
            timestamp: state.write_timestamp,
            value,
        };
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
            state: Mutex::from(State {
                timestamp: 0,
                acks: 0,
                write_timestamp: 0,
                request_id: 0,
                readlist: HashMap::new(),
                val: None,
            }),
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
            let message: RegisterMessage<Option<T>> = match serde_json::from_value(delivery.message)
            {
                Ok(m) => m,
                Err(e) => {
                    debug!("error deserializing broadcast envelope: {e}");
                    continue;
                }
            };

            let mut state = self.state.lock().await;
            if let RegisterMessage::Write { timestamp, value } = message {
                if timestamp > state.timestamp {
                    state.val = value;
                    state.timestamp = timestamp;
                    let response = RegisterMessage::<T>::Ack { timestamp };
                    let encoded =
                        serde_json::to_value(response).map_err(|e| InternalError(e.to_string()))?;
                    self.link.send(&encoded, delivery.sender).await?;
                }
            } else if let RegisterMessage::Read { request_id } = message {
                let response = RegisterMessage::Value {
                    request_id,
                    ts: state.timestamp,
                    value: &state.val,
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
            let message: RegisterMessage<Option<T>> =
                match serde_json::from_value(delivery.message.clone()) {
                    Ok(m) => m,
                    Err(e) => {
                        debug!("error deserializing link envelope: {e}");
                        debug!("message: {:?}", delivery.message);
                        continue;
                    }
                };

            let mut state = self.state.lock().await;
            if let RegisterMessage::Value {
                request_id,
                ts,
                value,
            } = message
            {
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
                    let highest_val = match &self.highest_val(&state.readlist) {
                        Some(v) => Some(v.1.clone()),
                        None => None,
                    };
                    let read_return = RegisterIndication::ReadReturn(highest_val.unwrap());
                    tx.send(read_return)
                        .await
                        .map_err(|e| InternalError(e.to_string()))?;
                }
            } else if let RegisterMessage::Ack { timestamp } = message {
                if timestamp == state.write_timestamp {
                    state.acks += 1;

                    if state.acks >= self.majority as u32 {
                        state.acks = 0;
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
        readlist: &'a HashMap<NodeId, (u32, Option<T>)>,
    ) -> Option<(u32, Option<T>)> {
        readlist
            .iter()
            .max_by_key(|(_, (ts, _))| *ts)
            .map(|(_, (ts, val))| (*ts, val.clone()))
    }
}
