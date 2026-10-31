use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use log::debug;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::sync::{Mutex, mpsc};

use crate::{
    broadcast::best_effort::BestEffortBroadcast,
    common::{NodeId, marshal, unmarshal},
    links::{LinkError, perfect_link::PerfectLink},
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

struct State<T> {
    timestamp: u32,
    write_rank: u16,
    value: Option<T>,
    request_id: u32,
    readval: Option<T>,
    writeval: Option<T>,
    reading: bool,
    readlist: HashMap<NodeId, (u32, u16, Option<T>)>,
    acks: u16,
}

pub struct ConsultMajorityRegister<T>
where
    T: Send + Sync + Serialize,
{
    state: Mutex<State<T>>,
    number_of_nodes: u16,
    local_id: NodeId,
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
            state: Mutex::from(State {
                reading: false,
                readval: None,
                readlist: HashMap::new(),
                writeval: None,
                timestamp: 0,
                request_id: 0,
                write_rank: 0,
                acks: 0,
                value: None,
            }),
            number_of_nodes,
            local_id,
            indications: Arc::from(Mutex::from(Some(rx))),
            indications_tx: Mutex::from(Some(tx)),
            broadcast,
            link,
        }
    }
    fn highest_val<'a>(
        &'a self,
        readlist: &'a HashMap<NodeId, (u32, u16, Option<T>)>,
    ) -> Option<(u32, u16, Option<T>)> {
        readlist
            .iter()
            .map(|(key, value)| (*key, value.0, value.1, value.2.clone()))
            .max_by(|a, b| a.1.cmp(&b.1).then_with(|| a.2.cmp(&b.2)))
            .map(|a| (a.1, a.2, a.3))
    }

    async fn handle_write(
        &self,
        sender: NodeId,
        write: RegisterMessage<Option<T>>,
    ) -> Result<(), LinkError> {
        if let RegisterMessage::Write {
            request_id,
            ts,
            write_rank,
            value,
        } = write
        {
            let response = {
                let mut state = self.state.lock().await;
                if ts > state.timestamp || (ts == state.timestamp && write_rank > state.write_rank)
                {
                    debug!("new write received: (ts={}, wr={})", ts, write_rank);
                    (state.value, state.timestamp, state.write_rank) = (value, ts, write_rank);
                }
                let response = RegisterMessage::<T>::Ack { request_id };
                marshal(response)?
            };

            self.link.send(&response, sender).await?;
        }
        Ok(())
    }

    async fn handle_read(
        &self,
        sender: NodeId,
        read: RegisterMessage<Option<T>>,
    ) -> Result<(), LinkError> {
        if let RegisterMessage::Read { request_id } = read {
            let response = {
                let state = self.state.lock().await;
                let response = RegisterMessage::Value {
                    request_id,
                    ts: state.timestamp,
                    write_rank: state.write_rank,
                    value: &state.value,
                };

                marshal(&response)?
            };

            self.link.send(&response, sender).await?;
        }
        Ok(())
    }

    async fn handle_ack(
        &self,
        ack: RegisterMessage<Option<T>>,
        tx: &mpsc::Sender<RegisterIndication<T>>,
    ) -> Result<(), LinkError> {
        if let RegisterMessage::Ack { request_id } = ack {
            let mut indication = None;
            {
                let mut state = self.state.lock().await;
                if request_id == state.request_id {
                    state.acks += 1;

                    if state.acks > self.number_of_nodes / 2 {
                        if state.reading {
                            state.reading = false;
                            indication =
                                Some(RegisterIndication::ReadReturn(state.readval.clone()));
                        } else {
                            indication = Some(RegisterIndication::WriteReturn);
                        }
                    }
                }
            }
            if let Some(indication) = indication {
                tx.send(indication)
                    .await
                    .map_err(LinkError::with_action("send indication"))?;
            }
        }
        Ok(())
    }

    async fn handle_value(
        &self,
        sender: NodeId,
        value: RegisterMessage<Option<T>>,
    ) -> Result<(), LinkError> {
        if let RegisterMessage::Value {
            request_id,
            ts,
            write_rank,
            value,
        } = value
        {
            let mut message = None;
            {
                let mut state = self.state.lock().await;
                if request_id != state.request_id {
                    return Ok(());
                }
                state.readlist.insert(sender, (ts, write_rank, value));
                if state.readlist.len() > (self.number_of_nodes / 2) as usize {
                    let (maxts, rr, readval) = self.highest_val(&state.readlist).unwrap();
                    state.readval = readval;
                    state.readlist.clear();

                    let msg: RegisterMessage<&Option<T>>;
                    if state.reading {
                        msg = RegisterMessage::<&Option<T>>::Write {
                            request_id: state.request_id,
                            ts: maxts,
                            write_rank: rr,
                            value: &state.readval,
                        };
                    } else {
                        msg = RegisterMessage::<&Option<T>>::Write {
                            request_id: state.request_id,
                            ts: maxts + 1,
                            write_rank: self.local_id.get(),
                            value: &state.writeval,
                        };
                    }
                    message = Some(marshal(msg)?);
                }
            }

            if let Some(m) = message {
                self.broadcast.broadcast(&m).await?;
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
            let message: RegisterMessage<Option<T>> = match unmarshal(delivery.message.clone()) {
                Ok(m) => m,
                Err(e) => {
                    debug!("error deserializing link envelope: {e:?}");
                    debug!("message: {:?}", delivery.message);
                    continue;
                }
            };

            if matches!(message, RegisterMessage::Ack { .. }) {
                self.handle_ack(message, &tx).await?;
            } else if matches!(message, RegisterMessage::Value { .. }) {
                self.handle_value(delivery.sender, message).await?;
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
            let message: RegisterMessage<Option<T>> = match unmarshal(delivery.message) {
                Ok(m) => m,
                Err(e) => {
                    debug!("error deserializing broadcast envelope: {e:?}");
                    continue;
                }
            };

            if matches!(message, RegisterMessage::Write { .. }) {
                self.handle_write(delivery.sender, message).await?;
            } else if matches!(message, RegisterMessage::Read { .. }) {
                self.handle_read(delivery.sender, message).await?;
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
        let response = {
            let mut state = self.state.lock().await;
            state.request_id += 1;
            state.acks = 0;
            state.readlist.clear();
            state.reading = true;

            marshal(RegisterMessage::<T>::Read {
                request_id: state.request_id,
            })?
        };

        self.broadcast.broadcast(&response).await
    }

    async fn write(&self, value: T) -> Result<(), LinkError> {
        let response = {
            let mut state = self.state.lock().await;
            state.request_id += 1;
            state.writeval = Some(value);
            state.acks = 0;
            state.readlist.clear();

            marshal(RegisterMessage::<T>::Read {
                request_id: state.request_id,
            })?
        };
        self.broadcast.broadcast(&response).await
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
