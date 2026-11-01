use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
};

use async_trait::async_trait;
use log::debug;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::{
    fs::{self, create_dir_all, rename},
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Mutex, mpsc},
};

use crate::{
    broadcast::stubborn::StubbornBroadcast,
    common::{NodeId, marshal, unmarshal},
    links::{LinkError, stubborn_link::StubbornLink},
    registers::RegisterIndication::{self, ReadReturn, WriteReturn},
};

#[derive(Serialize, Deserialize, Debug)]
enum RegisterMessage<T> {
    Write { request_id: u32, ts: u32, value: T },
    Ack { request_id: u32 },
    Read { request_id: u32 },
    Value { request_id: u32, ts: u32, value: T },
}

#[async_trait]
pub trait LoggedRegularRegister<T>: Send + Sync
where
    T: Send + Serialize + DeserializeOwned + Clone,
{
    async fn start(&self) -> Result<(), LinkError>;
    async fn stop(&self);
    async fn write(&self, value: T) -> Result<(), LinkError>;
    async fn read(&self) -> Result<(), LinkError>;
    async fn take_indications(&self) -> Option<mpsc::Receiver<RegisterIndication<T>>>;
}

#[derive(Serialize, Deserialize)]
struct State<T> {
    timestamp: u32,
    val: Option<T>,
    write_timestamp: u32,
    request_id: u32,
    acklist: HashSet<NodeId>,
    readlist: HashMap<NodeId, (u32, Option<T>)>,
    writing: bool,
    reading: bool,
}

impl<T> Default for State<T> {
    fn default() -> Self {
        Self {
            timestamp: 0,

            write_timestamp: 0,
            val: None,
            request_id: 0,
            acklist: HashSet::new(),
            readlist: HashMap::new(),
            writing: false,
            reading: false,
        }
    }
}

pub struct LoggedMajorityVoting<T>
where
    T: Sync + Send + Serialize + DeserializeOwned + Clone,
{
    state: Mutex<State<T>>,
    number_of_nodes: u16,
    filepath: Box<Path>,
    indications: Arc<Mutex<Option<mpsc::Receiver<RegisterIndication<T>>>>>,
    indications_tx: Mutex<Option<mpsc::Sender<RegisterIndication<T>>>>,
    broadcast: Arc<dyn StubbornBroadcast<Value>>,
    link: Arc<dyn StubbornLink<Value>>,
}

#[async_trait]
impl<T> LoggedRegularRegister<T> for LoggedMajorityVoting<T>
where
    T: Sync + Send + Serialize + DeserializeOwned + Clone,
{
    async fn start(&self) -> Result<(), LinkError> {
        let message = {
            let mut state = self.state.lock().await;
            if let Ok(mut s) = self.retrieve().await {
                s.readlist.clear();
                s.reading = false;
                *state = s;
            }
            if state.writing {
                state.request_id += 1;
                Some(RegisterMessage::Write {
                    request_id: state.request_id,
                    ts: state.timestamp,
                    value: state.val.clone(),
                })
            } else {
                None
            }
        };

        if let Some(m) = message {
            self.broadcast.broadcast(&marshal(m)?).await?;
        }

        tokio::try_join!(
            self.handle_messages(),
            self.handle_broadcast(),
            self.broadcast.listen(),
            self.link.listen()
        )?;
        Ok(())
    }

    async fn stop(&self) {
        self.broadcast.close().await;
        self.link.close().await;
    }

    async fn write(&self, value: T) -> Result<(), LinkError> {
        let message = {
            let mut state = self.state.lock().await;
            state.write_timestamp += 1;
            (state.timestamp, state.val) = (state.write_timestamp, Some(value.clone()));
            state.request_id += 1;
            state.acklist.clear();
            state.writing = true;
            self.store(&state).await?;
            RegisterMessage::Write {
                request_id: state.request_id,
                ts: state.write_timestamp,
                value,
            }
        };

        self.broadcast.broadcast(&marshal(message)?).await
    }
    async fn read(&self) -> Result<(), LinkError> {
        let message = {
            let mut state = self.state.lock().await;
            state.request_id += 1;
            state.readlist.clear();
            state.reading = true;

            RegisterMessage::<T>::Read {
                request_id: state.request_id,
            }
        };

        self.broadcast.broadcast(&marshal(message)?).await
    }

    async fn take_indications(&self) -> Option<mpsc::Receiver<RegisterIndication<T>>> {
        self.indications.lock().await.take()
    }
}

impl<T> LoggedMajorityVoting<T>
where
    T: Sync + Send + Serialize + DeserializeOwned + Clone,
{
    pub fn new(
        id: NodeId,
        number_of_nodes: u16,
        broadcast: Arc<dyn StubbornBroadcast<Value>>,
        link: Arc<dyn StubbornLink<Value>>,
        filepath_override: Option<String>,
    ) -> Self {
        let (indications_tx, indications) = mpsc::channel(32);
        let filepath = filepath_override.unwrap_or(format!("./data/register_{}.log", id));

        Self {
            number_of_nodes,
            state: Mutex::from(State::default()),
            filepath: Box::from(Path::new(&filepath)),
            indications_tx: Mutex::from(Some(indications_tx)),
            indications: Arc::from(Mutex::from(Some(indications))),
            broadcast,
            link,
        }
    }

    async fn store(&self, state: &State<T>) -> Result<(), LinkError> {
        if let Some(parent_dir) = self.filepath.parent()
            && !parent_dir.exists()
        {
            create_dir_all(parent_dir)
                .await
                .map_err(LinkError::with_action("create register log directory"))?;
        }

        let tmp_filepath = (self.filepath.to_string_lossy() + ".tmp").to_string();
        let mut file = fs::File::create(&tmp_filepath)
            .await
            .map_err(LinkError::with_action("create temporary register log"))?;

        let encoded =
            serde_json::to_string(state).map_err(LinkError::with_action("encode register log"))?;

        file.write_all(encoded.as_bytes())
            .await
            .map_err(LinkError::with_action("write register log"))?;

        rename(tmp_filepath, &self.filepath)
            .await
            .map_err(LinkError::with_action("persist register log"))
    }

    async fn retrieve(&self) -> Result<State<T>, LinkError> {
        if !self.filepath.exists() {
            return Ok(State::default());
        }
        let mut file = fs::File::open(&self.filepath)
            .await
            .map_err(LinkError::with_action("open register log"))?;
        // match

        let mut encoded = String::new();
        file.read_to_string(&mut encoded)
            .await
            .map_err(LinkError::with_action("read register log"))?;

        let state: State<T> = serde_json::from_str(&encoded)
            .map_err(LinkError::with_action("decode register log"))?;

        Ok(state)
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
            let message: RegisterMessage<Option<T>> = match unmarshal(delivery.message) {
                Ok(m) => m,
                Err(e) => {
                    debug!("error unmarshaling message: {:?}", e);
                    continue;
                }
            };

            if let RegisterMessage::Ack { request_id } = message {
                let mut indication: Option<RegisterIndication<T>> = None;
                {
                    let mut state = self.state.lock().await;
                    if request_id != state.request_id {
                        continue;
                    }
                    state.acklist.insert(delivery.sender);
                    if state.writing && state.acklist.len() > (self.number_of_nodes / 2) as usize {
                        state.acklist.clear();
                        state.writing = false;
                        self.store(&state).await?;
                        indication = Some(WriteReturn {});
                    }
                }

                if let Some(i) = indication {
                    tx.send(i)
                        .await
                        .map_err(LinkError::with_action("send write return indication"))?;
                }
            } else if let RegisterMessage::Value {
                request_id,
                ts,
                value,
            } = message
            {
                let mut indication: Option<RegisterIndication<T>> = None;
                {
                    let mut state = self.state.lock().await;
                    if request_id != state.request_id {
                        continue;
                    }

                    state.readlist.insert(delivery.sender, (ts, value.clone()));
                    debug!(
                        "#readlist: {}, majority: {}",
                        state.readlist.len(),
                        self.number_of_nodes / 2 + 1
                    );
                    if state.reading && state.readlist.len() > (self.number_of_nodes / 2) as usize {
                        state.val = self.highestval(&state.readlist);
                        state.readlist.clear();
                        state.reading = false;
                        indication = Some(ReadReturn(value));
                    }
                }

                if let Some(i) = indication {
                    tx.send(i)
                        .await
                        .map_err(LinkError::with_action("send read return indication"))?;
                }
            }
        }
        Ok(())
    }

    fn highestval(&self, readlist: &HashMap<NodeId, (u32, Option<T>)>) -> Option<T> {
        readlist
            .iter()
            .max_by_key(|(_, (ts, _))| *ts)
            .map(|(_, (_, value))| value.clone())
            .unwrap_or(None)
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
                    debug!("error unmarshaling message: {:?}", e);
                    continue;
                }
            };

            if let RegisterMessage::Write {
                request_id,
                ts,
                value,
            } = message
            {
                let response = {
                    let mut state = self.state.lock().await;
                    if ts > state.timestamp {
                        (state.timestamp, state.val) = (ts, (value));
                        self.store(&state).await?;
                    }
                    RegisterMessage::<&T>::Ack { request_id }
                };
                self.link.send(&marshal(response)?, delivery.sender).await?;
            } else if let RegisterMessage::Read { request_id } = message {
                let response = {
                    let state = self.state.lock().await;
                    RegisterMessage::Value {
                        request_id,
                        ts: state.timestamp,
                        value: state.val.clone(),
                    }
                };

                self.link.send(&marshal(response)?, delivery.sender).await?;
            }
        }
        /*Note that stubborn links are implemented by retransmitting messages periodi-
        cally; however, this retransmission can be stopped by a writer and a reader once it
        has logged the reply from some process or, at the latest, once it receives enough
        replies and has completed its operation.*/
        Ok(())
    }
}
