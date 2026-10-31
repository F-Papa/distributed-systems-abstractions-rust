use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::{
    fs::{self, create_dir_all, rename},
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Mutex, mpsc},
};

use crate::{common::NodeId, links::LinkError, registers::RegisterIndication};

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
    T: Send + Serialize + DeserializeOwned + Clone,
{
    state: Mutex<State<T>>,
    filepath: Box<Path>,
    indications: Arc<Mutex<Option<mpsc::Receiver<RegisterIndication<T>>>>>,
    indications_tx: Mutex<Option<mpsc::Receiver<RegisterIndication<T>>>>,
}

impl<T> LoggedMajorityVoting<T>
where
    T: Send + Serialize + DeserializeOwned + Clone,
{
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
}
