use async_trait::async_trait;
use log::debug;
use serde::{Serialize, de::DeserializeOwned};
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use tokio::sync::{Mutex, mpsc};

use crate::{
    links::LinkError::{self},
    registers::{RegisterIndication, regular::OneManyRegularRegister},
};

#[async_trait]
pub trait OneOneAtomicRegister<T>: Send + Sync
where
    T: Send + Sync + Serialize,
{
    async fn read(&self) -> Result<(), LinkError>;
    async fn write(&self, value: T) -> Result<(), LinkError>;
    async fn take_indications(&self) -> Option<mpsc::Receiver<RegisterIndication<T>>>;
    async fn start(&self) -> Result<(), LinkError>;
    async fn stop(&self);
}

pub struct WriteOneReadOneAtomic<T>
where
    T: Send + Sync + Serialize,
{
    write_timestamp: AtomicU32,
    last_timestamp: AtomicU32,
    val: Arc<Mutex<Option<T>>>,
    indications: Arc<Mutex<Option<mpsc::Receiver<RegisterIndication<T>>>>>,
    indications_tx: Mutex<Option<mpsc::Sender<RegisterIndication<T>>>>,
    register: Arc<dyn OneManyRegularRegister<(u32, T)>>,
}

#[async_trait]
impl<T> OneOneAtomicRegister<T> for WriteOneReadOneAtomic<T>
where
    T: Send + Sync + Serialize + DeserializeOwned + Clone,
{
    async fn read(&self) -> Result<(), LinkError> {
        self.register.read().await
    }

    async fn write(&self, value: T) -> Result<(), LinkError> {
        let write_timestamp =
            self.write_timestamp
                .update(Ordering::Relaxed, Ordering::Relaxed, |ts| ts + 1)
                + 1;

        self.register.write((write_timestamp, value)).await
    }

    async fn take_indications(&self) -> Option<mpsc::Receiver<RegisterIndication<T>>> {
        self.indications.lock().await.take()
    }

    async fn start(&self) -> Result<(), LinkError> {
        tokio::try_join!(self.register.start(), self.handle_indications())?;
        Ok(())
    }

    async fn stop(&self) {
        self.register.stop().await
    }
}

impl<T> WriteOneReadOneAtomic<T>
where
    T: Send + Sync + Serialize + DeserializeOwned + Clone,
{
    pub fn new(register: Arc<dyn OneManyRegularRegister<(u32, T)>>) -> Self {
        let (tx, rx) = mpsc::channel(32);
        Self {
            last_timestamp: AtomicU32::new(0),
            write_timestamp: AtomicU32::new(0),
            val: Arc::from(Mutex::from(None)),
            indications: Arc::from(Mutex::from(Some(rx))),
            indications_tx: Mutex::from(Some(tx)),
            register,
        }
    }

    async fn handle_indications(&self) -> Result<(), LinkError> {
        let mut indications = match self.register.take_indications().await {
            None => return Err(LinkError::LinkClosed()),
            Some(rx) => rx,
        };

        let tx = match self.indications_tx.lock().await.take() {
            None => return Err(LinkError::LinkClosed()),
            Some(tx) => tx,
        };

        while let Some(indication) = indications.recv().await {
            match indication {
                RegisterIndication::ReadReturn(Some((timestamp, value))) => {
                    let last_timestamp = self.last_timestamp.load(Ordering::Relaxed);
                    debug!(
                        "read timestamp: {}, last_timestamp: {}",
                        timestamp, last_timestamp
                    );
                    if timestamp > last_timestamp {
                        let mut value_guard = self.val.lock().await;
                        *value_guard = Some(value.clone());
                        self.last_timestamp.store(timestamp, Ordering::Relaxed);
                        tx.send(RegisterIndication::<T>::ReadReturn(Some(value)))
                            .await
                            .map_err(|e| LinkError::InternalError(e.to_string()))?;
                    }
                }
                RegisterIndication::ReadReturn(None) => {
                    tx.send(RegisterIndication::<T>::ReadReturn(None))
                        .await
                        .map_err(|e| LinkError::InternalError(e.to_string()))?;
                }
                RegisterIndication::WriteReturn => {
                    tx.send(RegisterIndication::<T>::WriteReturn)
                        .await
                        .map_err(|e| LinkError::InternalError(e.to_string()))?;
                }
            }
        }
        Ok(())
    }
}
