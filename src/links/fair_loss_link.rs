use std::sync::Arc;

use log::debug;
use tokio::{
    net::UdpSocket,
    select,
    sync::{Mutex, Notify, mpsc},
};

use crate::links::{LinkError, MAX_LENGTH, NodeId, Resolver};

pub struct FllDeliver {
    pub message: Arc<[u8]>,
    pub sender: NodeId,
}

pub struct FairLossLink {
    pub socket: UdpSocket,
    pub resolver: Box<dyn Resolver>,
    pub indications: Arc<Mutex<mpsc::Receiver<FllDeliver>>>,
    indications_tx: Mutex<Option<mpsc::Sender<FllDeliver>>>,
    local_id: NodeId,
    shutdown: Arc<Notify>,
}

impl FairLossLink {
    pub async fn new(local_id: NodeId, resolver: Box<dyn Resolver>) -> Result<Self, LinkError> {
        let socket = UdpSocket::bind(resolver.resolve(local_id))
            .await
            .map_err(|e| LinkError::SocketError(e.to_string()))?;

        let (tx, rx) = mpsc::channel::<FllDeliver>(32);

        let fll = FairLossLink {
            socket,
            resolver,
            indications: Arc::from(Mutex::new(rx)),
            indications_tx: Mutex::new(Some(tx)),
            local_id,
            shutdown: Arc::new(Notify::new()),
        };

        Ok(fll)
    }

    pub async fn send(&self, msg: &[u8], recipient: NodeId) -> Result<(), LinkError> {
        let addr = self.resolver.resolve(recipient);

        if msg.len() > MAX_LENGTH {
            return Err(LinkError::MessageError(format!(
                "Message too long: {} vs {}",
                msg.len(),
                MAX_LENGTH
            )));
        }

        self.socket
            .send_to(&self.add_envelope(msg), addr)
            .await
            .map_err(|e| LinkError::SocketError(e.to_string()))?;

        Ok(())
    }

    pub async fn close(&self) {
        self.shutdown.notify_one();
    }

    pub async fn listen(&self) -> Result<(), LinkError> {
        let mut indications_tx = self.indications_tx.lock().await;
        if indications_tx.is_none() {
            return Err(LinkError::LinkClosed());
        }

        loop {
            let mut buf = [0_u8; MAX_LENGTH];
            let len = select! {
                _ = self.shutdown.notified() => {
                    break
                }

                result = self.socket.recv(&mut buf) => {
                    result.map_err(|e| LinkError::SocketError(e.to_string()))?
                }
            };

            let (id, payload) = self.take_envelope(&buf[..len])?;
            debug!("{:?} from {id}", payload);

            let indication = FllDeliver {
                message: Arc::from(payload),
                sender: id,
            };

            indications_tx
                .as_ref()
                .expect("get indications_tx")
                .send(indication)
                .await
                .map_err(|e| LinkError::InternalError(e.to_string()))?;
        }

        *indications_tx = None;
        debug!("finished");
        Ok(())
    }

    fn add_envelope(&self, msg: &[u8]) -> Vec<u8> {
        let mut result = Vec::with_capacity(2 + msg.len());
        result.extend_from_slice(&self.local_id.to_be_bytes());
        result.extend_from_slice(msg);
        result
    }

    fn take_envelope<'a>(&self, msg: &'a [u8]) -> Result<(NodeId, &'a [u8]), LinkError> {
        if msg.len() < NodeId::ENCODED_LEN {
            return Err(LinkError::MessageError("Message to short".to_owned()));
        }

        let (envelope, payload) = msg.split_at(NodeId::ENCODED_LEN);
        let id_bytes: [u8; NodeId::ENCODED_LEN] = envelope.try_into().map_err(|e| {
            LinkError::MessageError(format!("Could not obtain sender rank: {:?}", e))
        })?;

        Ok((NodeId::from_be_bytes(id_bytes), payload))
    }
}
