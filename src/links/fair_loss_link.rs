use std::sync::Arc;

use tokio::{
    net::UdpSocket,
    select,
    sync::{Mutex, mpsc},
};

use crate::links::{LinkError, MAX_LENGTH, Resolver, signals};

pub struct FllDeliver<T> {
    pub message: Arc<[u8]>,
    pub sender: T,
}

pub struct FairLossLink<T> {
    pub socket: UdpSocket,
    pub resolver: Box<dyn Resolver<T, String>>,
    pub indications: Arc<Mutex<mpsc::Receiver<FllDeliver<T>>>>,
    indications_tx: Mutex<Option<mpsc::Sender<FllDeliver<T>>>>,
}

impl FairLossLink<u16> {
    pub async fn new(
        local_rank: u16,
        resolver: Box<dyn Resolver<u16, String>>,
    ) -> Result<Self, LinkError> {
        let socket = UdpSocket::bind(resolver.resolve(local_rank))
            .await
            .map_err(|e| LinkError::SocketError(e.to_string()))?;

        let (tx, rx) = mpsc::channel::<FllDeliver<u16>>(32);

        let fll = FairLossLink {
            socket,
            resolver,
            indications: Arc::from(Mutex::new(rx)),
            indications_tx: Mutex::new(Some(tx)),
        };

        Ok(fll)
    }

    pub async fn send(&self, msg: &[u8], recipient: u16) -> Result<(), LinkError> {
        let addr = self.resolver.resolve(recipient);
        self.socket
            .send_to(msg, addr)
            .await
            .map_err(|e| LinkError::SocketError(e.to_string()))?;
        Ok(())
    }

    pub async fn listen(&self) -> Result<(), LinkError> {
        let (mut sigint, mut sigterm) = signals();

        let mut indications_tx = self.indications_tx.lock().await;
        loop {
            let mut buf = [0_u8; MAX_LENGTH];
            select! {
                _ = sigterm.recv() => {
                    break;
                }

                _ = sigint.recv() => {
                    break;
                }

                result = self.socket.recv(&mut buf) => {
                    result.map_err(|e| LinkError::SocketError(e.to_string()))?;
                }
            }

            let indication = FllDeliver {
                message: Arc::from(buf),
                sender: 0_u16,
            };

            indications_tx
                .as_ref()
                .expect("get indications_tx")
                .send(indication)
                .await
                .map_err(|e| LinkError::InternalError(e.to_string()))?;
        }

        *indications_tx = None;
        Ok(())
    }
}
