use log::debug;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    net::UdpSocket,
    select,
    sync::{Mutex, mpsc},
};

use crate::{
    common::ShutdownHandler,
    links::{LinkError, MAX_LENGTH, NodeId, Resolver},
};

#[derive(Debug)]
pub struct FllDeliver {
    pub message: Value,
    pub sender: NodeId,
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use super::*;

    struct AddressResolver(String);

    impl Resolver for AddressResolver {
        fn resolve(&self, _: NodeId) -> String {
            self.0.clone()
        }
    }

    #[tokio::test]
    async fn indications_have_one_owner_and_still_receive_messages() {
        let node_id = NodeId::new(1);
        let mut link = FairLossLink::new(node_id, Box::new(AddressResolver("127.0.0.1:0".into())))
            .await
            .unwrap();
        link.resolver = Box::new(AddressResolver(
            link.socket.local_addr().unwrap().to_string(),
        ));
        let link = Arc::new(link);

        let (first, second) = tokio::join!(link.take_indications(), link.take_indications());
        let mut receiver = match (first, second) {
            (Some(receiver), None) | (None, Some(receiver)) => receiver,
            _ => panic!("exactly one consumer must obtain the receiver"),
        };
        assert!(link.take_indications().await.is_none());

        let listener = Arc::clone(&link);
        let listener = tokio::spawn(async move { listener.listen().await });
        let message = serde_json::json!({ "message": "hello" });
        link.send(&message, node_id).await.unwrap();
        let received = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await;

        link.close().await;
        tokio::time::timeout(Duration::from_secs(2), listener)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        let received = received.unwrap().unwrap();
        assert_eq!(received.sender, node_id);
        assert_eq!(received.message, message);
        assert!(receiver.recv().await.is_none());
    }
}

pub struct FairLossLink {
    pub socket: UdpSocket,
    pub resolver: Box<dyn Resolver>,
    indications: Mutex<Option<mpsc::Receiver<FllDeliver>>>,
    indications_tx: Mutex<Option<mpsc::Sender<FllDeliver>>>,
    local_id: NodeId,
    shutdown_handler: ShutdownHandler,
}

#[derive(Debug, Serialize, Deserialize)]
struct Envelope<T> {
    sender: NodeId,
    recipient: NodeId,
    payload: T,
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
            indications: Mutex::new(Some(rx)),
            indications_tx: Mutex::new(Some(tx)),
            local_id,
            shutdown_handler: ShutdownHandler::new(),
        };

        Ok(fll)
    }

    /// Transfers the receiver to its consumer. Returns `None` after the first take.
    pub async fn take_indications(&self) -> Option<mpsc::Receiver<FllDeliver>> {
        self.indications.lock().await.take()
    }

    pub async fn send(&self, msg: &Value, recipient: NodeId) -> Result<(), LinkError> {
        let addr = self.resolver.resolve(recipient);

        let envelope = Envelope {
            sender: self.local_id,
            recipient,
            payload: msg,
        };

        let bytes =
            serde_json::to_vec(&envelope).map_err(|e| LinkError::MessageError(e.to_string()))?;

        if bytes.len() > MAX_LENGTH {
            return Err(LinkError::MessageError(format!(
                "Message too long: {} vs {}",
                bytes.len(),
                MAX_LENGTH
            )));
        }

        self.socket
            .send_to(&bytes, addr)
            .await
            .map_err(|e| LinkError::SocketError(e.to_string()))?;
        debug!("Sending {} bytes to {}", bytes.len(), recipient);

        Ok(())
    }

    pub async fn close(&self) {
        self.shutdown_handler.shutdown().await
    }

    pub async fn listen(&self) -> Result<(), LinkError> {
        let mut indications_tx = self.indications_tx.lock().await;
        if indications_tx.is_none() {
            return Err(LinkError::LinkClosed());
        }

        loop {
            let mut buf = [0_u8; MAX_LENGTH];
            let len = select! {
                _ = self.shutdown_handler.wait() => { break }

                result = self.socket.recv(&mut buf) => {
                    result.map_err(|e| LinkError::SocketError(e.to_string()))?
                }
            };

            let envelope: Envelope<Value> = match serde_json::from_slice(&buf[..len]) {
                Err(e) => {
                    debug!("invalid message received ({e}): {:?}", &buf[..len]);
                    continue;
                }
                Ok(e) => e,
            };

            debug!("{} bytes from {}", len, envelope.sender);
            if envelope.recipient != self.local_id {
                debug!(
                    "dropped message from {} because of wrong recipient {}",
                    envelope.sender, envelope.recipient
                );
                continue;
            }

            let indication = FllDeliver {
                message: envelope.payload,
                sender: envelope.sender,
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
}
