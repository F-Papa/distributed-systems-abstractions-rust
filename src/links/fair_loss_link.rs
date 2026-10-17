use tokio::net::UdpSocket;

use crate::links::{Callback, LinkError, MAX_LENGTH, Resolver};

pub struct FairLossLink<T> {
    pub socket: UdpSocket,
    pub callback: Callback,
    pub resolver: Box<dyn Resolver<T, String>>,
}

impl FairLossLink<u16> {
    pub async fn new(
        local_rank: u16,
        resolver: Box<dyn Resolver<u16, String>>,
        callback: Callback,
    ) -> Result<Self, LinkError> {
        let socket = UdpSocket::bind(resolver.resolve(local_rank))
            .await
            .map_err(|e| LinkError::SocketError(e.to_string()))?;

        let fll = FairLossLink {
            socket,
            callback,
            resolver,
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
        loop {
            let mut buf = [0_u8; MAX_LENGTH];
            let len = self
                .socket
                .recv(&mut buf)
                .await
                .map_err(|e| LinkError::SocketError(e.to_string()))?;

            (self.callback)(&buf[..len])?;
        }
    }
}
