use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};
use uuid;

use crate::links::stubborn_link::StubbornLink;
use crate::links::{Callback, LinkError, Resolver};

pub struct PerfectLink<T> {
    pub stubborn_link: Arc<StubbornLink<T>>,
}

impl PerfectLink<u16> {
    pub async fn new(
        local_rank: u16,
        resolver: Box<dyn Resolver<u16, String>>,
        callback: Callback,
    ) -> Result<Self, LinkError> {
        let inbox = Arc::new(Mutex::new(HashSet::new()));
        let callback = internal_callback(Arc::clone(&inbox), callback);
        let stubborn_link = StubbornLink::new(local_rank, resolver, callback).await?;

        let perfect_link = PerfectLink {
            stubborn_link: Arc::new(stubborn_link),
        };

        Ok(perfect_link)
    }

    pub async fn send(&self, msg: &[u8], recipient: u16) -> Result<(), LinkError> {
        self.stubborn_link.send(&add_envelope(msg), recipient).await
    }

    pub async fn listen(self: Arc<Self>) -> Result<(), LinkError> {
        Arc::clone(&self.stubborn_link).listen().await
    }
}

fn internal_callback(inbox: Arc<Mutex<HashSet<uuid::Uuid>>>, callback: Callback) -> Callback {
    let wrapped = move |msg: &[u8]| -> Result<(), LinkError> {
        let (id, payload) = take_envelope(msg)?;

        let is_new = { !inbox.lock().expect("Inbox lock poisoned").contains(&id) };

        if is_new {
            callback(payload)?;
            inbox.lock().expect("Inbox lock poisoned").insert(id);
        }

        Ok(())
    };

    Arc::new(Box::new(wrapped))
}

fn take_envelope(msg: &[u8]) -> Result<(uuid::Uuid, &[u8]), LinkError> {
    const UUID_LEN: usize = 16;
    if msg.len() < UUID_LEN {
        return Err(LinkError::MessageError(format!(
            "Message is shorter than {UUID_LEN}"
        )));
    }

    let (id_bytes, payload) = msg.split_at(UUID_LEN);
    let id_bytes: [u8; 16] = id_bytes
        .try_into()
        .map_err(|e| LinkError::MessageError(format!("Could not obtain message id: {:?}", e)))?;

    let id = uuid::Uuid::from_bytes(id_bytes);
    Ok((id, payload))
}

fn add_envelope(msg: &[u8]) -> Vec<u8> {
    let id = uuid::Uuid::new_v4().into_bytes();
    let mut result = Vec::with_capacity(id.len() + msg.len());
    result.extend_from_slice(&id);
    result.extend_from_slice(msg);
    result
}
