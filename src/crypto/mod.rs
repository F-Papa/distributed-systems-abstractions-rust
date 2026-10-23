use std::fs;

use ed25519_dalek::{SigningKey, VerifyingKey};

use crate::common::NodeId;

pub fn get_signing_key(id: NodeId) -> Result<SigningKey, String> {
    let path = format!("keys/node{}", id.get());
    let file_bytes = fs::read(path).map_err(|e| e.to_string())?;
    let signing_key_bytes =
        TryInto::<[u8; 32]>::try_into(file_bytes).map_err(|_| "Could not turn into")?;
    let signing_key = SigningKey::from_bytes(&signing_key_bytes);
    Ok(signing_key)
}

pub fn get_verifier(id: NodeId) -> Result<VerifyingKey, String> {
    let path = format!("keys/node{}.pub", id.get());
    let file_bytes = fs::read(path).map_err(|e| e.to_string())?;
    let verifier_bytes =
        TryInto::<[u8; 32]>::try_into(file_bytes).map_err(|_| "Could not turn into")?;

    let verifier_key = VerifyingKey::from_bytes(&verifier_bytes).map_err(|e| e.to_string())?;
    Ok(verifier_key)
}
