use serde::{Deserialize, Serialize};

use crate::links::NodeId;

#[derive(Serialize, Deserialize, Debug)]
pub enum EpfdIndication {
    Suspect(NodeId),
    Restore(NodeId),
}

pub struct EventuallyPerfectFailureDetector {}

impl EventuallyPerfectFailureDetector {
    pub async fn new() -> Self {
        Self {}
    }
}
