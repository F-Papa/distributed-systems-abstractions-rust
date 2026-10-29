use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use ed25519_dalek::{SigningKey, VerifyingKey};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{
    broadcast::{
        best_effort::{BasicBroadcast, BestEffortBroadcast},
        reliable::{EagerReliableBroadcast, ReliableBroadcast},
        stubborn::{BasicStubbonBroadcast, StubbornBroadcast},
        uniform::{MajorityAckBroadcast, UniformBroadcast},
    },
    common::NodeId,
    crypto, factory,
    failure_detector::{
        eventually_perfect_failure_detector::{
            EventuallyPerfectFailureDetector, IncreasingTimeout,
        },
        perfect_failure_detector::{ExcludeOnTimeout, PerfectFailureDetector},
    },
    leader_election::{
        LeaderElection, MonarchicalLeaderElection,
        byzantine_leader_election::{ByzantineLeaderElection, RotatingElection},
        eventual_leader_elector::{EventualLeaderElection, MonarchicalEventualLeaderElector},
    },
    links::{
        auth_perfect_link::{AuthenticatedPerfectLink, SignAndAcknowledge},
        fair_loss_link::{self, FairLossLink},
        perfect_link::{AcknowledgeMessages, PerfectLink},
        stubborn_link::{RetransmitForever, StubbornLink},
    },
    registers::{
        atomic::write_one_read_many::{OneManyAtomicRegister, WriteOneReadManyAtomic},
        regular::{OneManyRegularRegister, WriteOneReadAll},
    },
};

// Args

#[derive(Clone)]
pub struct ClusterConfig {
    pub local_id: NodeId,
    pub number_of_nodes: u16,
}

impl ClusterConfig {
    pub(crate) fn nodes(&self) -> HashSet<NodeId> {
        (1..=self.number_of_nodes).map(NodeId::new).collect()
    }

    pub(crate) fn nodes_vec(&self) -> Vec<NodeId> {
        (1..=self.number_of_nodes).map(NodeId::new).collect()
    }

    pub(crate) fn peers(&self) -> HashSet<NodeId> {
        self.nodes()
            .into_iter()
            .filter(|id| *id != self.local_id)
            .collect()
    }

    pub(crate) fn signing_key(&self) -> SigningKey {
        crypto::get_signing_key(self.local_id).unwrap()
    }

    pub(crate) fn verifiers(&self) -> HashMap<NodeId, Arc<VerifyingKey>> {
        HashMap::from_iter(
            self.nodes()
                .into_iter()
                .map(|id| (id, Arc::from(crypto::get_verifier(id).unwrap()))),
        )
    }
}

// Links

pub fn stubborn_link(
    fair_loss_link: FairLossLink,
    _config: &ClusterConfig,
) -> Arc<dyn StubbornLink<Value>> {
    Arc::from(RetransmitForever::new(None, fair_loss_link))
}

pub fn perfect_link(
    fair_loss_link: FairLossLink,
    _config: &ClusterConfig,
) -> Arc<dyn PerfectLink<Value>> {
    Arc::from(AcknowledgeMessages::new(fair_loss_link, None))
}

pub fn auth_perfect_link(
    fair_loss_link: FairLossLink,
    config: &ClusterConfig,
) -> Arc<dyn AuthenticatedPerfectLink<Value>> {
    Arc::from(SignAndAcknowledge::new(
        config.local_id,
        config.signing_key(),
        config.verifiers(),
        fair_loss_link,
        None,
    ))
}

// Failure Detectors

pub fn perfect_failure_detector(
    fair_loss_link: FairLossLink,
    config: &ClusterConfig,
) -> Arc<dyn PerfectFailureDetector> {
    Arc::from(ExcludeOnTimeout::new(
        config.peers(),
        perfect_link(fair_loss_link, config),
    ))
}

pub fn eventually_perfect_failure_detector(
    fair_loss_link: FairLossLink,
    config: &ClusterConfig,
) -> Arc<dyn EventuallyPerfectFailureDetector> {
    Arc::from(IncreasingTimeout::new(
        config.peers(),
        factory::perfect_link(fair_loss_link, config),
    ))
}

// Leader Electors

pub fn leader_elector(
    fair_loss_link: FairLossLink,
    config: &ClusterConfig,
) -> Arc<dyn LeaderElection> {
    Arc::from(MonarchicalLeaderElection::new(
        config.nodes(),
        perfect_failure_detector(fair_loss_link, config),
    ))
}

pub fn eventual_leader_elector(
    fair_loss_link: FairLossLink,
    config: &ClusterConfig,
) -> Arc<dyn EventualLeaderElection> {
    Arc::from(MonarchicalEventualLeaderElector::new(
        config.nodes(),
        eventually_perfect_failure_detector(fair_loss_link, config),
    ))
}

pub fn byzantine_leader_elector(
    fair_loss_link: FairLossLink,
    config: &ClusterConfig,
) -> Arc<dyn ByzantineLeaderElection> {
    Arc::from(RotatingElection::new(
        config.peers(),
        config.nodes_vec(),
        auth_perfect_link(fair_loss_link, config),
        (config.number_of_nodes.saturating_sub(1) / 3) as usize,
    ))
}

// Broadcast

pub fn best_effort_broadcast(
    fair_loss_link: FairLossLink,
    config: &ClusterConfig,
) -> Arc<dyn BestEffortBroadcast<Value>> {
    Arc::from(BasicBroadcast::new(
        perfect_link(fair_loss_link, config),
        config.nodes(),
    ))
}

pub fn reliable_broadcast(
    fair_loss_link: FairLossLink,
    config: &ClusterConfig,
) -> Arc<dyn ReliableBroadcast<Value>> {
    Arc::from(EagerReliableBroadcast::new(
        config.local_id,
        best_effort_broadcast(fair_loss_link, config),
    ))
}

pub fn uniform_broadcast(
    fair_loss_link: FairLossLink,
    config: &ClusterConfig,
) -> Arc<dyn UniformBroadcast<Value>> {
    Arc::from(MajorityAckBroadcast::new(
        config.local_id,
        config.number_of_nodes,
        best_effort_broadcast(fair_loss_link, config),
    ))
}

pub fn stubborn_broadcast(
    fair_loss_link: FairLossLink,
    config: &ClusterConfig,
) -> Arc<dyn StubbornBroadcast<Value>> {
    Arc::from(BasicStubbonBroadcast::new(
        config.local_id,
        config.peers(),
        stubborn_link(fair_loss_link, config),
    ))
}

// Registers

pub fn one_many_regular_register<T>(
    fair_loss_link_1: FairLossLink,
    fair_loss_link_2: FairLossLink,
    config: &ClusterConfig,
) -> Arc<dyn OneManyRegularRegister<T>>
where
    T: Send + Sync + Serialize + DeserializeOwned + Clone + 'static,
{
    Arc::from(WriteOneReadAll::new(
        config.number_of_nodes,
        perfect_link(fair_loss_link_1, config),
        best_effort_broadcast(fair_loss_link_2, config),
    ))
}

pub fn one_many_atomic_register<T>(
    fair_loss_link_1: FairLossLink,
    fair_loss_link_2: FairLossLink,
    config: &ClusterConfig,
) -> Arc<dyn OneManyAtomicRegister<T>>
where
    T: Send + Sync + Serialize + DeserializeOwned + Clone + 'static,
{
    Arc::from(WriteOneReadManyAtomic::new(
        config.number_of_nodes,
        best_effort_broadcast(fair_loss_link_1, config),
        perfect_link(fair_loss_link_2, config),
    ))
}
