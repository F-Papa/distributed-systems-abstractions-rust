use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use ed25519_dalek::{SigningKey, VerifyingKey};
use serde_json::Value;

use crate::{
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
        fair_loss_link::FairLossLink,
        perfect_link::{AcknowledgeMessages, PerfectLink},
        stubborn_link::{RetransmitForever, StubbornLink},
    },
};

// Args

pub struct ClusterConfig {
    pub local_id: NodeId,
    pub number_of_nodes: u16,
    pub base_port: u16,
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
) -> Arc<dyn LeaderElection<Value>> {
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
