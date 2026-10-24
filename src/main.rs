use std::{collections::HashSet, sync::Arc};

use crate::{
    common::NodeId,
    failure_detector::perfect_failure_detector::ExcludeOnTimeout,
    leader_election::{LeaderElection, MonarchicalLeaderElection},
    links::{
        LinkError, LocalhostResolver,
        fair_loss_link::FairLossLink,
        perfect_link::AcknowledgeMessages,
    },
};

pub mod common;
pub mod crypto;
pub mod failure_detector;
pub mod leader_election;
pub mod links;

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args: Vec<String> = std::env::args().collect();
    assert!(
        args.len() == 4,
        "Expected args: base_port, local_rank, n_peers"
    );

    let base_port = args[1].parse::<u16>().expect("Invalid base port");
    let local_rank = NodeId::new(args[2].parse::<u16>().expect("Invalid local rank"));
    let n_peers = args[3].parse::<u16>().expect("Invalid number of peers");

    let nodes: HashSet<NodeId> = (1..=n_peers)
        .map(NodeId::from)
        .collect();

    let peers: HashSet<NodeId> = nodes
        .clone()
        .into_iter()
        .filter(|id| *id != local_rank)
        .collect();

    let fll = FairLossLink::new(local_rank, Box::from(LocalhostResolver { base_port }))
        .await
        .expect("fair_loss_link");

    let pl = Arc::from(AcknowledgeMessages::new(fll, None));
    let pfd = Arc::from(ExcludeOnTimeout::new(peers, pl));
    let le = Arc::from(MonarchicalLeaderElection::new(nodes, pfd));

    let mut i = le.take_indications().await.unwrap();
    let h = async move {
        while let Some(i) = i.recv().await {
            println!("New leader: {}", i.node_id);
        }
        let res: Result<(), LinkError> = Ok(());
        res
    };

    tokio::try_join!(le.start(), h).unwrap();
}
