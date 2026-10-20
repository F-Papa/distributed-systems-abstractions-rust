use std::{collections::HashSet, sync::Arc};
use tokio::try_join;

use crate::{
    failure_detector::perfect_failure_detector::{ExcludeOnTimeout, PerfectFailureDetector},
    links::{
        LocalhostResolver, NodeId, fair_loss_link::FairLossLink, perfect_link::EliminateDuplicates,
        stubborn_link::RetransmitForever,
    },
};

pub mod common;
pub mod crypto;
pub mod failure_detector;
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

    let peers: HashSet<NodeId> = (1..=n_peers)
        .map(NodeId::from)
        .filter(|id| *id != local_rank)
        .collect();

    let fll = FairLossLink::new(local_rank, Box::from(LocalhostResolver { base_port }))
        .await
        .expect("fair_loss_link");

    let sbl = RetransmitForever::new(None, fll);
    let pl = EliminateDuplicates::new(Arc::from(sbl));
    let detector = Arc::from(ExcludeOnTimeout::new(peers, Arc::from(pl)));

    let detector_cpy = Arc::clone(&detector);
    let handle_indications = async move {
        let indications = detector_cpy.get_indications();
        let mut indications = indications.lock().await;
        loop {
            match indications.recv().await {
                None => break,
                Some(indication) => {
                    println!("Process {} crashed!", indication.node_id)
                }
            };
        }

        Ok(())
    };

    try_join!(detector.start(), handle_indications).unwrap();
}
