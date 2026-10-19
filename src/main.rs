use std::{collections::HashSet, sync::Arc};
use tokio::try_join;

use crate::{
    failure_detector::perfect_failure_detector::ExcludeOnTimeout,
    links::{LocalhostResolver, NodeId},
};

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
        .map(|id| NodeId::from(id))
        .filter(|id| *id != local_rank)
        .collect();

    let detector = Arc::new(
        ExcludeOnTimeout::new(local_rank, Box::new(LocalhostResolver { base_port }), peers)
            .await
            .expect("detector"),
    );

    let detector_cpy = Arc::clone(&detector);
    let handle_indications = async move {
        let mut indications = detector_cpy.indications.lock().await;
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
