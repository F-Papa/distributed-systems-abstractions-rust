use std::sync::Arc;

use crate::links::{
    LocalhostResolver, NodeId,
    fair_loss_link::FairLossLink,
    perfect_link::{AcknowledgeMessages, PerfectLink},
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
    // let n_peers = args[3].parse::<u16>().expect("Invalid number of peers");

    // let peers: HashSet<NodeId> = (1..=n_peers)
    //     .map(NodeId::from)
    //     .filter(|id| *id != local_rank)
    //     .collect();

    let fll = FairLossLink::new(local_rank, Box::from(LocalhostResolver { base_port }))
        .await
        .expect("fair_loss_link");

    let pl = Arc::from(AcknowledgeMessages::new(fll, None));

    let pl_copy = Arc::clone(&pl);
    let handle_indications = async move {
        let indications = pl_copy.get_indications();
        let mut indications = indications.lock().await;
        loop {
            match indications.recv().await {
                None => break,
                Some(indication) => {
                    println!(
                        "Message from {}: {}",
                        indication.sender,
                        indication.message.to_string()
                    )
                }
            };
        }
    };

    let pl_copy = Arc::clone(&pl);
    tokio::spawn(async move { pl_copy.listen().await });

    let msg = serde_json::to_value("Hello gege").unwrap();

    let pl_copy = Arc::clone(&pl);
    pl_copy
        .send(msg, NodeId::new(local_rank.get() + 1))
        .await
        .unwrap();

    tokio::spawn(handle_indications).await.unwrap();
}
