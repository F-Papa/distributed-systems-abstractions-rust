use std::time::Duration;

use crate::{
    common::NodeId,
    factory::ClusterConfig,
    links::{LinkError, LocalhostResolver, fair_loss_link::FairLossLink},
};

pub mod common;
pub mod crypto;
pub mod factory;
pub mod failure_detector;
pub mod leader_election;
pub mod links;
mod broadcast;

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args: Vec<String> = std::env::args().collect();
    assert!(
        args.len() == 4,
        "Expected args: base_port, local_rank, n_peers"
    );

    let base_port = args[1].parse::<u16>().expect("Invalid base port");
    let local_id = NodeId::new(args[2].parse::<u16>().expect("Invalid local rank"));
    let number_of_nodes = args[3].parse::<u16>().expect("Invalid number of peers");
    let cluster_config = ClusterConfig {
        number_of_nodes,
        local_id,
    };

    let fll = FairLossLink::new(local_id, Box::from(LocalhostResolver { base_port }))
        .await
        .unwrap();

    let beb = factory::best_effort_broadcast(fll, &cluster_config);
    let mut i = beb.take_indications().await.unwrap();

    let h = async move {
        while let Some(i) = i.recv().await {
            println!("[{}]: {:?}", i.sender, i.message);
        }
        let res: Result<(), LinkError> = Ok(());
        res
    };

    let beb_cpy = beb.clone();
    let c = async move {
        let mut counter = 1;
        loop {
            tokio::time::sleep(Duration::from_secs(rand::random_range(1..=10))).await;
            let msg = serde_json::to_value(format!("Message {}", counter)).unwrap();
            beb_cpy.broadcast(&msg).await?;
            counter +=1;
        }

        let res: Result<(), LinkError> = Ok(());
        res
    };

    tokio::try_join!(beb.listen(), c, h).unwrap();
}
