use std::time::Duration;

use crate::{
    common::NodeId,
    factory::ClusterConfig,
    links::{LinkError, LocalhostResolver, fair_loss_link::FairLossLink},
};

pub mod common;
pub mod crypto;
mod factory;
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
    let local_id = NodeId::new(args[2].parse::<u16>().expect("Invalid local rank"));
    let number_of_nodes = args[3].parse::<u16>().expect("Invalid number of peers");
    let cluster_config = ClusterConfig {
        number_of_nodes,
        local_id,
    };

    let fll = FairLossLink::new(local_id, Box::from(LocalhostResolver { base_port }))
        .await
        .unwrap();

    let ble = factory::byzantine_leader_elector(fll, &cluster_config);
    let mut i = ble.take_indications().await.unwrap();

    let h = async move {
        while let Some(i) = i.recv().await {
            println!("Leader: {:?}", i.node_id);
        }
        let res: Result<(), LinkError> = Ok(());
        res
    };

    let ble_cpy = ble.clone();
    let c = async move {
        loop {
            tokio::time::sleep(Duration::from_secs(rand::random_range(1..=10))).await;
            println!("Complaining");
            ble_cpy.complain().await?;
        }

        let res: Result<(), LinkError> = Ok(());
        res
    };

    tokio::try_join!(ble.start(), c, h).unwrap();
}
