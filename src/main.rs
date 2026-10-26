use std::{os::unix::thread, sync::Arc, time::Duration};

use env_logger::fmt::style::AnsiColor::BrightBlue;

use crate::{
    common::{InputHandler, NodeId},
    factory::ClusterConfig,
    links::{LinkError, LocalhostResolver, fair_loss_link::FairLossLink},
};

mod broadcast;
pub mod common;
pub mod crypto;
pub mod factory;
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

    let rb = factory::reliable_broadcast(fll, &cluster_config);
    let rb_cpy = rb.clone();

    let mut i = rb.take_indications().await.unwrap();
    let handler = async move {
        while let Some(i) = i.recv().await {
            println!("[{}]: {:?}", i.sender, i.message);
        }
        let res: Result<(), LinkError> = Ok(());
        res
    };


    let input_handler = Arc::from(InputHandler::new());
    input_handler.clone().start();

    let broadcast = async move {
        let mut rx = input_handler.take_rx().unwrap();
        while let Some(input) = rx.recv().await {
            rb_cpy
                .broadcast(&serde_json::to_value(input).unwrap())
                .await
                .unwrap();
        }
        let res: Result<(), LinkError> = Ok(());
        res
    };

    tokio::try_join!(rb.listen(), handler, broadcast).unwrap();
}
