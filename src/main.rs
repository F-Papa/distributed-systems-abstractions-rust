use std::sync::Arc;

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

    let ub = factory::uniform_broadcast(fll, &cluster_config);
    let ub_cpy = ub.clone();

    let mut i = ub.take_indications().await.unwrap();
    let print_messages = async move {
        while let Some(i) = i.recv().await {
            println!("[{}]: {:?}", i.sender, i.message);
        }
        let res: Result<(), LinkError> = Ok(());
        res
    };

    let broadcast_input = async |input: String| {
        ub_cpy
            .broadcast(&serde_json::to_value(input).unwrap())
            .await?;
        Ok::<(), LinkError>(())
    };

    let input_handler = Arc::from(InputHandler::new());
    input_handler.clone().start();

    tokio::try_join!(
        ub.listen(),
        print_messages,
        input_handler.handle_input(broadcast_input)
    )
    .unwrap();
}
