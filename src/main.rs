use std::{sync::Arc, time::Duration};

use log::error;

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
pub mod registers;

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

    let fll1 = FairLossLink::new(local_id, Box::from(LocalhostResolver { base_port }))
        .await
        .unwrap();

    let fll2 = FairLossLink::new(
        local_id,
        Box::from(LocalhostResolver {
            base_port: base_port + 1000,
        }),
    )
    .await
    .unwrap();

    let register = factory::one_one_atomic_register::<usize>(fll1, fll2, &cluster_config);

    let mut i = register.take_indications().await.unwrap();
    let print_returns = async move {
        while let Some(i) = i.recv().await {
            println!("{:?}", i);
        }
        Ok::<(), LinkError>(())
    };

    let register_cpy = register.clone();
    let write_input = async |input: String| {
        if local_id.get() == 1 {
            register_cpy
                .write(input.strip_suffix("\n").unwrap().parse::<usize>().unwrap())
                .await?;
        } else {
            error!("Only node 1 can write");
        }
        Ok::<(), LinkError>(())
    };

    let register_cpy2 = register_cpy.clone();
    let read_value = async move {
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            register_cpy2.read().await;
        }
        Ok::<(), LinkError>(())
    };

    let input_handler = Arc::from(InputHandler::new());
    input_handler.clone().start();

    tokio::try_join!(
        register.start(),
        print_returns,
        input_handler.handle_input(write_input),
        read_value
    )
    .unwrap();
}
