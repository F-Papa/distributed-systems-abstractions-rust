use std::{collections::HashSet, sync::Arc};

use crate::{
    common::NodeId,
    failure_detector::eventually_perfect_failure_detector::EventuallyPerfectFailureDetector,
    links::{
        LinkError, LocalhostResolver,
        fair_loss_link::FairLossLink,
        logged_perfect_link::{CommitAndAcknowledge, LoggedPerfectLink},
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

    let nodes: HashSet<NodeId> = (1..=n_peers).map(NodeId::from).collect();

    let _peers: HashSet<NodeId> = nodes
        .clone()
        .into_iter()
        .filter(|id| *id != local_rank)
        .collect();

    let fll = FairLossLink::new(local_rank, Box::from(LocalhostResolver { base_port }))
        .await
        .expect("fair_loss_link");

    let pl = Arc::from(AcknowledgeMessages::new(fll, None));
    let lpl = Arc::from(CommitAndAcknowledge::new(local_rank, pl, None));

    let mut i = lpl.take_indications().await.take().unwrap();
    let h = async move {
        while let Some(i) = i.recv().await {
            println!("Delivered: {:?}", i.delivered);
        }
        let res: Result<(), LinkError> = Ok(());
        res
    };

    let lpl_cpy = Arc::clone(&lpl);
    let send = async move {
        let mut counter = 1;
        while counter < 5 {
            counter +=1 ;
            let msg = format!("Message number {counter}");
            let value = serde_json::to_value(msg).unwrap();
            lpl_cpy.send(value, NodeId::new(local_rank.get()+1)).await.unwrap();
        }
        Ok(()) as Result<(), LinkError>
    };

    tokio::try_join!(lpl.listen(), h, send).unwrap();
}
