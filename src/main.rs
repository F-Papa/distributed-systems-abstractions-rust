use ed25519_dalek::{SigningKey, VerifyingKey};
use log::{debug, error, info, warn};
use std::{
    collections::{HashMap, HashSet},
    io::{Write, stdin, stdout},
    sync::Arc,
    thread,
};
use tokio::{fs, select, sync::mpsc};

use crate::{
    failure_detector::perfect_failure_detector::PerfectFailureDetector,
    links::{
        LinkError, LocalhostResolver, NodeId, Resolver,
        auth_perfect_link::{AplDeliver, AuthPerfectLink},
        signals,
    },
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
        PerfectFailureDetector::new(local_rank, Box::new(LocalhostResolver { base_port }), peers)
            .await
            .expect("detector"),
    );

    let detector_cpy = Arc::clone(&detector);
    let ind_handle = tokio::spawn(async move {
        let mut indications = detector_cpy.indications.lock().await;
        loop {
            match indications.recv().await {
                None => break,
                Some(indication) => {
                    println!("Process {} crashed!", indication.node_id)
                }
            };
        }
    });

    detector.start().await;
    ind_handle.await;
}

async fn handle_indications(indications: &mut mpsc::Receiver<AplDeliver>) {
    loop {
        select! {
            indications = indications.recv() => {
                match indications {
                    None => break,
                    Some(sbl_deliver) => {
                        println!(
                            "\n\t\tMessage from {}: {}",
                            sbl_deliver.sender,
                            str::from_utf8(&sbl_deliver.message).unwrap_or("<Invalid Message>")
                        );
                    }
                }
            }
        }
    }
}

fn get_user_input(tx: mpsc::Sender<(NodeId, String)>) {
    loop {
        print!("Enter recipient's rank: ");
        stdout().flush().unwrap();

        let mut input = String::new();
        if let Err(e) = stdin().read_line(&mut input) {
            error!("Error reading user input: {:?}", e)
        }

        let recipient = match input.trim().parse::<u16>() {
            Err(e) => {
                error!("Invalid recipient: {input}.{:?}", e);
                continue;
            }
            Ok(n) => NodeId::new(n),
        };

        input.clear();
        print!("Enter message for {recipient}: ");
        stdout().flush().unwrap();
        if let Err(e) = stdin().read_line(&mut input) {
            error!("Error reading user input: {:?}", e)
        }

        if let Err(e) = tx.blocking_send((recipient, input)) {
            error!("Channel closed. Shutting down: {:?}", e);
            return;
        }
    }
}

async fn handle_signals(link: &AuthPerfectLink) {
    let (mut sigint, mut sigterm) = signals();
    select! {
        _ = sigterm.recv() => {}
        _ = sigint.recv() => {}
    };
    link.close().await
}

async fn consume_user_input(
    link: &AuthPerfectLink,
    mut rx: mpsc::Receiver<(NodeId, String)>,
) -> Result<(), LinkError> {
    let (mut sigint, mut sigterm) = signals();
    loop {
        select!(
            _ = sigterm.recv() => { break }
            _ = sigint.recv() => { break }
            received = rx.recv() => {
                match received {
                    None => return Ok(()),
                    Some((recipient, msg)) => {
                        link.send(msg.trim().as_bytes(), recipient).await?;
                    }
                }
            }
        )
    }
    Ok(())
}
