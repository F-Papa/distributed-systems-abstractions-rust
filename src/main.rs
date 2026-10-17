use std::{
    io::{Write, stdin, stdout},
    sync::Arc,
    thread,
};
use tokio::{select, sync::mpsc, task::JoinHandle};

use crate::links::{LinkError, LocalhostResolver, perfect_link::PerfectLink, signals};

pub mod links;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert!(
        args.len() == 4,
        "Expected args: base_port, local_rank, n_peers"
    );

    let base_port = args[1].parse::<u16>().expect("Invalid base port");
    let local_rank = args[2].parse::<u16>().expect("Invalid local rank");
    // let n_peers = args[3].parse::<u16>().expect("Invalid number of peers");

    let addr = format!("localhost:{}", base_port + local_rank);
    println!("Listening on: {addr}");

    let link = PerfectLink::new(local_rank, Box::new(LocalhostResolver { base_port }), None)
        .await
        .expect("Creating link");

    let link = Arc::new(link);

    let link_cpy = Arc::clone(&link);
    let listen_handle = tokio::spawn(async move { link_cpy.listen().await });

    let (tx, mut rx) = mpsc::channel::<(u16, String)>(32);
    thread::spawn(move || {
        loop {
            print!("Enter recipient's rank: ");
            stdout().flush().unwrap();

            let mut input = String::new();
            if let Err(e) = stdin().read_line(&mut input) {
                println!("Error reading user input: {:?}", e)
            }

            let recipient: u16 = match input.trim().parse::<u16>() {
                Err(e) => {
                    println!("Invalid recipient: {input}.{:?}", e);
                    continue;
                }
                Ok(n) => n,
            };

            input.clear();
            print!("Enter message for {recipient}: ");
            stdout().flush().unwrap();
            if let Err(e) = stdin().read_line(&mut input) {
                println!("Error reading user input: {:?}", e)
            }

            if let Err(e) = tx.blocking_send((recipient, input)) {
                println!("Channel closed. Shutting down: {:?}", e);
                return;
            }
        }
    });

    let link_cpy = Arc::clone(&link);
    let send_handle: JoinHandle<Result<(), LinkError>> = tokio::spawn(async move {
        let (mut sigint, mut sigterm) = signals();

        loop {
            select!(
                received = rx.recv() => {
                    match received {
                        None => return Ok(()),
                        Some((recipient, msg)) => {
                            link_cpy.send(msg.trim().as_bytes(), recipient).await?;
                        }
                    }
                }

                _ = sigterm.recv() => {
                    break
                }

                _ = sigint.recv() => {
                    break
                }
            )
        }

        Ok(())
    });

    let link_cpy = Arc::clone(&link);

    let signal_handle = tokio::spawn(async move {
        let (mut sigint, mut sigterm) = signals();
        select! {
            _ = sigterm.recv() => {}
            _ = sigint.recv() => {}
        };
        link_cpy.close().await
    });

    let mut indications = link.indications.lock().await;
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
    println!("\nindication consumer stopped");

    if let Err(e) = send_handle.await.expect("Await send_handle") {
        println!("{:?}", e);
    }
    println!("send_handle joined");

    if let Err(e) = listen_handle.await.expect("Await listen_handle") {
        println!("{:?}", e);
    }
    println!("listen_handle joined");

    signal_handle.await.expect("Await signal_handle");
    println!("signal_handle joined");
}
