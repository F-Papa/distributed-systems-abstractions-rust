use std::sync::Arc;

use crate::links::{LinkError, LocalhostResolver, perfect_link::PerfectLink};

pub mod links;

fn print_msg(raw: &[u8]) -> Result<(), LinkError> {
    let msg = str::from_utf8(raw).map_err(|e| LinkError::MessageError(e.to_string()))?;
    println!("{}", msg);
    Ok(())
    // Err(ListenError::MessageError("Cagamos".to_owned()))
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert!(
        args.len() == 4,
        "Expected args: base_port, local_rank, n_peers"
    );

    let base_port = args[1].parse::<u16>().expect("Invalid base port");
    let local_rank = args[2].parse::<u16>().expect("Invalid local rank");
    let n_peers = args[3].parse::<u16>().expect("Invalid number of peers");

    let addr = format!("localhost:{}", base_port + local_rank);
    println!("Listening on: {addr}");

    let link = PerfectLink::new(
        local_rank,
        Box::new(LocalhostResolver { base_port }),
        Arc::new(print_msg),
    )
    .await
    .unwrap();

    let link = Arc::new(link);
    let link_cpy = Arc::clone(&link);
    let listen_handle = tokio::spawn(async move { link_cpy.listen().await });

    for i in 1..n_peers + 1 {
        if i == local_rank {
            continue;
        }
        _ = link.send("Hello".as_bytes(), i).await;
    }

    for i in 1..n_peers + 1 {
        if i == local_rank {
            continue;
        }
        _ = link.send("Bye".as_bytes(), i).await;
    }

    if let Err(e) = listen_handle.await.expect("Await listen_handle") {
        println!("{:?}", e);
    }
}
