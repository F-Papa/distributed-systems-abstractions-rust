use std::fs;

use ed25519_dalek::SigningKey;
use getrandom::{SysRng, rand_core::UnwrapErr};

fn generate_keys(num_of_pairs: usize, out_dir: &str) {
    for i in 1..num_of_pairs + 1 {
        let signing_key = SigningKey::generate(&mut UnwrapErr(SysRng));
        let public_key = signing_key.verifying_key();
        let path = format!("{out_dir}/node{i}");
        fs::write(path, signing_key.as_bytes()).expect("Writing signing key");
        let path = format!("./keys/node{i}.pub");
        fs::write(path, public_key.as_bytes()).expect("Writing pub key");
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert!(args.len() == 3, "Expected args: num_of_pairs, output directory.\nExample: cargo run --bin generate_keys 10 ./keys");
    let num_of_pairs = args[1].parse::<usize>().expect("Invalid number");
    generate_keys(num_of_pairs, &args[2]);
}
