//! Minimal end-to-end client example: connect, append two events (one guarded), then read
//! everything back.
//!
//! Run against a `dcbdb-server`:
//!
//! ```text
//! cargo run -p dcbdb-server -- 127.0.0.1:9000 /tmp/dcbdb-net &
//! cargo run -p dcbdb-client --example roundtrip -- 127.0.0.1:9000
//! ```

use std::env;
use std::error::Error;

use dcbdb_client::{Client, condition, event, query_all, query_item, query_items};

fn main() -> Result<(), Box<dyn Error>> {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9000".to_string());
    let mut client = Client::connect(&addr)?;
    println!("connected to {addr}");

    let range = client.append(
        vec![event("Enrolled", &["course:c1", "student:s1"], b"{}")],
        None,
    )?;
    println!("appended Enrolled at position {}", range.first());

    // A uniqueness guard: reserve a username, failing if one already exists.
    let guard = condition(query_items(vec![query_item(&[], &["username:alice"])]), 0);
    let range = client.append(
        vec![event("UsernameReserved", &["username:alice"], b"{}")],
        Some(guard),
    )?;
    println!("reserved username:alice at position {}", range.first());

    // The same guard now conflicts with the event just written.
    let guard = condition(query_items(vec![query_item(&[], &["username:alice"])]), 0);
    match client.append(
        vec![event("UsernameReserved", &["username:alice"], b"{}")],
        Some(guard),
    ) {
        Ok(_) => println!("second reservation unexpectedly succeeded"),
        Err(err) => println!("second reservation rejected: {err}"),
    }

    let (events, watermark) = client.read_all(query_all(), 0)?;
    println!("log holds {} events (watermark {watermark}):", events.len());
    for sequenced in &events {
        let ev = sequenced.event();
        let tags: Vec<&str> = ev.tags().iter().map(|t| t.to_str().unwrap()).collect();
        println!(
            "  {} {} {tags:?}",
            sequenced.position(),
            ev.r#type().to_str().unwrap()
        );
    }
    Ok(())
}
