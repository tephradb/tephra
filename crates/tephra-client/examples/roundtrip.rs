//! Minimal end-to-end client example: connect, append two events (one guarded), then read
//! everything back.
//!
//! Run against a `tephra-server`:
//!
//! ```text
//! cargo run -p tephra-server -- 127.0.0.1:9000 /tmp/tephra-net &
//! cargo run -p tephra-client --example roundtrip -- 127.0.0.1:9000
//! ```

use std::env;
use std::error::Error;

use tephra_client::{AppendCondition, Client, Event, Position, Query, QueryItem};

fn main() -> Result<(), Box<dyn Error>> {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9000".to_string());
    let mut client = Client::connect(&addr)?;
    println!("connected to {addr}");

    let range = client.append(
        [Event::new("Enrolled", &["course:c1", "student:s1"], b"{}")?],
        None,
    )?;
    println!("appended Enrolled at position {}", range.first);

    // A uniqueness guard: reserve a username, failing if one already exists.
    let guard = AppendCondition::new(Query::item(QueryItem::with_tags(tags(&["username:alice"])?)));
    let range = client.append(
        [Event::new("UsernameReserved", &["username:alice"], b"{}")?],
        Some(guard),
    )?;
    println!("reserved username:alice at position {}", range.first);

    // The same guard now conflicts with the event just written.
    let guard = AppendCondition::new(Query::item(QueryItem::with_tags(tags(&["username:alice"])?)));
    match client.append(
        [Event::new("UsernameReserved", &["username:alice"], b"{}")?],
        Some(guard),
    ) {
        Ok(_) => println!("second reservation unexpectedly succeeded"),
        Err(err) => println!("second reservation rejected: {err}"),
    }

    let (events, watermark) = client.read_all(Query::all(), Position::ZERO)?;
    println!("log holds {} events (watermark {watermark}):", events.len());
    for sequenced in &events {
        let ev = sequenced.event();
        let tags: Vec<&str> = ev.tags().collect();
        println!("  {} {} {tags:?}", sequenced.position(), ev.event_type());
    }
    Ok(())
}

/// Builds a validated tag set for a query item.
fn tags(items: &[&str]) -> Result<tephra_client::Tags, Box<dyn Error>> {
    let mut out = Vec::with_capacity(items.len());
    for tag in items {
        out.push(tephra_client::Tag::new(*tag)?);
    }
    Ok(tephra_client::Tags::new(out)?)
}
