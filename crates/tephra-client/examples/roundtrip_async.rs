//! Minimal async client example: connect once, fire several appends concurrently over the one
//! connection (multiplexing), read everything back, then tail a short subscription.
//!
//! Run against a `tephra-server`:
//!
//! ```text
//! cargo run -p tephra-server -- 127.0.0.1:9000 /tmp/tephra-net &
//! cargo run -p tephra-client --example roundtrip_async --features async -- 127.0.0.1:9000
//! ```

use std::env;
use std::error::Error;

use tephra_client::{AsyncClient, Event, Position, Query, SubEvent};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9000".to_string());
    let client = AsyncClient::connect(&addr).await?;
    println!("connected to {addr}");

    // Three appends in flight at once over a single connection (the blocking client would run
    // these one after another). `join!` awaits them concurrently.
    let (a, b, c) = tokio::join!(
        client.append([Event::new("Enrolled", &["course:c1"], b"{}")?], None),
        client.append([Event::new("Enrolled", &["course:c2"], b"{}")?], None),
        client.append([Event::new("Enrolled", &["course:c3"], b"{}")?], None),
    );
    for range in [a?, b?, c?] {
        println!("appended at position {}", range.first);
    }

    let (events, watermark) = client.read_all(Query::all(), Position::ZERO).await?;
    println!("log holds {} events (watermark {watermark}):", events.len());
    for sequenced in &events {
        let ev = sequenced.event();
        let tags: Vec<&str> = ev.tags().collect();
        println!("  {} {} {tags:?}", sequenced.position(), ev.event_type());
    }

    // Tail a subscription from the start until it reaches the live edge, then stop (dropping the
    // stream cancels it server-side without disturbing the connection).
    println!("subscribing until caught up:");
    let mut sub = client.subscribe(Query::all(), Position::ZERO).await;
    while let Some(item) = sub.next().await {
        match item? {
            SubEvent::Event(sequenced) => {
                println!("  live {} {}", sequenced.position(), sequenced.event().event_type());
            }
            SubEvent::CaughtUp(watermark) => {
                println!("  caught up at {watermark}");
                break;
            }
        }
    }
    Ok(())
}
