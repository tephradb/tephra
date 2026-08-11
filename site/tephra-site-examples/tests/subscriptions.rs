//! Building a read model with a subscription, and resuming it from a persisted cursor.

use std::error::Error;

use tephra_client::{Client, Event, Position, Query, SequencedEvent, SubEvent};
use tephra_site_examples::TestServer;

/// Drains a subscription from `cursor` up to the live edge, calling `on_event` for each event and
/// returning the new cursor to persist. On a cold start, pass `Position::ZERO`.
///
/// The read model owns its own connection: a subscription takes over the connection it runs on,
/// so it is kept separate from whatever connection issues writes.
fn drain_read_model(
    addr: &str,
    cursor: Position,
    mut on_event: impl FnMut(&SequencedEvent),
) -> Result<Position, Box<dyn Error>> {
    // ANCHOR: resume
    let mut client = Client::connect(addr)?;
    let mut cursor = cursor;

    // Subscribe from the persisted cursor. `after` is exclusive, so already-processed events are
    // not redelivered.
    let (mut stream, cancel) = client.subscribe(Query::all(), cursor)?;
    for item in &mut stream {
        match item? {
            SubEvent::Event(seq) => {
                on_event(&seq);
                // Advance the cursor only after the event is handled, then persist it.
                cursor = seq.position();
            }
            // Reached the live edge: the read model is current. A long-running consumer keeps the
            // stream open; here we stop and return the cursor to store.
            SubEvent::CaughtUp(_) => break,
        }
    }
    cancel.cancel();
    Ok(cursor)
    // ANCHOR_END: resume
}

#[test]
fn a_read_model_resumes_from_its_cursor() {
    let server = TestServer::start();
    let addr = server.addr();
    let mut writer = Client::connect(&addr).expect("connect writer");

    writer
        .append([Event::new("A", &["k:1"], b"{}".to_vec()).unwrap()], None)
        .unwrap();
    writer
        .append([Event::new("B", &["k:1"], b"{}".to_vec()).unwrap()], None)
        .unwrap();

    // Cold start: process everything, keep the cursor.
    let mut seen = 0usize;
    let cursor = drain_read_model(&addr, Position::ZERO, |_| seen += 1).expect("drain");
    assert_eq!(seen, 2);

    // A new event arrives; resuming from the cursor delivers only that one.
    writer
        .append([Event::new("C", &["k:1"], b"{}".to_vec()).unwrap()], None)
        .unwrap();
    let mut seen_after = 0usize;
    let cursor2 = drain_read_model(&addr, cursor, |_| seen_after += 1).expect("resume");
    assert_eq!(seen_after, 1);
    assert!(cursor2 > cursor);
}
