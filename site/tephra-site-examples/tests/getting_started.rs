//! Getting started against a running server: connect, append, read, subscribe.
//!
//! The body is a function taking the server address so the shown snippets read naturally
//! (`Client::connect(addr)`); the test drives it against an in-process server.

use std::error::Error;

use tephra_client::{Client, Event, Position, Query, QueryItem, SubEvent, Tag, Tags};
use tephra_site_examples::TestServer;

fn quickstart(addr: &str) -> Result<(), Box<dyn Error>> {
    // ANCHOR: connect
    let mut client = Client::connect(addr)?;
    // ANCHOR_END: connect

    // ANCHOR: append
    let event = Event::new(
        "CourseOpened",
        &["course:c1"],
        br#"{"course":"c1","seats":30}"#.to_vec(),
    )?;
    let result = client.append([event], None)?;
    println!("recorded positions {} to {}", result.first, result.last);
    // ANCHOR_END: append

    // ANCHOR: read
    let query = Query::item(QueryItem::with_tags(Tags::new([Tag::new("course:c1")?])?));
    // `None` reads the whole matching history; pass `Some(n)` to cap the number of events.
    let (events, _watermark) = client.read_all(query, Position::ZERO, None)?;
    for seq in &events {
        println!("{} {}", seq.position(), seq.event().event_type());
    }
    // ANCHOR_END: read
    assert_eq!(events.len(), 1);

    // A couple more events on the same course, so there is something to page through.
    for seat in ["s1", "s2"] {
        let event = Event::new(
            "SeatReserved",
            &["course:c1"],
            format!("{{\"seat\":\"{seat}\"}}"),
        )?;
        client.append([event], None)?;
    }

    // ANCHOR: paginate
    // `limit` caps how many events a read returns. Pair it with `after` (an exclusive lower
    // bound) to page through a large result: each page resumes at the last position seen, with
    // no gap and no duplicate at the seam. A page shorter than the limit is the last one.
    let query = Query::item(QueryItem::with_tags(Tags::new([Tag::new("course:c1")?])?));
    let page_size = 2;
    let mut after = Position::ZERO;
    let mut seen = 0usize;
    loop {
        let (page, _watermark) = client.read_all(query.clone(), after, Some(page_size))?;
        let short = (page.len() as u64) < page_size;
        if let Some(last) = page.last() {
            after = last.position();
        }
        seen += page.len();
        if short {
            break;
        }
    }
    // ANCHOR_END: paginate
    assert_eq!(seen, 3);

    // ANCHOR: subscribe
    // Subscribe from the start: catch up on history, then a CaughtUp marker at the live edge.
    let query = Query::item(QueryItem::with_tags(Tags::new([Tag::new("course:c1")?])?));
    let (mut stream, cancel) = client.subscribe(query, Position::ZERO)?;
    for item in &mut stream {
        match item? {
            SubEvent::Event(seq) => {
                println!("live {} {}", seq.position(), seq.event().event_type());
            }
            // Reached the live edge. A long-running consumer keeps going; this example stops.
            SubEvent::CaughtUp(_) => break,
        }
    }
    cancel.cancel();
    // ANCHOR_END: subscribe

    Ok(())
}

#[test]
fn connect_append_read_subscribe() {
    let server = TestServer::start();
    quickstart(&server.addr()).expect("quickstart");
}
