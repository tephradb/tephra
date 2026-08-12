//! The landing-page hero: append guarded by a condition, then read.

use std::error::Error;

use tephra_client::{AppendCondition, Client, Event, Position, Query, QueryItem, Tag, Tags};
use tephra_site_examples::TestServer;

fn hero(addr: &str) -> Result<(), Box<dyn Error>> {
    // ANCHOR: hero
    // Connect to a running Tephra server.
    let mut client = Client::connect(addr)?;

    // Record an enrolment, but only if this course has no enrolment recorded yet.
    let event = Event::new(
        "StudentEnrolled",
        &["course:c1", "student:s1"],
        br#"{"course":"c1","student":"s1"}"#.to_vec(),
    )?;
    let guard = AppendCondition::new(Query::item(QueryItem::with_tags(Tags::new([Tag::new(
        "course:c1",
    )?])?)));

    match client.append([event], Some(guard)) {
        Ok(result) => println!("recorded at {}", result.first),
        Err(err) => eprintln!("append rejected: {err:?}"),
    }

    // Read every event tagged course:c1, ascending, from the start of the log.
    let query = Query::item(QueryItem::with_tags(Tags::new([Tag::new("course:c1")?])?));
    let (events, _watermark) = client.read_all(query, Position::ZERO, None)?;
    for seq in &events {
        println!("{} {}", seq.position(), seq.event().event_type());
    }
    // ANCHOR_END: hero

    Ok(())
}

#[test]
fn landing_hero_runs() {
    let server = TestServer::start();
    hero(&server.addr()).expect("hero");
}
