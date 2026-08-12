//! The decision-model shape shown on the Introduction page: read the events the decision
//! depends on, fold two projections over one pass, decide, and append guarded by the same query.

use std::error::Error;

use tephra_client::{AppendCondition, Client, Event, Position, Query, QueryItem, Tag, Tags};
use tephra_site_examples::TestServer;

fn decide(addr: &str) -> Result<(), Box<dyn Error>> {
    const SEAT_LIMIT: usize = 30;
    const ENROLMENT_LIMIT: usize = 6;

    let mut client = Client::connect(addr)?;

    // ANCHOR: decide
    // One query, OR across two items: everything tagged course:c1, and everything tagged student:s1.
    let course = QueryItem::with_tags(Tags::new([Tag::new("course:c1")?])?);
    let student = QueryItem::with_tags(Tags::new([Tag::new("student:s1")?])?);
    let query = Query::items([course, student]);

    // Read once, fold both projections over the same pass.
    let (events, watermark) = client.read_all(query.clone(), Position::ZERO, None)?;
    let mut seats_used = 0usize;
    let mut student_enrolments = 0usize;
    for seq in &events {
        if seq.event().event_type() == "StudentEnrolled" {
            let tags: Vec<&str> = seq.event().tags().collect();
            if tags.contains(&"course:c1") {
                seats_used += 1;
            }
            if tags.contains(&"student:s1") {
                student_enrolments += 1;
            }
        }
    }

    if seats_used >= SEAT_LIMIT || student_enrolments >= ENROLMENT_LIMIT {
        // The decision fails on the model we just built; no write.
        return Ok(());
    }

    // Append guarded by the same query, from the position we read up to.
    let event = Event::new(
        "StudentEnrolled",
        &["course:c1", "student:s1"],
        br#"{"course":"c1","student":"s1"}"#.to_vec(),
    )?;
    let guard = AppendCondition::new(query).after(watermark);
    client.append([event], Some(guard))?;
    // ANCHOR_END: decide

    Ok(())
}

#[test]
fn decision_model_shape_runs() {
    let server = TestServer::start();
    decide(&server.addr()).expect("decide");
}
