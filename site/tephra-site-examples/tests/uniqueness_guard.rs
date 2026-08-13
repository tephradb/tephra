//! The uniqueness guard: an append condition with `after` omitted means "fail if any matching
//! event exists at all", which enforces "this may happen at most once".

use std::error::Error;

use tephra_client::{
    AppendCondition, Client, ClientError, ErrorCode, Event, EventType, Query, QueryItem, Tag, Tags,
};
use tephra_site_examples::TestServer;

/// Opens a course. Returns `true` if this call opened it, `false` if it was already open.
fn open_course(client: &mut Client, course: &str) -> Result<bool, Box<dyn Error>> {
    // ANCHOR: guard
    let course_tag = format!("course:{course}");
    let event = Event::new("CourseOpened", [course_tag.as_str()], b"{}".to_vec())?;

    // No `after`: the guard means "fail if a CourseOpened for this course already exists".
    let guard = AppendCondition::new(Query::item(QueryItem::new(
        vec![EventType::new("CourseOpened")?],
        Tags::new([Tag::new(course_tag)?])?,
    )));

    match client.append([event], Some(guard)) {
        Ok(_) => Ok(true),
        // A conflict means the course already exists (or is being opened concurrently).
        Err(ClientError::Server {
            code: ErrorCode::Conflict,
            ..
        }) => Ok(false),
        Err(err) => Err(Box::new(err)),
    }
    // ANCHOR_END: guard
}

#[test]
fn a_course_opens_at_most_once() {
    let server = TestServer::start();
    let mut client = Client::connect(server.addr()).expect("connect");
    assert!(open_course(&mut client, "c1").expect("first open"));
    assert!(!open_course(&mut client, "c1").expect("second open"));
}
