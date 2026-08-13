//! Query semantics: OR across items, AND within an item's tags, type filters, and Query::all.

use std::error::Error;

use tephra_client::{Client, Event, EventType, Position, Query, QueryItem, Tag, Tags};
use tephra_site_examples::TestServer;

fn count(client: &mut Client, query: Query) -> Result<usize, Box<dyn Error>> {
    let (events, _watermark) = client.read_all(query, Position::ZERO, None)?;
    Ok(events.len())
}

#[test]
fn query_semantics() -> Result<(), Box<dyn Error>> {
    let server = TestServer::start();
    let mut client = Client::connect(server.addr())?;

    // A small history: one course opened, two enrolments across two courses for one student.
    client.append(
        [Event::new("CourseOpened", ["course:c1"], b"{}".to_vec())?],
        None,
    )?;
    client.append(
        [Event::new(
            "StudentEnrolled",
            ["course:c1", "student:s1"],
            b"{}".to_vec(),
        )?],
        None,
    )?;
    client.append(
        [Event::new(
            "StudentEnrolled",
            ["course:c2", "student:s1"],
            b"{}".to_vec(),
        )?],
        None,
    )?;

    // ANCHOR: and
    // AND within an item: an event must carry every tag listed. Only the c1 enrolment matches.
    let and = Query::item(QueryItem::with_tags(Tags::new([
        Tag::new("course:c1")?,
        Tag::new("student:s1")?,
    ])?));
    // ANCHOR_END: and
    assert_eq!(count(&mut client, and)?, 1);

    // ANCHOR: or
    // OR across items: an event matching either item is returned, and an item can itself be an
    // AND. The c2 enrolment matches the first item; the c1 enrolment matches the second.
    let or = Query::items([
        QueryItem::with_tags(Tags::new([Tag::new("course:c2")?])?),
        QueryItem::with_tags(Tags::new([
            Tag::new("course:c1")?,
            Tag::new("student:s1")?,
        ])?),
    ]);
    // ANCHOR_END: or
    assert_eq!(count(&mut client, or)?, 2);

    // ANCHOR: types
    // A type filter with no tags: matches on event type alone (empty type list means any type).
    let by_type = Query::item(QueryItem::of_types(vec![EventType::new(
        "StudentEnrolled",
    )?]));
    // ANCHOR_END: types
    assert_eq!(count(&mut client, by_type)?, 2);

    // ANCHOR: all
    // Query::all matches every event, bypassing the index for a straight log scan.
    let everything = Query::all();
    // ANCHOR_END: all
    assert_eq!(count(&mut client, everything)?, 3);

    Ok(())
}
