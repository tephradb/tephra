//! Handling append conflicts: a same-batch conflict is advisory and retryable; a durable
//! conflict is terminal until the decision model is rebuilt against the new tail.

use std::error::Error;

use tephra_client::{
    AppendCondition, Client, ClientError, Event, Position, Query, QueryItem, Tag, Tags,
};
use tephra_site_examples::TestServer;

const SEAT_LIMIT: usize = 2;
const MAX_ATTEMPTS: usize = 8;

// The variants carry the position a caller would act on (where the seat landed, or the durable
// conflict's position). The test below asserts on the shapes rather than each field.
#[derive(Debug)]
#[allow(dead_code)]
enum Reserve {
    Ok(Position),
    Full,
    Conflicted(Option<Position>),
}

fn reserve_seat(
    client: &mut Client,
    course: &str,
    student: &str,
) -> Result<Reserve, Box<dyn Error>> {
    let course_tag = format!("course:{course}");
    let student_tag = format!("student:{student}");

    // ANCHOR: retry
    for _attempt in 0..MAX_ATTEMPTS {
        // Rebuild the decision model from the current tail on each attempt.
        let query = Query::items([
            QueryItem::with_tags(Tags::new([Tag::new(course_tag.clone())?])?),
            QueryItem::with_tags(Tags::new([Tag::new(student_tag.clone())?])?),
        ]);
        let (events, watermark) = client.read_all(query.clone(), Position::ZERO, None)?;

        let seats_used = events
            .iter()
            .filter(|seq| {
                seq.event().event_type() == "StudentEnrolled"
                    && seq.event().tags().any(|t| t == course_tag.as_str())
            })
            .count();
        if seats_used >= SEAT_LIMIT {
            return Ok(Reserve::Full);
        }

        let event = Event::new(
            "StudentEnrolled",
            [course_tag.as_str(), student_tag.as_str()],
            b"{}".to_vec(),
        )?;
        let guard = AppendCondition::new(query).after(watermark);

        match client.append([event], Some(guard)) {
            Ok(result) => return Ok(Reserve::Ok(result.first)),
            // Same-batch: advisory. The tag-only staged check cannot see event type, so this may
            // be a false alarm. Retry immediately with a fresh read.
            Err(ClientError::Server {
                retryable: true, ..
            }) => continue,
            // Durable: a real conflicting event landed since we read. Terminal for this attempt;
            // the caller (or the next loop turn) must decide again against the changed tail.
            Err(ClientError::Server {
                retryable: false,
                conflict_position,
                ..
            }) => return Ok(Reserve::Conflicted(conflict_position)),
            Err(err) => return Err(Box::new(err)),
        }
    }
    Ok(Reserve::Conflicted(None))
    // ANCHOR_END: retry
}

#[test]
fn seat_reservation_holds_the_limit() {
    let server = TestServer::start();
    let mut client = Client::connect(server.addr()).expect("connect");
    assert!(matches!(
        reserve_seat(&mut client, "c1", "s1").expect("s1"),
        Reserve::Ok(_)
    ));
    assert!(matches!(
        reserve_seat(&mut client, "c1", "s2").expect("s2"),
        Reserve::Ok(_)
    ));
    assert!(matches!(
        reserve_seat(&mut client, "c1", "s3").expect("s3"),
        Reserve::Full
    ));
}
