//! The full decision-model cycle: read exactly what the decision depends on, fold, decide,
//! append guarded by the same query, and retry on an advisory same-batch conflict.

use std::error::Error;

use tephra_client::{
    AppendCondition, Client, ClientError, Event, Position, Query, QueryItem, Tag, Tags,
};
use tephra_site_examples::TestServer;

// ANCHOR: cycle
/// The outcome of trying to enrol a student in a course.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Enrolled,
    CourseFull,
    AlreadyEnrolled,
}

/// Enrol a student in a course, guarded so the seat limit holds even against a concurrent writer.
///
/// The cycle reads exactly the events the decision depends on, folds them into the counts it
/// needs, decides, and appends guarded by the same query from the position it read up to. A
/// same-batch conflict is advisory, so the loop rebuilds the model and retries; a durable
/// conflict is terminal and returns to the caller.
fn enrol(
    client: &mut Client,
    course: &str,
    student: &str,
    seat_limit: usize,
) -> Result<Outcome, Box<dyn Error>> {
    let course_tag = format!("course:{course}");
    let student_tag = format!("student:{student}");

    loop {
        // The boundary: everything tagged with this course, OR everything tagged with this student.
        let query = Query::items([
            QueryItem::with_tags(Tags::new([Tag::new(&course_tag)?])?),
            QueryItem::with_tags(Tags::new([Tag::new(&student_tag)?])?),
        ]);

        // Read once, fold both projections over the same pass.
        let (events, watermark) = client.read_all(query.clone(), Position::ZERO)?;
        let mut seats_used = 0usize;
        let mut already_enrolled = false;
        for seq in &events {
            if seq.event().event_type() != "StudentEnrolled" {
                continue;
            }
            let tags: Vec<&str> = seq.event().tags().collect();
            if tags.contains(&course_tag.as_str()) {
                seats_used += 1;
                if tags.contains(&student_tag.as_str()) {
                    already_enrolled = true;
                }
            }
        }

        if already_enrolled {
            return Ok(Outcome::AlreadyEnrolled);
        }
        if seats_used >= seat_limit {
            return Ok(Outcome::CourseFull);
        }

        // Append guarded by the same query, from the position the decision was made against.
        let event = Event::new(
            "StudentEnrolled",
            &[course_tag.as_str(), student_tag.as_str()],
            format!(r#"{{"course":"{course}","student":"{student}"}}"#).into_bytes(),
        )?;
        let guard = AppendCondition::new(query).after(watermark);

        match client.append([event], Some(guard)) {
            Ok(_) => return Ok(Outcome::Enrolled),
            // Same-batch conflict: advisory and retryable, so rebuild the model and try again.
            Err(ClientError::Server { retryable: true, .. }) => continue,
            // Durable conflict, or any other server error: terminal for this attempt.
            Err(err) => return Err(Box::new(err)),
        }
    }
}
// ANCHOR_END: cycle

#[test]
fn enrolment_respects_the_seat_limit() {
    let server = TestServer::start();
    let mut client = Client::connect(server.addr()).expect("connect");

    assert_eq!(enrol(&mut client, "c1", "s1", 2).unwrap(), Outcome::Enrolled);
    assert_eq!(enrol(&mut client, "c1", "s2", 2).unwrap(), Outcome::Enrolled);
    // A third enrolment exceeds the two-seat limit.
    assert_eq!(enrol(&mut client, "c1", "s3", 2).unwrap(), Outcome::CourseFull);
    // Re-enrolling an existing student is idempotent, not a new seat.
    assert_eq!(
        enrol(&mut client, "c1", "s1", 2).unwrap(),
        Outcome::AlreadyEnrolled
    );
}
