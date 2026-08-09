//! Round-trip and framing tests for the wire protocol.

use std::io::Cursor;

use tephra_proto::tephra::{AppendRequest, Event, Request, Response, request};
use tephra_proto::{DEFAULT_MAX_FRAME_LEN, FrameError, read_frame, write_frame};

fn sample_event(ty: &str, tags: &[&str], payload: &[u8]) -> Event {
    let mut ev = Event::new();
    ev.set_type(ty);
    for tag in tags {
        ev.tags_mut().push(*tag);
    }
    ev.set_payload(payload);
    ev
}

#[test]
fn request_round_trips_through_a_frame() {
    let mut append = AppendRequest::new();
    append.events_mut().push(sample_event(
        "Enrolled",
        &["course:c1", "student:s1"],
        b"{}",
    ));
    let mut req = Request::new();
    req.set_request_id(7);
    req.set_append(append);

    let mut buf = Vec::new();
    write_frame(&mut buf, &req, DEFAULT_MAX_FRAME_LEN).unwrap();

    let mut cursor = Cursor::new(buf);
    let got: Request = read_frame(&mut cursor, DEFAULT_MAX_FRAME_LEN)
        .unwrap()
        .expect("one frame present");

    assert_eq!(got.request_id(), 7);
    match got.kind() {
        request::KindOneof::Append(append) => {
            let events = append.events();
            assert_eq!(events.len(), 1);
            let ev = events.get(0).unwrap();
            assert_eq!(ev.r#type().to_str().unwrap(), "Enrolled");
            let tags: Vec<String> = ev
                .tags()
                .iter()
                .map(|t| t.to_str().unwrap().to_string())
                .collect();
            assert_eq!(tags, vec!["course:c1", "student:s1"]);
            assert_eq!(ev.payload(), b"{}");
        }
        other => panic!("expected append, got {other:?}"),
    }
}

#[test]
fn multiple_frames_read_in_sequence_then_clean_eof() {
    let mut buf = Vec::new();
    for id in 0..3u64 {
        let mut resp = Response::new();
        resp.set_request_id(id);
        write_frame(&mut buf, &resp, DEFAULT_MAX_FRAME_LEN).unwrap();
    }

    let mut cursor = Cursor::new(buf);
    for id in 0..3u64 {
        let resp: Response = read_frame(&mut cursor, DEFAULT_MAX_FRAME_LEN)
            .unwrap()
            .expect("frame present");
        assert_eq!(resp.request_id(), id);
    }
    // A clean boundary at EOF is `Ok(None)`, not an error.
    assert!(
        read_frame::<Response, _>(&mut cursor, DEFAULT_MAX_FRAME_LEN)
            .unwrap()
            .is_none()
    );
}

#[test]
fn empty_reader_is_a_clean_eof() {
    let mut cursor = Cursor::new(Vec::new());
    assert!(
        read_frame::<Request, _>(&mut cursor, DEFAULT_MAX_FRAME_LEN)
            .unwrap()
            .is_none()
    );
}

#[test]
fn a_frame_at_the_limit_round_trips_but_over_it_is_rejected() {
    // A payload sized so the whole encoded Response lands just under a tiny cap.
    let mut resp = Response::new();
    resp.set_request_id(1);
    let mut buf = Vec::new();
    write_frame(&mut buf, &resp, DEFAULT_MAX_FRAME_LEN).unwrap();
    let body_len = (buf.len() - 4) as u32;

    // Exactly at the limit: accepted.
    let mut cursor = Cursor::new(buf.clone());
    assert!(
        read_frame::<Response, _>(&mut cursor, body_len)
            .unwrap()
            .is_some()
    );

    // One below the limit: the length prefix is rejected before allocating the body.
    let mut cursor = Cursor::new(buf);
    match read_frame::<Response, _>(&mut cursor, body_len - 1) {
        Err(FrameError::TooLarge { len, max }) => {
            assert_eq!(len, body_len);
            assert_eq!(max, body_len - 1);
        }
        other => panic!("expected TooLarge, got {other:?}"),
    }
}

#[test]
fn write_rejects_a_frame_over_the_limit() {
    let mut resp = Response::new();
    resp.set_request_id(1);
    let mut buf = Vec::new();
    match write_frame(&mut buf, &resp, 1) {
        Err(FrameError::TooLarge { max, .. }) => assert_eq!(max, 1),
        other => panic!("expected TooLarge, got {other:?}"),
    }
    assert!(
        buf.is_empty(),
        "nothing is written when the frame is rejected"
    );
}

#[test]
fn a_torn_frame_is_an_error_not_a_clean_eof() {
    // A length prefix promising 100 bytes, followed by only 3: an unexpected EOF, distinct
    // from the clean boundary case.
    let mut bytes = 100u32.to_be_bytes().to_vec();
    bytes.extend_from_slice(b"abc");
    let mut cursor = Cursor::new(bytes);
    match read_frame::<Request, _>(&mut cursor, DEFAULT_MAX_FRAME_LEN) {
        Err(FrameError::Io(err)) => assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof),
        other => panic!("expected Io(UnexpectedEof), got {other:?}"),
    }
}
