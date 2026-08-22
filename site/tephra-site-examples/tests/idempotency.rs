//! Idempotent appends: a `fail_if_exists` clause makes an append fail if its dedupe key already
//! exists anywhere in the log, so retrying a command is a safe no-op rather than a duplicate.

use std::error::Error;

use tephra_client::{
    AppendCondition, Client, ClientError, ErrorCode, Event, Query, QueryItem, Tag, Tags,
};
use tephra_site_examples::TestServer;

/// Applies a command exactly once. Returns `true` if this call applied it, `false` if the same
/// command (by idempotency key) had already been applied.
fn place_order(client: &mut Client, key: &str) -> Result<bool, Box<dyn Error>> {
    // ANCHOR: idempotent
    let dedupe_tag = format!("cmd:{key}");
    let event = Event::new("OrderPlaced", [dedupe_tag.as_str()], b"{}".to_vec())?;

    // `fail_if_exists` asserts this command's dedupe key exists nowhere in the log, independent of
    // any boundary cursor. Its conflict is a distinct `AlreadyExists`, not a boundary `Conflict`.
    let condition =
        AppendCondition::exists_only(Query::item(QueryItem::with_tags(Tags::new([Tag::new(
            dedupe_tag,
        )?])?)));

    match client.append([event], Some(condition)) {
        Ok(_) => Ok(true),
        // `AlreadyExists` means the command was already applied: a successful no-op, not an error
        // to retry (a retry would only be rejected again).
        Err(ClientError::Server {
            code: ErrorCode::AlreadyExists,
            ..
        }) => Ok(false),
        Err(err) => Err(Box::new(err)),
    }
    // ANCHOR_END: idempotent
}

#[test]
fn a_command_applies_at_most_once() {
    let server = TestServer::start();
    let mut client = Client::connect(server.addr()).expect("connect");

    assert!(place_order(&mut client, "order-1").expect("first apply"));

    // Unrelated activity advances the log; idempotency must still hold across it.
    client
        .append(
            [Event::new("Noise", ["k:1"], b"{}".to_vec()).unwrap()],
            None,
        )
        .expect("noise");

    assert!(!place_order(&mut client, "order-1").expect("retry is a no-op"));
    // A different command key is not blocked.
    assert!(place_order(&mut client, "order-2").expect("distinct command"));
}
