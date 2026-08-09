//! Per-connection request handling: sequential request/response over one socket, with reads
//! streamed as multiple frames.

use std::io::{BufReader, BufWriter, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use dcbdb::read::WaitOutcome;
use dcbdb::writer::WriteHandle;
use dcbdb::{Event, Position};

use dcbdb_proto::dcbdb as pb;
use dcbdb_proto::{FrameError, read_frame, write_frame};

use crate::ServerConfig;
use crate::convert;

/// Serves one connection until the client disconnects, the socket is shut down, or a
/// transport error occurs. Each request is answered fully (a read streams several frames)
/// before the next is read.
pub(crate) fn serve_connection(
    stream: TcpStream,
    handle: WriteHandle,
    config: ServerConfig,
    running: Arc<AtomicBool>,
) {
    let peer = stream.peer_addr().ok();
    if let Err(err) = stream.set_nodelay(true) {
        tracing::warn!(?peer, %err, "failed to set TCP_NODELAY");
    }

    // A cloned handle for reading and the original for writing: the two halves address the
    // same socket, and request/response is sequential, so they never race each other.
    let read_stream = match stream.try_clone() {
        Ok(clone) => clone,
        Err(err) => {
            tracing::warn!(?peer, %err, "failed to clone connection stream");
            return;
        }
    };
    let mut reader = BufReader::new(read_stream);
    let mut writer = BufWriter::new(stream);

    loop {
        let request = match read_frame::<pb::Request, _>(&mut reader, config.max_frame_len) {
            Ok(Some(request)) => request,
            Ok(None) => break, // clean close at a frame boundary
            Err(err) => {
                // Tell the client why before closing, when the failure is at a nameable frame
                // boundary (oversized or unparseable). The request id is unknown (the frame
                // never decoded), so it is reported as 0. A transport or encode error cannot be
                // communicated (this also covers a socket shutdown during graceful stop), so
                // just close.
                let error = match &err {
                    FrameError::TooLarge { .. } => Some(convert::too_large(err.to_string())),
                    FrameError::Parse(_) => Some(convert::bad_request(err.to_string())),
                    FrameError::Io(_) | FrameError::Serialize(_) => None,
                };
                if let Some(error) = error {
                    let _ = send(
                        &mut writer,
                        0,
                        ResponseKind::Error(error),
                        config.max_frame_len,
                    );
                }
                tracing::debug!(?peer, %err, "connection read ended");
                break;
            }
        };

        if let Err(err) = dispatch(&request, &handle, &config, &running, &mut writer) {
            tracing::debug!(?peer, %err, "connection write ended");
            break;
        }
    }
}

/// Handles one request, writing its full response (one frame for an append, several for a
/// read). A transport error is returned so the caller drops the connection.
fn dispatch(
    request: &pb::Request,
    handle: &WriteHandle,
    config: &ServerConfig,
    running: &AtomicBool,
    writer: &mut BufWriter<TcpStream>,
) -> Result<(), FrameError> {
    let request_id = request.request_id();
    match request.kind() {
        pb::request::KindOneof::Append(append) => {
            handle_append(request_id, append, handle, config, writer)
        }
        pb::request::KindOneof::Read(read) => handle_read(request_id, read, handle, config, writer),
        pb::request::KindOneof::Subscribe(subscribe) => {
            handle_subscribe(request_id, subscribe, handle, config, running, writer)
        }
        // No kind set, or a future kind this server does not understand.
        _ => {
            let error = convert::bad_request("request has no append, read, or subscribe set");
            send(
                writer,
                request_id,
                ResponseKind::Error(error),
                config.max_frame_len,
            )
        }
    }
}

fn handle_append(
    request_id: u64,
    append: pb::AppendRequestView<'_>,
    handle: &WriteHandle,
    config: &ServerConfig,
    writer: &mut BufWriter<TcpStream>,
) -> Result<(), FrameError> {
    let events = match convert::events_from_proto(append) {
        Ok(events) => events,
        Err(err) => {
            let error = convert::bad_request(err);
            return send(
                writer,
                request_id,
                ResponseKind::Error(error),
                config.max_frame_len,
            );
        }
    };
    let condition = match append.condition_opt() {
        Some(condition) => match convert::condition_from_proto(condition) {
            Ok(condition) => Some(condition),
            Err(err) => {
                let error = convert::bad_request(err);
                return send(
                    writer,
                    request_id,
                    ResponseKind::Error(error),
                    config.max_frame_len,
                );
            }
        },
        None => None,
    };

    match handle.append(events, condition) {
        Ok(range) => {
            let mut ok = pb::AppendResponse::new();
            ok.set_first(range.first.get());
            ok.set_last(range.last.get());
            send(
                writer,
                request_id,
                ResponseKind::Append(ok),
                config.max_frame_len,
            )
        }
        Err(err) => {
            let error = convert::append_error_to_proto(&err);
            send(
                writer,
                request_id,
                ResponseKind::Error(error),
                config.max_frame_len,
            )
        }
    }
}

fn handle_read(
    request_id: u64,
    read: pb::ReadRequestView<'_>,
    handle: &WriteHandle,
    config: &ServerConfig,
    writer: &mut BufWriter<TcpStream>,
) -> Result<(), FrameError> {
    let query = match convert::query_from_proto(read.query()) {
        Ok(query) => query,
        Err(err) => {
            let error = convert::bad_request(err);
            return send(
                writer,
                request_id,
                ResponseKind::Error(error),
                config.max_frame_len,
            );
        }
    };
    let after = Position::new(read.after());

    let mut reads = handle.read(query, after);
    let watermark = reads.watermark();

    let mut batch = pb::ReadEvents::new();
    let mut batch_bytes = 0usize;

    while let Some(item) = reads.next() {
        let sequenced = match item {
            Ok(sequenced) => sequenced,
            Err(err) => {
                // Terminate the stream with a single error frame; the log is the source of
                // truth, so this is an integrity failure, not a normal empty result.
                let error = convert::internal_error(err);
                return send(
                    writer,
                    request_id,
                    ResponseKind::Error(error),
                    config.max_frame_len,
                );
            }
        };
        batch_bytes += sequenced.event.as_bytes().len();
        batch.events_mut().push(convert::sequenced_to_proto(
            sequenced.position,
            sequenced.event,
        ));

        if batch.events().len() >= config.read_batch_events
            || batch_bytes >= config.read_batch_bytes
        {
            send(
                writer,
                request_id,
                ResponseKind::ReadEvents(batch),
                config.max_frame_len,
            )?;
            batch = pb::ReadEvents::new();
            batch_bytes = 0;
        }
    }

    if !batch.events().is_empty() {
        send(
            writer,
            request_id,
            ResponseKind::ReadEvents(batch),
            config.max_frame_len,
        )?;
    }

    let mut end = pb::ReadEnd::new();
    end.set_watermark(watermark.get());
    send(
        writer,
        request_id,
        ResponseKind::ReadEnd(end),
        config.max_frame_len,
    )
}

/// Serves a live subscription: catch up on matching events after `after`, then tail new ones,
/// framing events exactly like [`handle_read`]. Unlike a read it never sends a `ReadEnd`; it
/// runs until the connection breaks, the store shuts down, or the server stops. The connection
/// is dedicated to this subscription from here on (the request loop is not re-entered).
fn handle_subscribe(
    request_id: u64,
    subscribe: pb::SubscribeRequestView<'_>,
    handle: &WriteHandle,
    config: &ServerConfig,
    running: &AtomicBool,
    writer: &mut BufWriter<TcpStream>,
) -> Result<(), FrameError> {
    let query = match convert::query_from_proto(subscribe.query()) {
        Ok(query) => query,
        Err(err) => {
            let error = convert::bad_request(err);
            return send(
                writer,
                request_id,
                ResponseKind::Error(error),
                config.max_frame_len,
            );
        }
    };
    let after = Position::new(subscribe.after());
    let mut sub = handle.subscribe(query, after);

    // Whether a `SubscribeCaughtUp` has already been sent for the current live edge. It is set
    // when we reach the edge and cleared the moment we deliver anything, so the marker fires
    // exactly once per catch-up burst (re-armed) rather than once per wait tick: an idle
    // subscription must not turn the bounded wait into a heartbeat frame.
    let mut announced = false;

    loop {
        // Observe server shutdown even mid-catch-up and between waits.
        if !running.load(Ordering::Acquire) {
            return Ok(());
        }
        let batch = match sub.poll_batch() {
            Ok(batch) => batch,
            Err(err) => {
                // An integrity failure reading the log: terminate with an error frame.
                let error = convert::internal_error(err);
                return send(
                    writer,
                    request_id,
                    ResponseKind::Error(error),
                    config.max_frame_len,
                );
            }
        };

        if batch.is_empty() {
            // Reached the live edge. Announce caught-up once for this edge (non-decreasing
            // watermark), then block for the next commit. A subsequent tick re-polls empty
            // without re-announcing; the bounded wait only keeps the subscription responsive
            // to `running`.
            if !announced {
                let mut caught_up = pb::SubscribeCaughtUp::new();
                caught_up.set_watermark(sub.position().get());
                send(
                    writer,
                    request_id,
                    ResponseKind::CaughtUp(caught_up),
                    config.max_frame_len,
                )?;
                announced = true;
            }
            match sub.wait_timeout(config.subscribe_wait_tick) {
                // New events, or just a tick: loop and re-poll (the top re-checks `running`).
                WaitOutcome::Advanced | WaitOutcome::TimedOut => {}
                // The write coordinator shut down: no more events will ever arrive.
                WaitOutcome::Closed => return Ok(()),
            }
        } else {
            send_event_batch(request_id, &batch, config, writer)?;
            // Delivered events: the next time we reach the edge is a new one, re-arm.
            announced = false;
        }
    }
}

/// Frames a batch of subscription events into one or more `ReadEvents` responses, flushing on
/// the same event-count / byte thresholds a streamed read uses.
fn send_event_batch(
    request_id: u64,
    events: &[(Position, Event)],
    config: &ServerConfig,
    writer: &mut BufWriter<TcpStream>,
) -> Result<(), FrameError> {
    let mut batch = pb::ReadEvents::new();
    let mut batch_bytes = 0usize;
    for (position, event) in events {
        batch_bytes += event.as_bytes().len();
        batch
            .events_mut()
            .push(convert::sequenced_to_proto(*position, event.as_ref()));
        if batch.events().len() >= config.read_batch_events
            || batch_bytes >= config.read_batch_bytes
        {
            send(
                writer,
                request_id,
                ResponseKind::ReadEvents(batch),
                config.max_frame_len,
            )?;
            batch = pb::ReadEvents::new();
            batch_bytes = 0;
        }
    }
    if !batch.events().is_empty() {
        send(
            writer,
            request_id,
            ResponseKind::ReadEvents(batch),
            config.max_frame_len,
        )?;
    }
    Ok(())
}

/// The payload of one response frame.
enum ResponseKind {
    Append(pb::AppendResponse),
    ReadEvents(pb::ReadEvents),
    ReadEnd(pb::ReadEnd),
    CaughtUp(pb::SubscribeCaughtUp),
    Error(pb::ErrorResponse),
}

/// Frames one `Response` (with its `request_id` echoed) and flushes it, so a streamed read
/// is delivered frame by frame rather than buffered until the end.
fn send(
    writer: &mut BufWriter<TcpStream>,
    request_id: u64,
    kind: ResponseKind,
    max_frame_len: u32,
) -> Result<(), FrameError> {
    let mut response = pb::Response::new();
    response.set_request_id(request_id);
    match kind {
        ResponseKind::Append(append) => response.set_append(append),
        ResponseKind::ReadEvents(events) => response.set_read_events(events),
        ResponseKind::ReadEnd(end) => response.set_read_end(end),
        ResponseKind::CaughtUp(caught_up) => response.set_caught_up(caught_up),
        ResponseKind::Error(error) => response.set_error(error),
    }
    write_frame(writer, &response, max_frame_len)?;
    writer.flush()?;
    Ok(())
}
