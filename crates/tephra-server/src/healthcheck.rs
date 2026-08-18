//! The `--healthcheck` probe: connect to a running server, issue a stats request, and report
//! whether it answered. Used by container `HEALTHCHECK` directives, so it needs no shell.

use std::io::{BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::time::Duration;

use tephra_proto::tephra as pb;
use tephra_proto::{DEFAULT_MAX_FRAME_LEN, read_frame, write_frame};

/// How long to wait for the connection and the stats reply before declaring the server
/// unhealthy.
const TIMEOUT: Duration = Duration::from_secs(5);

/// Connects to `bind`, asks for stats, and returns `Ok` only if the server replies with a
/// stats response. Any transport error, timeout, or unexpected reply is a failure.
pub fn probe(bind: &str) -> Result<(), String> {
    probe_with_timeout(bind, TIMEOUT)
}

/// [`probe`] with an explicit timeout, so tests can drive the read-timeout path quickly. A server
/// that accepts the connection but never answers must fail here within `timeout` rather than hang:
/// `read_frame` surfaces the socket read timeout as an error instead of looping on it.
fn probe_with_timeout(bind: &str, timeout: Duration) -> Result<(), String> {
    let addr = connect_addr(bind);
    // connect_timeout needs a resolved SocketAddr, and resolving also bounds the connect for a
    // black-holed host (a plain TcpStream::connect would wait the OS default, up to minutes).
    let target = addr
        .to_socket_addrs()
        .map_err(|err| format!("resolve {addr}: {err}"))?
        .next()
        .ok_or_else(|| format!("no address resolved for {addr}"))?;
    let stream = TcpStream::connect_timeout(&target, timeout)
        .map_err(|err| format!("connect to {addr}: {err}"))?;
    stream.set_read_timeout(Some(timeout)).ok();
    stream.set_write_timeout(Some(timeout)).ok();
    stream.set_nodelay(true).ok();

    let read_half = stream.try_clone().map_err(|err| err.to_string())?;
    let mut reader = BufReader::new(read_half);
    let mut writer = BufWriter::new(stream);

    let mut request = pb::Request::new();
    request.set_request_id(1);
    request.set_stats(pb::StatsRequest::new());
    write_frame(&mut writer, &request, DEFAULT_MAX_FRAME_LEN).map_err(|err| err.to_string())?;
    writer.flush().map_err(|err| err.to_string())?;

    match read_frame::<pb::Response, _>(&mut reader, DEFAULT_MAX_FRAME_LEN) {
        Ok(Some(response)) => match response.kind() {
            pb::response::KindOneof::Stats(_) => Ok(()),
            other => Err(format!("unexpected response: {other:?}")),
        },
        Ok(None) => Err("connection closed before a response".to_string()),
        Err(err) => Err(err.to_string()),
    }
}

/// The address to dial for a bind string. A server bound to an unspecified address
/// (`0.0.0.0`, `[::]`) is reached over loopback; anything else is dialed verbatim.
fn connect_addr(bind: &str) -> String {
    match bind.parse::<SocketAddr>() {
        Ok(addr) if addr.ip().is_unspecified() => {
            if addr.is_ipv6() {
                format!("[::1]:{}", addr.port())
            } else {
                format!("127.0.0.1:{}", addr.port())
            }
        }
        _ => bind.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::thread;
    use std::time::{Duration, Instant};

    use super::{connect_addr, probe_with_timeout};

    #[test]
    fn probe_fails_fast_on_a_silent_server() {
        // A server that accepts the connection but never responds must be reported unhealthy within
        // the timeout, not hang forever. Guards the regression where read_frame looped on a read
        // timeout instead of surfacing it.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        // Accept the probe's connection and hold it open, silent, so the read times out (dropping
        // it instead would send EOF, a different failure path).
        let accepter = thread::spawn(move || {
            if let Ok((stream, _)) = listener.accept() {
                // Outlive the probe's short timeout, then close.
                thread::sleep(Duration::from_secs(1));
                drop(stream);
            }
        });

        let timeout = Duration::from_millis(500);
        let start = Instant::now();
        let result = probe_with_timeout(&addr.to_string(), timeout);
        let elapsed = start.elapsed();

        assert!(
            result.is_err(),
            "a silent server must be reported unhealthy"
        );
        assert!(
            elapsed < timeout * 4,
            "probe hung for {elapsed:?} instead of failing near its {timeout:?} timeout"
        );
        let _ = accepter.join();
    }

    #[test]
    fn unspecified_bind_dials_loopback() {
        assert_eq!(connect_addr("0.0.0.0:9000"), "127.0.0.1:9000");
        assert_eq!(connect_addr("[::]:9000"), "[::1]:9000");
    }

    #[test]
    fn concrete_bind_is_dialed_verbatim() {
        assert_eq!(connect_addr("127.0.0.1:9000"), "127.0.0.1:9000");
        assert_eq!(connect_addr("10.0.0.5:7000"), "10.0.0.5:7000");
        // A hostname (not a SocketAddr) passes through unchanged.
        assert_eq!(connect_addr("tephra.internal:9000"), "tephra.internal:9000");
    }
}
