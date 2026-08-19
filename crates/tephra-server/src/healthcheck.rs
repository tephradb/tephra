//! The `--healthcheck` probe: connect to a running server, issue a stats request, and report
//! whether it answered. Used by container `HEALTHCHECK` directives, so it needs no shell.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::path::Path;
use std::time::Duration;

use tephra_proto::tephra as pb;
use tephra_proto::{DEFAULT_MAX_FRAME_LEN, read_frame, write_frame};

/// How long to wait for the connection and the stats reply before declaring the server
/// unhealthy.
const TIMEOUT: Duration = Duration::from_secs(5);

/// Connects to `bind`, completes the opening Hello (with `auth_token` when the server requires it),
/// asks for stats, and returns `Ok` only if the server replies with a stats response. Any transport
/// error, timeout, or unexpected reply is a failure. When `tls_cert` is set (the server's configured
/// certificate) the probe connects over TLS, pinning that certificate.
pub fn probe(bind: &str, auth_token: Option<&str>, tls_cert: Option<&Path>) -> Result<(), String> {
    probe_with_timeout(bind, TIMEOUT, auth_token, tls_cert)
}

/// [`probe`] with an explicit timeout, so tests can drive the read-timeout path quickly. A server
/// that accepts the connection but never answers must fail here within `timeout` rather than hang:
/// `read_frame` surfaces the socket read timeout as an error instead of looping on it.
fn probe_with_timeout(
    bind: &str,
    timeout: Duration,
    auth_token: Option<&str>,
    tls_cert: Option<&Path>,
) -> Result<(), String> {
    let addr = connect_addr(bind);
    // connect_timeout needs a resolved SocketAddr, and resolving also bounds the connect for a
    // black-holed host (a plain TcpStream::connect would wait the OS default, up to minutes).
    let target = addr
        .to_socket_addrs()
        .map_err(|err| format!("resolve {addr}: {err}"))?
        .next()
        .ok_or_else(|| format!("no address resolved for {addr}"))?;

    #[cfg(feature = "tls")]
    if let Some(cert) = tls_cert {
        return tls_probe::probe(target, timeout, cert, auth_token);
    }
    #[cfg(not(feature = "tls"))]
    let _ = tls_cert;

    let mut stream = TcpStream::connect_timeout(&target, timeout)
        .map_err(|err| format!("connect to {addr}: {err}"))?;
    stream.set_read_timeout(Some(timeout)).ok();
    stream.set_write_timeout(Some(timeout)).ok();
    stream.set_nodelay(true).ok();
    exchange(&mut stream, auth_token)
}

/// The application exchange, shared by the plaintext and TLS paths: the mandatory opening Hello
/// (version negotiation plus authentication when a token is set), then a stats request. Generic
/// over the transport so a raw `TcpStream` and a TLS stream drive it identically.
fn exchange<S: Read + Write>(stream: &mut S, auth_token: Option<&str>) -> Result<(), String> {
    let hello = tephra_proto::hello_request(1, auth_token);
    write_frame(stream, &hello, DEFAULT_MAX_FRAME_LEN).map_err(|err| err.to_string())?;
    stream.flush().map_err(|err| err.to_string())?;
    match read_frame::<pb::Response, _>(stream, DEFAULT_MAX_FRAME_LEN) {
        Ok(Some(response)) => match response.kind() {
            pb::response::KindOneof::HelloAck(_) => {}
            pb::response::KindOneof::Error(error) => {
                return Err(format!(
                    "hello rejected: {}",
                    error.message().to_str().unwrap_or_default()
                ));
            }
            other => return Err(format!("unexpected hello response: {other:?}")),
        },
        Ok(None) => return Err("connection closed before the hello ack".to_string()),
        Err(err) => return Err(err.to_string()),
    }

    let mut request = pb::Request::new();
    request.set_request_id(2);
    request.set_stats(pb::StatsRequest::new());
    write_frame(stream, &request, DEFAULT_MAX_FRAME_LEN).map_err(|err| err.to_string())?;
    stream.flush().map_err(|err| err.to_string())?;

    match read_frame::<pb::Response, _>(stream, DEFAULT_MAX_FRAME_LEN) {
        Ok(Some(response)) => match response.kind() {
            pb::response::KindOneof::Stats(_) => Ok(()),
            other => Err(format!("unexpected response: {other:?}")),
        },
        Ok(None) => Err("connection closed before a response".to_string()),
        Err(err) => Err(err.to_string()),
    }
}

/// The TLS probe: a handshake whose server certificate is pinned to the server's configured one.
/// The probe is local (loopback) liveness, so it trusts the certificate on disk directly rather
/// than a hostname: it verifies the peer presents exactly that certificate and proves possession of
/// its key (the real handshake-signature check), which sidesteps any certificate name (SAN)
/// dependency the probe would otherwise need.
#[cfg(feature = "tls")]
mod tls_probe {
    use std::net::{SocketAddr, TcpStream};
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{
        ClientConfig, ClientConnection, DigitallySignedStruct, SignatureScheme, StreamOwned,
    };

    pub(super) fn probe(
        target: SocketAddr,
        timeout: Duration,
        cert: &Path,
        auth_token: Option<&str>,
    ) -> Result<(), String> {
        let expected = CertificateDer::from_pem_file(cert)
            .map_err(|err| format!("healthcheck: load certificate {}: {err}", cert.display()))?;
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = Arc::new(PinnedCert {
            expected,
            provider: Arc::clone(&provider),
        });
        let config = ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|err| format!("healthcheck: tls config: {err}"))?
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_no_client_auth();

        let stream = TcpStream::connect_timeout(&target, timeout)
            .map_err(|err| format!("connect to {target}: {err}"))?;
        stream.set_read_timeout(Some(timeout)).ok();
        stream.set_write_timeout(Some(timeout)).ok();
        stream.set_nodelay(true).ok();

        // The pinning verifier ignores the name, but the handshake API requires a valid one.
        let name = ServerName::try_from("localhost")
            .map_err(|err| format!("healthcheck: server name: {err}"))?;
        let conn = ClientConnection::new(Arc::new(config), name)
            .map_err(|err| format!("healthcheck: tls session: {err}"))?;
        let mut tls = StreamOwned::new(conn, stream);
        super::exchange(&mut tls, auth_token)
    }

    /// Accepts the server's certificate if, and only if, it is byte-for-byte the configured one.
    #[derive(Debug)]
    struct PinnedCert {
        expected: CertificateDer<'static>,
        provider: Arc<CryptoProvider>,
    }

    impl ServerCertVerifier for PinnedCert {
        fn verify_server_cert(
            &self,
            end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            if end_entity.as_ref() == self.expected.as_ref() {
                Ok(ServerCertVerified::assertion())
            } else {
                Err(rustls::Error::General(
                    "healthcheck: server certificate does not match the configured certificate"
                        .to_string(),
                ))
            }
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            verify_tls12_signature(
                message,
                cert,
                dss,
                &self.provider.signature_verification_algorithms,
            )
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            verify_tls13_signature(
                message,
                cert,
                dss,
                &self.provider.signature_verification_algorithms,
            )
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.provider
                .signature_verification_algorithms
                .supported_schemes()
        }
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
        let result = probe_with_timeout(&addr.to_string(), timeout, None, None);
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

    #[cfg(feature = "tls")]
    #[test]
    fn probe_succeeds_over_tls_and_rejects_a_mismatched_cert() {
        use std::io::Write as _;

        use tempfile::NamedTempFile;
        use tephra::log::set::{SegmentConfig, SegmentSet};
        use tephra::writer::{WriteCoordinator, WriterConfig};
        use tephra_server::{Server, ServerConfig};

        fn write_cert(pem: &str) -> NamedTempFile {
            let mut file = NamedTempFile::new().unwrap();
            file.write_all(pem.as_bytes()).unwrap();
            file.flush().unwrap();
            file
        }

        let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert = write_cert(&generated.cert.pem());
        let key = write_cert(&generated.signing_key.serialize_pem());

        let dir = tempfile::TempDir::new().unwrap();
        let set = SegmentSet::open(dir.path(), SegmentConfig::new(16 * 1024 * 1024)).unwrap();
        let (coordinator, handle) = WriteCoordinator::start(set, WriterConfig::default()).unwrap();
        let tls = tephra_server::tls::build_server_config(cert.path(), key.path()).unwrap();
        let server = Server::bind("127.0.0.1:0", handle, ServerConfig::default())
            .unwrap()
            .with_data_dir(dir.path())
            .with_tls(tls);
        let addr = server.local_addr();
        let shutdown = server.shutdown_handle();
        let server_thread = thread::spawn(move || server.run().expect("server run"));

        let timeout = Duration::from_secs(5);
        // Pinned to the real certificate: the handshake completes and the probe succeeds.
        let ok = probe_with_timeout(&addr.to_string(), timeout, None, Some(cert.path()));
        assert!(ok.is_ok(), "tls probe should succeed, got {ok:?}");

        // Pinned to a different certificate: the handshake is rejected, so the probe fails.
        let other = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let other_cert = write_cert(&other.cert.pem());
        let bad = probe_with_timeout(&addr.to_string(), timeout, None, Some(other_cert.path()));
        assert!(bad.is_err(), "a mismatched pinned certificate must fail");

        shutdown.shutdown();
        let _ = server_thread.join();
        coordinator.shutdown();
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
