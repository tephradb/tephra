//! Server-side TLS setup: turning a configured certificate chain and private key into an
//! `Arc<rustls::ServerConfig>`. The record-layer adapters that carry frames over a session live in
//! [`tephra_proto::tls`]; the per-connection handshake and split live in the `conn` module. This
//! module only loads the PEM material and builds the shared configuration once, at startup.

use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::Arc;

use rustls::ServerConfig;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// Builds a TLS server configuration from a PEM certificate chain and a PEM private key. Fails fast
/// on a missing file, an empty chain, or a missing key, so a misconfiguration is a clear startup
/// error rather than a per-connection handshake failure. TLS 1.3 only (the `tls12` rustls feature
/// is not enabled) and no client authentication (server-authenticated TLS; mTLS is a later step).
pub fn build_server_config(cert_path: &Path, key_path: &Path) -> io::Result<Arc<ServerConfig>> {
    let certs = load_certs(cert_path)?;
    let key = load_key(key_path)?;
    ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map(Arc::new)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, format!("tls: {err}")))
}

fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path).map_err(|err| {
        io::Error::new(
            err.kind(),
            format!("tls: open certificate {}: {err}", path.display()),
        )
    })?;
    let certs = CertificateDer::pem_reader_iter(file)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("tls: parse certificate {}: {err}", path.display()),
            )
        })?;
    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("tls: no certificates in {}", path.display()),
        ));
    }
    Ok(certs)
}

fn load_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    let file = File::open(path).map_err(|err| {
        io::Error::new(
            err.kind(),
            format!("tls: open private key {}: {err}", path.display()),
        )
    })?;
    PrivateKeyDer::from_pem_reader(file).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("tls: no usable private key in {}: {err}", path.display()),
        )
    })
}
