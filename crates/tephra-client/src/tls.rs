//! Client-side TLS configuration helpers: building an `Arc<rustls::ClientConfig>` from either the
//! platform's native root store or a caller-supplied CA certificate (for a self-signed or private
//! CA server). Pass the result to [`Client::connect_tls`](crate::Client::connect_tls).

use std::fs::File;
use std::io::{self, BufReader};
use std::path::Path;
use std::sync::Arc;

use rustls::{ClientConfig, RootCertStore};

/// A client configuration trusting the platform's native root certificate store.
pub fn config_with_native_roots() -> io::Result<Arc<ClientConfig>> {
    let mut roots = RootCertStore::empty();
    let loaded = rustls_native_certs::load_native_certs();
    for cert in loaded.certs {
        let _ = roots.add(cert);
    }
    if roots.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "tls: no native root certificates found",
        ));
    }
    Ok(build(roots))
}

/// A client configuration trusting exactly the certificate(s) in `ca_pem`. Use this for a
/// self-signed server certificate (the certificate is its own trust anchor) or a private CA.
pub fn config_with_custom_ca(ca_pem: &Path) -> io::Result<Arc<ClientConfig>> {
    let mut roots = RootCertStore::empty();
    let mut reader = BufReader::new(File::open(ca_pem).map_err(|err| {
        io::Error::new(
            err.kind(),
            format!("tls: open ca {}: {err}", ca_pem.display()),
        )
    })?);
    let mut added = 0;
    for cert in rustls_pemfile::certs(&mut reader) {
        roots
            .add(cert?)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, format!("tls: {err}")))?;
        added += 1;
    }
    if added == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("tls: no certificates in {}", ca_pem.display()),
        ));
    }
    Ok(build(roots))
}

fn build(roots: RootCertStore) -> Arc<ClientConfig> {
    Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    )
}
