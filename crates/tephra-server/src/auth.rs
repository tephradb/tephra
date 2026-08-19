//! Bearer-token authentication: the accepted-token set the Hello handshake checks against.
//!
//! Tokens are held as SHA-256 digests, not raw strings. Comparing digests is the standard API-key
//! pattern: a match reveals only that the presented token hashes into the set, and a non-constant
//! comparison of digests leaks nothing but hash bits, useless without a preimage, so no
//! constant-time-compare dependency is needed.

use std::collections::HashSet;

use sha2::{Digest, Sha256};

/// The set of accepted bearer tokens, built once at startup and shared across every connection
/// behind an `Arc`. Membership is tested against SHA-256 digests, never the raw token strings.
pub struct AuthConfig {
    digests: HashSet<[u8; 32]>,
}

impl AuthConfig {
    /// Builds the accepted-token set by hashing each token.
    pub fn new(tokens: impl IntoIterator<Item = String>) -> AuthConfig {
        AuthConfig {
            digests: tokens.into_iter().map(|token| digest(&token)).collect(),
        }
    }

    /// Whether `token` is one of the accepted tokens.
    pub fn accepts(&self, token: &str) -> bool {
        self.digests.contains(&digest(token))
    }
}

/// The SHA-256 digest of a token.
fn digest(token: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_a_configured_token_and_rejects_others() {
        let auth = AuthConfig::new(["alpha".to_string(), "beta".to_string()]);
        assert!(auth.accepts("alpha"));
        assert!(auth.accepts("beta"));
        assert!(!auth.accepts("gamma"));
        assert!(!auth.accepts(""));
        // A prefix of a real token must not match: membership is over the whole digest.
        assert!(!auth.accepts("alph"));
    }

    #[test]
    fn an_empty_set_accepts_nothing() {
        let auth = AuthConfig::new(std::iter::empty());
        assert!(!auth.accepts("alpha"));
    }
}
