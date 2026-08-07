//! String interners: tag strings to [`TermId`], event-type strings to [`TypeId`].
//!
//! Two concrete interners rather than one generic over the id width. They look almost
//! identical, but their overflow semantics genuinely differ: a segment can hold billions
//! of distinct tags in principle (so the tag interner's `u32` is checked but practically
//! never trips), while distinct event types are low cardinality (10s to 100s), so the
//! type interner's `u16` is a real limit that must be *rejected* with a named error at the
//! point the offending type string is in hand. Collapsing the two behind a generic bound
//! (`From<u32> + TryFrom<usize>` plus per-width error handling) costs more than the ~30
//! lines of duplication it saves.
//!
//! Ids are dense from 0 and segment-local (Lucene-style): each [`TailIndex`](super::TailIndex)
//! owns its own interners, so there is no global id-stability problem.

use std::collections::HashMap;
use std::sync::Arc;

use thiserror::Error;

use super::{TermId, TypeId};

/// A tag string interner. Maps each distinct tag to a dense [`TermId`], and back.
///
/// The forward map and the reverse vec share one `Arc<str>` per term, so interning a new
/// tag allocates the string once (plus a refcount bump) rather than twice. `Arc`, not
/// `Rc`: the index is fed on the writer thread and read cross-thread, so it must stay
/// `Send`. Lookups still take `&str` for free (`Arc<str>: Borrow<str>`), so recording from
/// `EventRef::tags()` and querying from `Tag::as_str()` allocate nothing.
#[derive(Debug, Default)]
pub struct TermInterner {
    map: HashMap<Arc<str>, TermId>,
    /// Id to string, dense: `terms[id]` is the tag interned as `id`.
    terms: Vec<Arc<str>>,
}

impl TermInterner {
    pub fn new() -> Self {
        TermInterner::default()
    }

    /// Returns the id for `tag`, interning it if new.
    pub fn intern(&mut self, tag: &str) -> TermId {
        if let Some(&id) = self.map.get(tag) {
            return id;
        }
        // A segment is bounded to well under 4 GiB and every tagged event costs several
        // bytes, so the count of distinct tags cannot approach `u32::MAX`. Assert rather
        // than truncate: a silent wrap would alias two tags to one id.
        assert!(
            self.terms.len() < u32::MAX as usize,
            "term id space exhausted (u32) in one segment"
        );
        let id = TermId(self.terms.len() as u32);
        let shared: Arc<str> = Arc::from(tag);
        self.terms.push(Arc::clone(&shared));
        self.map.insert(shared, id);
        id
    }

    /// The id for `tag` if it has been interned, without interning it.
    pub fn get(&self, tag: &str) -> Option<TermId> {
        self.map.get(tag).copied()
    }

    /// The tag string for a previously interned id.
    pub fn term(&self, id: TermId) -> &str {
        &self.terms[id.0 as usize]
    }

    pub fn len(&self) -> usize {
        self.terms.len()
    }

    pub fn is_empty(&self) -> bool {
        self.terms.is_empty()
    }
}

/// An event-type interner. Maps each distinct type to a dense [`TypeId`], rejecting more
/// than `u16::MAX + 1` distinct types in one segment (the width of the dense type column).
///
/// Shares one `Arc<str>` per type between the forward map and the reverse vec, like
/// [`TermInterner`].
#[derive(Debug, Default)]
pub struct TypeInterner {
    map: HashMap<Arc<str>, TypeId>,
    types: Vec<Arc<str>>,
}

impl TypeInterner {
    pub fn new() -> Self {
        TypeInterner::default()
    }

    /// Returns the id for `event_type`, interning it if new. Fails with [`TooManyTypes`]
    /// if the segment already holds `u16::MAX + 1` distinct types, so the caller learns
    /// at push time (offending type in hand) rather than at seal.
    pub fn intern(&mut self, event_type: &str) -> Result<TypeId, TooManyTypes> {
        if let Some(&id) = self.map.get(event_type) {
            return Ok(id);
        }
        // Valid ids are 0..=u16::MAX, so the next id fits only while len <= u16::MAX.
        if self.types.len() > u16::MAX as usize {
            return Err(TooManyTypes {
                max: u16::MAX as usize + 1,
            });
        }
        let id = TypeId(self.types.len() as u16);
        let shared: Arc<str> = Arc::from(event_type);
        self.types.push(Arc::clone(&shared));
        self.map.insert(shared, id);
        Ok(id)
    }

    /// The id for `event_type` if it has been interned, without interning it.
    pub fn get(&self, event_type: &str) -> Option<TypeId> {
        self.map.get(event_type).copied()
    }

    /// The type string for a previously interned id.
    pub fn type_name(&self, id: TypeId) -> &str {
        &self.types[id.0 as usize]
    }

    pub fn len(&self) -> usize {
        self.types.len()
    }

    pub fn is_empty(&self) -> bool {
        self.types.is_empty()
    }
}

/// A segment held more distinct event types than the dense `u16` type column can address.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("too many distinct event types in one segment (maximum {max})")]
pub struct TooManyTypes {
    pub max: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn term_ids_are_dense_and_stable() {
        let mut interner = TermInterner::new();
        let a = interner.intern("course:c1");
        let b = interner.intern("student:s1");
        // Re-interning returns the same id.
        assert_eq!(interner.intern("course:c1"), a);
        assert_eq!(interner.intern("student:s1"), b);
        // Dense from 0, in first-seen order.
        assert_eq!(a, TermId(0));
        assert_eq!(b, TermId(1));
        assert_eq!(interner.len(), 2);
    }

    #[test]
    fn term_get_and_round_trip() {
        let mut interner = TermInterner::new();
        assert_eq!(interner.get("absent"), None);
        let id = interner.intern("course:c1");
        assert_eq!(interner.get("course:c1"), Some(id));
        assert_eq!(interner.term(id), "course:c1");
    }

    #[test]
    fn type_ids_are_dense_and_stable() {
        let mut interner = TypeInterner::new();
        let a = interner.intern("Registered").unwrap();
        let b = interner.intern("Enrolled").unwrap();
        assert_eq!(interner.intern("Registered").unwrap(), a);
        assert_eq!(a, TypeId(0));
        assert_eq!(b, TypeId(1));
        assert_eq!(interner.get("Enrolled"), Some(b));
        assert_eq!(interner.type_name(b), "Enrolled");
    }

    #[test]
    fn type_interner_rejects_overflow() {
        let mut interner = TypeInterner::new();
        // Fill ids 0..=u16::MAX (that many distinct types is allowed).
        for i in 0..=u16::MAX as u32 {
            interner.intern(&format!("T{i}")).unwrap();
        }
        assert_eq!(interner.len(), u16::MAX as usize + 1);
        // One more distinct type overflows the u16 id space.
        let err = interner.intern("one-too-many").unwrap_err();
        assert_eq!(
            err,
            TooManyTypes {
                max: u16::MAX as usize + 1
            }
        );
        // An already-interned type still resolves (no new id needed).
        assert!(interner.intern("T0").is_ok());
    }
}
