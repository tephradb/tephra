//! Event type and tag names, and the sorted [`Tags`] set.
//!
//! An [`EventType`] and a [`Tag`] are arbitrary opaque, non-empty strings (the spec never
//! parses them), each stored as an exact-sized `Box<str>`. [`Tags`] is a sorted,
//! duplicate-free set of tags. These are the addressable surface of an event: they drive
//! queries and the append condition, and they become index keys in the engine.

use std::fmt;

use smallvec::SmallVec;
use thiserror::Error;

/// Maximum length, in bytes, of an [`EventType`] or [`Tag`]. Each is stored with a
/// fixed-width `u16` length in the engine's encoded header, so the field capacity is the
/// limit.
pub const MAX_NAME_LEN: usize = u16::MAX as usize;

// ---------------------------------------------------------------------------
// EventType and Tag
// ---------------------------------------------------------------------------

/// Error constructing an [`EventType`] or [`Tag`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum NameError {
    #[error("{what} must not be empty")]
    Empty { what: &'static str },
    #[error("{what} is {len} bytes, exceeding the {max}-byte maximum")]
    TooLong {
        what: &'static str,
        len: usize,
        max: usize,
    },
}

fn validate_name(s: &str, what: &'static str) -> Result<(), NameError> {
    if s.is_empty() {
        return Err(NameError::Empty { what });
    }
    if s.len() > MAX_NAME_LEN {
        return Err(NameError::TooLong {
            what,
            len: s.len(),
            max: MAX_NAME_LEN,
        });
    }
    Ok(())
}

/// An event type. An arbitrary opaque, non-empty string (the spec never parses it),
/// stored as an exact-sized `Box<str>`.
///
/// There is deliberately no `Deref<Target = str>`: reaching through a newtype makes
/// method resolution ambiguous (`ty.len()` would silently mean the string length).
/// Use [`as_str`](Self::as_str), [`AsRef<str>`], or [`Display`](fmt::Display).
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct EventType(Box<str>);

impl EventType {
    /// Constructs an event type, rejecting empty or over-long input.
    pub fn new(s: impl AsRef<str>) -> Result<Self, NameError> {
        let s = s.as_ref();
        validate_name(s, "event type")?;
        Ok(EventType(Box::from(s)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for EventType {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A tag, e.g. `course:c1`. An arbitrary opaque, non-empty string (the spec does not
/// split it into key/value), stored as an exact-sized `Box<str>`. Like [`EventType`],
/// it has no `Deref`; use [`as_str`](Self::as_str), [`AsRef<str>`], or `Display`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Tag(Box<str>);

impl Tag {
    /// Constructs a tag, rejecting empty or over-long input.
    pub fn new(s: impl AsRef<str>) -> Result<Self, NameError> {
        let s = s.as_ref();
        validate_name(s, "tag")?;
        Ok(Tag(Box::from(s)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for Tag {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

/// Error constructing [`Tags`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum TagsError {
    #[error("duplicate tag '{tag}'")]
    Duplicate { tag: Tag },
}

/// A sorted, duplicate-free set of tags.
///
/// Sortedness is load-bearing: it makes the encoded form canonical (identical sets
/// produce identical bytes) and makes AND-matching a linear merge over two sorted
/// slices. Duplicates are rejected rather than silently deduped, because a duplicate
/// is a caller bug and swallowing it would make an event round-trip to something the
/// caller did not submit.
///
/// A `SmallVec<[Tag; 4]>` rather than a `BTreeSet`: tag sets are 1 to 4 entries, so an
/// inline sorted vec beats node allocation and pointer chasing, and the encoder needs
/// a slice anyway.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Tags(SmallVec<[Tag; 4]>);

impl Tags {
    /// Constructs a tag set, sorting the input and rejecting any duplicate.
    ///
    /// Accepts anything iterable over [`Tag`]: an array literal (`[a, b]`), a `Vec`, or another
    /// iterator. The `impl IntoIterator` bound is what lets an array of any length be passed
    /// directly (unlike `Into<SmallVec<..>>`, which only converts an array of the exact inline
    /// size); collection into the backing `SmallVec` uses its `FromIterator` impl.
    pub fn new(tags: impl IntoIterator<Item = Tag>) -> Result<Self, TagsError> {
        let mut tags: SmallVec<[Tag; 4]> = tags.into_iter().collect();
        tags.sort_unstable();
        // Scan for the first adjacent duplicate. `find` short-circuits, and on the
        // error path the vec is discarded, so move the offender out with an O(1)
        // `swap_remove` rather than cloning it.
        if let Some(i) = (1..tags.len()).find(|&i| tags[i] == tags[i - 1]) {
            return Err(TagsError::Duplicate {
                tag: tags.swap_remove(i),
            });
        }
        Ok(Tags(tags))
    }

    /// The empty tag set.
    pub fn empty() -> Self {
        Tags(SmallVec::new())
    }

    pub fn as_slice(&self) -> &[Tag] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Tag> {
        self.0.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tag(s: &str) -> Tag {
        Tag::new(s).unwrap()
    }

    fn tags(items: &[&str]) -> Tags {
        Tags::new(items.iter().map(|s| tag(s)).collect::<SmallVec<[Tag; 4]>>()).unwrap()
    }

    #[test]
    fn name_rejects_empty() {
        assert_eq!(
            EventType::new(""),
            Err(NameError::Empty { what: "event type" })
        );
        assert_eq!(Tag::new(""), Err(NameError::Empty { what: "tag" }));
    }

    #[test]
    fn name_rejects_over_long() {
        let big = "x".repeat(MAX_NAME_LEN + 1);
        assert_eq!(
            EventType::new(&big),
            Err(NameError::TooLong {
                what: "event type",
                len: MAX_NAME_LEN + 1,
                max: MAX_NAME_LEN,
            })
        );
        // Exactly at the maximum is accepted.
        assert!(Tag::new("y".repeat(MAX_NAME_LEN)).is_ok());
    }

    #[test]
    fn name_accessors_and_ordering() {
        assert_eq!(EventType::new("Registered").unwrap().as_str(), "Registered");
        assert_eq!(<Tag as AsRef<str>>::as_ref(&tag("course:c1")), "course:c1");
        assert!(tag("course:a") < tag("course:b"));
        assert_eq!(format!("{}", tag("student:s1")), "student:s1");
    }

    #[test]
    fn tags_sorts_input() {
        let t = tags(&["course:c1", "student:s1", "admin:a1"]);
        let got: Vec<&str> = t.iter().map(|t| t.as_str()).collect();
        assert_eq!(got, ["admin:a1", "course:c1", "student:s1"]);
    }

    #[test]
    fn tags_rejects_duplicates() {
        let input: SmallVec<[Tag; 4]> = [tag("course:c1"), tag("course:c1")].into_iter().collect();
        assert_eq!(
            Tags::new(input),
            Err(TagsError::Duplicate {
                tag: tag("course:c1")
            })
        );
    }

    #[test]
    fn tags_empty_is_empty() {
        assert!(Tags::empty().is_empty());
        assert_eq!(Tags::empty().len(), 0);
    }
}
