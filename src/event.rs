use std::fmt;

use thiserror::Error;

pub struct Event {
    pub event_type: EventType,
    pub tags: Tags,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Default, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct EventType(String);

impl EventType {
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

#[derive(Clone, Debug, Default, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Tag(String);

impl Tag {
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

#[derive(Debug, Error)]
pub enum TagsError {
    #[error("duplicate tag '{tag}'")]
    Duplicate { tag: Tag },
}

pub struct Tags(Vec<Tag>);

impl Tags {
    pub fn new(mut tags: Vec<Tag>) -> Result<Self, TagsError> {
        tags.sort_unstable();
        // Scan for the first adjacent duplicate. `find` short-circuits, and on the
        // error path the `Vec` is discarded, so move the offender out with an O(1)
        // `swap_remove` rather than cloning it.
        if let Some(i) = (1..tags.len()).find(|&i| tags[i] == tags[i - 1]) {
            return Err(TagsError::Duplicate {
                tag: tags.swap_remove(i),
            });
        }
        Ok(Tags(tags))
    }

    /// Constructs `Tags` without checking its invariant.
    ///
    /// # Safety
    ///
    /// The caller must ensure `tags` is sorted in ascending order and contains no
    /// duplicates. `Tags` guarantees that invariant to its consumers, and other
    /// methods may rely on it (e.g. for ordered/set comparison); passing an
    /// unsorted or duplicate-bearing vector silently breaks those guarantees.
    pub unsafe fn from_sorted_unchecked(tags: Vec<Tag>) -> Self {
        Tags(tags)
    }

    pub fn as_slice(&self) -> &[Tag] {
        &self.0
    }
}
