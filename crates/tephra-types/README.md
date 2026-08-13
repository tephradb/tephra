# tephra-types

[![Crates.io](https://img.shields.io/crates/v/tephra-types.svg)](https://crates.io/crates/tephra-types)
[![Documentation](https://docs.rs/tephra-types/badge.svg)](https://docs.rs/tephra-types)
[![License](https://img.shields.io/crates/l/tephra-types.svg)](https://github.com/tqwewe/tephra/blob/main/LICENSE)

Shared vocabulary types for [tephra](https://crates.io/crates/tephra): the concepts a client
and the storage engine both speak.

This crate is pure data and validation with no I/O and no storage machinery, so a client can
link it (and the wire protocol) without pulling in the engine. It holds:

- `Position`: the dense, monotonic, 1-based event position (position 0 is the "before
  everything" sentinel).
- `EventType`, `Tag`, and the sorted, deduplicated `Tags` set, validated on construction.
- The query model: `Query`, `QueryItem`, and `AppendCondition` (OR across items, AND within an
  item's tags).

It is the single source of truth for name and tag validation across the workspace.

## Example

```rust
use tephra_types::{AppendCondition, Query, QueryItem, Tag, Tags};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A query item matches events carrying all of the listed tags.
    let item = QueryItem::with_tags(Tags::new([Tag::new("course:c1")?])?);
    let query = Query::item(item);

    // The same query guards an append: fail if any matching event already exists.
    let _condition = AppendCondition::new(query);
    Ok(())
}
```

## Related crates

- [`tephra`](https://crates.io/crates/tephra): the embedded event store engine.
- [`tephra-proto`](https://crates.io/crates/tephra-proto): the wire protocol built on this vocabulary.
- [`tephra-client`](https://crates.io/crates/tephra-client): a synchronous client speaking this vocabulary.

## License

Licensed under the [Apache License, Version 2.0](https://github.com/tqwewe/tephra/blob/main/LICENSE).
