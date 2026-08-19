# seglog

A simple, high-performance segment log implementation for Rust.

`seglog` provides low-level read and write operations for fixed-size segment files with built-in CRC-32 validation. It's designed for event sourcing systems, write-ahead logs, and other append-only storage use cases.

## Features

- **Single writer, multiple concurrent readers** - Lock-free reads with atomic offset coordination
- **CRC-32 validation** - Automatic data integrity checking on every read
- **Optimized I/O** - Reduces syscalls with optimistic reads (~40% faster for small records)
- **Fixed-size segments** - Pre-allocated files with configurable headers
- **Corruption recovery** - Detects and recovers from partial writes
- **Zero-copy reads** - Returns borrowed data when possible

## Quick Example

```rust,no_run
use seglog::read::{ReadHint, Reader};
use seglog::write::Writer;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a 1 MiB segment with no per-record header (`Writer<0>`).
    let mut writer = Writer::<0>::create("segment.log", 1024 * 1024, 0)?;

    // Append a record and flush it to disk.
    let (offset, _) = writer.append_data(b"event data")?;
    writer.sync()?;

    // Read concurrently, bounded by the writer's flushed offset.
    let flushed = writer.flushed_offset();
    let mut reader = Reader::<0>::open("segment.log", Some(flushed))?;
    let record = reader.read_record(offset, ReadHint::Random)?;

    assert_eq!(record.data.as_ref(), b"event data");
    Ok(())
}
```

## Record Format

Each record consists of an 8-byte header followed by variable-length data:

```text
┌─────────────┬─────────────┬────────────────┐
│ Length (4B) │ CRC-32 (4B) │ Data (N bytes) │
└─────────────┴─────────────┴────────────────┘
```

## Performance Optimizations

### Read Hints

- **`ReadHint::Sequential`** - Uses 64KB read-ahead buffer for streaming access
- **`ReadHint::Random`** - Optimistic reads (header + 2KB) to reduce syscalls

### Optimistic Reads

For random access, the reader performs a single syscall to read the header plus 2KB of data. Since most events in event sourcing are small (< 2KB), this eliminates one syscall per read, improving performance by ~40%.

## Header Support

Reserve space at the beginning of segments for application-specific headers:

```rust,no_run
use seglog::write::Writer;
use std::os::unix::fs::FileExt;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Reserve 64 bytes at the start of the file for an application header.
    const START_OFFSET: u64 = 64;
    let mut writer = Writer::<0>::create("segment.log", 1024 * 1024, START_OFFSET)?;

    // Write the header into the reserved region, before the first record.
    writer.file().write_all_at(b"MAGIC", 0)?;

    // Records automatically start after the reserved header.
    writer.append_data(b"data")?;
    Ok(())
}
```

## Concurrent Safety

The `FlushedOffset` provides atomic coordination between writers and readers:

- Writers update it after calling `sync()`
- Readers check it to avoid reading uncommitted data
- Shared via `Arc` for efficient cloning across threads

This ensures readers never see partial writes or corrupted data.

## Use Cases

- **Event sourcing** - Store domain events in append-only segments
- **Write-ahead logs** - Durable transaction logging
- **Message queues** - Persistent queue segments
- **Time-series data** - Append-only metric storage

## License

Licensed under the [Apache License, Version 2.0](https://github.com/tephradb/tephra/blob/main/LICENSE).
