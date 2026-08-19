# tephra-proto

[![Crates.io](https://img.shields.io/crates/v/tephra-proto.svg)](https://crates.io/crates/tephra-proto)
[![Documentation](https://docs.rs/tephra-proto/badge.svg)](https://docs.rs/tephra-proto)
[![License](https://img.shields.io/crates/l/tephra-proto.svg)](https://github.com/tephradb/tephra/blob/main/LICENSE)

Wire protocol shared by the [tephra](https://crates.io/crates/tephra) TCP server and client:
the protobuf message types, the length-prefixed framing, and the conversions between the wire
types and the shared [`tephra-types`](https://crates.io/crates/tephra-types) vocabulary.

This crate depends on `tephra-types` but not on the `tephra` storage engine, so a client links
the wire types and the vocabulary without pulling in the engine. It is the contract both ends of
the connection are built against, not something applications usually touch directly: reach for
[`tephra-client`](https://crates.io/crates/tephra-client) to talk to a server, or
[`tephra-server`](https://crates.io/crates/tephra-server) to run one.

Types are generated at build time by `protoc` from `proto/tephra.proto`. The `protobuf` Rust
crates are version-locked to a matching `protoc`, so building requires that exact `protoc`
version on the `PATH` (or via the `PROTOC` environment variable).

## Related crates

- [`tephra-types`](https://crates.io/crates/tephra-types): the shared vocabulary this protocol carries.
- [`tephra-server`](https://crates.io/crates/tephra-server): the server that speaks this protocol.
- [`tephra-client`](https://crates.io/crates/tephra-client): the client that speaks this protocol.

## License

Licensed under the [Apache License, Version 2.0](https://github.com/tephradb/tephra/blob/main/LICENSE).
