# tephra-server

[![Crates.io](https://img.shields.io/crates/v/tephra-server.svg)](https://crates.io/crates/tephra-server)
[![Documentation](https://docs.rs/tephra-server/badge.svg)](https://docs.rs/tephra-server)
[![License](https://img.shields.io/crates/l/tephra-server.svg)](https://github.com/tephradb/tephra/blob/main/LICENSE)

A synchronous, thread-per-connection TCP server exposing a
[tephra](https://crates.io/crates/tephra) event store over the length-prefixed protobuf
protocol.

The model mirrors the engine: tephra is single-writer and synchronous (appends block until
durable, reads run on the caller's own thread over a lock-free snapshot), so each connection is
served on its own thread. Connect to it with
[`tephra-client`](https://crates.io/crates/tephra-client).

## Running

```sh
cargo install tephra-server
tephra-server --bind 127.0.0.1:9000 --data-dir ./tephra-data
```

An official container image is also published; see the repository for the `Dockerfile` and the
GitHub Container Registry image.

## Configuration

Configuration is layered, later sources winning: built-in defaults, then a TOML file passed with
`--config`, then `TEPHRA__*` environment variables, then the command-line flags. The command
line carries only the launch essentials:

```text
tephra-server [--config PATH] [--bind ADDR] [--data-dir DIR] [--log FILTER]
```

Everything else (segment size, group-commit sizing, tips window, planner bias, frame and
read-batch limits) is set in the config file or the environment. Any key can be set from the
environment as `TEPHRA__<SECTION>__<KEY>`, for example
`TEPHRA__WRITER__MAX_BATCH_BYTES=16777216`. See `tephra.example.toml` in this crate for the full
surface, where every value shown is the built-in default.

## Related crates

- [`tephra`](https://crates.io/crates/tephra): the embedded engine this server wraps.
- [`tephra-client`](https://crates.io/crates/tephra-client): the client for this server.
- [`tephra-proto`](https://crates.io/crates/tephra-proto): the wire protocol they share.

## License

Licensed under the [Apache License, Version 2.0](https://github.com/tephradb/tephra/blob/main/LICENSE).
