# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.1](https://github.com/tephradb/tephra/compare/v0.4.0...v0.4.1) - 2026-08-19

### Other

- *(server)* de-flake subscribe_streams_catch_up_then_live

## [0.4.0](https://github.com/tephradb/tephra/compare/v0.3.6...v0.4.0) - 2026-08-19

### Added

- *(server)* use jemalloc as the default global allocator
- [**breaking**] regroup server settings into nested tables and rename RUST_LOG to TEPHRA_LOG
- [**breaking**] authenticate connections with bearer tokens over a mandatory Hello handshake ([#28](https://github.com/tephradb/tephra/pull/28))
- server-authenticated TLS over rustls (sync + async clients) ([#26](https://github.com/tephradb/tephra/pull/26))
- *(server)* reap idle, slow-loris, and stalled connections on timeout ([#24](https://github.com/tephradb/tephra/pull/24))
- *(server)* cap total concurrent connections with max_connections
- publish prebuilt tephra-server binaries and support cargo binstall

### Other

- point repository references at the tephradb/tephra org ([#27](https://github.com/tephradb/tephra/pull/27))
- *(server)* assert the interleave ack precedes ReadEnd instead of a frame count ([#25](https://github.com/tephradb/tephra/pull/25))

## [0.3.6](https://github.com/tephradb/tephra/compare/v0.3.5...v0.3.6) - 2026-08-17

### Other

- *(client)* default AsyncClient to a pool of 4 bulk read sockets

## [0.3.5](https://github.com/tephradb/tephra/compare/v0.3.4...v0.3.5) - 2026-08-17

### Added

- eliminate read head-of-line blocking on the TCP connection ([#20](https://github.com/tephradb/tephra/pull/20))

## [0.3.4](https://github.com/tephradb/tephra/compare/v0.3.3...v0.3.4) - 2026-08-15

### Added

- add an optional Prometheus `/metrics` endpoint ([#19](https://github.com/tephradb/tephra/pull/19))
- add a server stats op and `--healthcheck` probe ([#17](https://github.com/tephradb/tephra/pull/17))

### Other

- *(writer)* raise default max_batch_records to 2048 and queue_capacity to 16384
- shrink the server binary ~24% by dropping the regex-based env-filter

## [0.3.3](https://github.com/tephradb/tephra/compare/v0.3.2...v0.3.3) - 2026-08-14

### Added

- add backwards reads (read_back) across the engine, server, and client ([#15](https://github.com/tephradb/tephra/pull/15))

## [0.3.2](https://github.com/tephradb/tephra/compare/v0.3.1...v0.3.2) - 2026-08-14

### Other

- apply cargo fmt
- import std items instead of inlining full paths

## [0.3.1](https://github.com/tephradb/tephra/compare/v0.3.0...v0.3.1) - 2026-08-13

### Added

- take borrowed `Query` in read methods

### Fixed

- *(tephra)* recover a short trailing segment left by a failed extension ([#7](https://github.com/tephradb/tephra/pull/7))

### Other

- accept owned strings (Into<Box<str>>) in event, tag, and type constructors

## [0.3.0](https://github.com/tephradb/tephra/compare/v0.2.1...v0.3.0) - 2026-08-12

### Fixed

- serve reads from a shared worker pool instead of a thread per request ([#4](https://github.com/tephradb/tephra/pull/4))

## [0.2.1](https://github.com/tephradb/tephra/compare/v0.2.0...v0.2.1) - 2026-08-12

### Added

- handle SIGTERM for graceful shutdown and force-exit on a second signal

### Fixed

- bound outstanding AsyncClient requests and log connection-close paths

## [0.2.0](https://github.com/tephradb/tephra/compare/v0.1.1...v0.2.0) - 2026-08-12

### Added

- add a server-side read limit for result caps and pagination
- add async multiplexing client and concurrent per-connection server

## [0.1.1](https://github.com/tephradb/tephra/compare/v0.1.0...v0.1.1) - 2026-08-11

### Other

- apply cargo fmt
- add crate-level docs and per-crate READMEs
