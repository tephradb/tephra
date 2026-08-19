# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/tephradb/tephra/compare/tephra-proto-v0.1.6...tephra-proto-v0.2.0) - 2026-08-19

### Added

- [**breaking**] authenticate connections with bearer tokens over a mandatory Hello handshake ([#28](https://github.com/tephradb/tephra/pull/28))
- server-authenticated TLS over rustls (sync + async clients) ([#26](https://github.com/tephradb/tephra/pull/26))
- *(server)* reap idle, slow-loris, and stalled connections on timeout ([#24](https://github.com/tephradb/tephra/pull/24))
- *(server)* cap total concurrent connections with max_connections

### Other

- point repository references at the tephradb/tephra org ([#27](https://github.com/tephradb/tephra/pull/27))

## [0.1.6](https://github.com/tephradb/tephra/compare/tephra-proto-v0.1.5...tephra-proto-v0.1.6) - 2026-08-15

### Added

- add a server stats op and `--healthcheck` probe ([#17](https://github.com/tephradb/tephra/pull/17))

## [0.1.5](https://github.com/tephradb/tephra/compare/tephra-proto-v0.1.4...tephra-proto-v0.1.5) - 2026-08-14

### Added

- add backwards reads (read_back) across the engine, server, and client ([#15](https://github.com/tephradb/tephra/pull/15))

## [0.1.4](https://github.com/tephradb/tephra/compare/tephra-proto-v0.1.3...tephra-proto-v0.1.4) - 2026-08-14

### Other

- import std items instead of inlining full paths

## [0.1.3](https://github.com/tephradb/tephra/compare/tephra-proto-v0.1.2...tephra-proto-v0.1.3) - 2026-08-13

### Other

- add workspace checks and make the tree pass them ([#12](https://github.com/tephradb/tephra/pull/12))

## [0.1.2](https://github.com/tephradb/tephra/compare/tephra-proto-v0.1.1...tephra-proto-v0.1.2) - 2026-08-12

### Added

- add a server-side read limit for result caps and pagination
- add async multiplexing client and concurrent per-connection server

## [0.1.1](https://github.com/tephradb/tephra/compare/tephra-proto-v0.1.0...tephra-proto-v0.1.1) - 2026-08-11

### Other

- apply cargo fmt
- add crate-level docs and per-crate READMEs
