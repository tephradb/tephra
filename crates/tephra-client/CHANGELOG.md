# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.0](https://github.com/tephradb/tephra/compare/tephra-client-v0.4.1...tephra-client-v0.5.0) - 2026-08-19

### Added

- [**breaking**] authenticate connections with bearer tokens over a mandatory Hello handshake ([#28](https://github.com/tephradb/tephra/pull/28))
- server-authenticated TLS over rustls (sync + async clients) ([#26](https://github.com/tephradb/tephra/pull/26))
- *(server)* reap idle, slow-loris, and stalled connections on timeout ([#24](https://github.com/tephradb/tephra/pull/24))
- *(server)* cap total concurrent connections with max_connections

### Other

- expand the tephra-client README with async, TLS, auth, and stats
- point repository references at the tephradb/tephra org ([#27](https://github.com/tephradb/tephra/pull/27))

## [0.4.1](https://github.com/tephradb/tephra/compare/tephra-client-v0.4.0...tephra-client-v0.4.1) - 2026-08-17

### Other

- *(client)* default AsyncClient to a pool of 4 bulk read sockets

## [0.4.0](https://github.com/tephradb/tephra/compare/tephra-client-v0.3.3...tephra-client-v0.4.0) - 2026-08-17

### Added

- eliminate read head-of-line blocking on the TCP connection ([#20](https://github.com/tephradb/tephra/pull/20))

## [0.3.3](https://github.com/tephradb/tephra/compare/tephra-client-v0.3.2...tephra-client-v0.3.3) - 2026-08-15

### Added

- add a server stats op and `--healthcheck` probe ([#17](https://github.com/tephradb/tephra/pull/17))

## [0.3.2](https://github.com/tephradb/tephra/compare/tephra-client-v0.3.1...tephra-client-v0.3.2) - 2026-08-14

### Added

- add backwards reads (read_back) across the engine, server, and client ([#15](https://github.com/tephradb/tephra/pull/15))

## [0.3.1](https://github.com/tephradb/tephra/compare/tephra-client-v0.3.0...tephra-client-v0.3.1) - 2026-08-14

### Other

- import std items instead of inlining full paths

## [0.3.0](https://github.com/tephradb/tephra/compare/tephra-client-v0.2.1...tephra-client-v0.3.0) - 2026-08-13

### Other

- add workspace checks and make the tree pass them ([#12](https://github.com/tephradb/tephra/pull/12))
- accept owned strings (Into<Box<str>>) in event, tag, and type constructors

## [0.2.1](https://github.com/tephradb/tephra/compare/tephra-client-v0.2.0...tephra-client-v0.2.1) - 2026-08-12

### Fixed

- bound outstanding AsyncClient requests and log connection-close paths

## [0.2.0](https://github.com/tephradb/tephra/compare/tephra-client-v0.1.1...tephra-client-v0.2.0) - 2026-08-12

### Added

- add a server-side read limit for result caps and pagination
- add async multiplexing client and concurrent per-connection server

## [0.1.1](https://github.com/tephradb/tephra/compare/tephra-client-v0.1.0...tephra-client-v0.1.1) - 2026-08-11

### Other

- apply cargo fmt
- add crate-level docs and per-crate READMEs
