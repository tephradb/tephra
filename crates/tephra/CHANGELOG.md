# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.4](https://github.com/tqwewe/tephra/compare/tephra-v0.3.3...tephra-v0.3.4) - 2026-08-17

### Added

- eliminate read head-of-line blocking on the TCP connection ([#20](https://github.com/tqwewe/tephra/pull/20))

## [0.3.3](https://github.com/tqwewe/tephra/compare/tephra-v0.3.2...tephra-v0.3.3) - 2026-08-15

### Added

- add a server stats op and `--healthcheck` probe ([#17](https://github.com/tqwewe/tephra/pull/17))

### Other

- *(writer)* raise default max_batch_records to 2048 and queue_capacity to 16384

## [0.3.2](https://github.com/tqwewe/tephra/compare/tephra-v0.3.1...tephra-v0.3.2) - 2026-08-14

### Added

- add backwards reads (read_back) across the engine, server, and client ([#15](https://github.com/tqwewe/tephra/pull/15))

## [0.3.1](https://github.com/tqwewe/tephra/compare/tephra-v0.3.0...tephra-v0.3.1) - 2026-08-14

### Added

- *(tephra)* add async subscription tailing, read head accessor, and drain-on-close

### Other

- import std items instead of inlining full paths

## [0.3.0](https://github.com/tqwewe/tephra/compare/tephra-v0.2.0...tephra-v0.3.0) - 2026-08-13

### Added

- *(tephra)* add an async read pool for embedded use ([#9](https://github.com/tqwewe/tephra/pull/9))
- take borrowed `Query` in read methods

### Fixed

- *(tephra)* recover a short trailing segment left by a failed extension ([#7](https://github.com/tqwewe/tephra/pull/7))

### Other

- add workspace checks and make the tree pass them ([#12](https://github.com/tqwewe/tephra/pull/12))
- accept owned strings (Into<Box<str>>) in event, tag, and type constructors

## [0.2.0](https://github.com/tqwewe/tephra/compare/tephra-v0.1.1...tephra-v0.2.0) - 2026-08-12

### Added

- add a server-side read limit for result caps and pagination
- add async multiplexing client and concurrent per-connection server

## [0.1.1](https://github.com/tqwewe/tephra/compare/tephra-v0.1.0...tephra-v0.1.1) - 2026-08-11

### Added

- re-export DEFAULT_MAX_BATCH_EVENTS

### Other

- apply cargo fmt
- add crate-level docs and per-crate READMEs
