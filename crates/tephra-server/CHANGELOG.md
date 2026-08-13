# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.1](https://github.com/tqwewe/tephra/compare/v0.3.0...v0.3.1) - 2026-08-13

### Added

- take borrowed `Query` in read methods

### Other

- accept owned strings (Into<Box<str>>) in event, tag, and type constructors

## [0.3.0](https://github.com/tqwewe/tephra/compare/v0.2.1...v0.3.0) - 2026-08-12

### Fixed

- serve reads from a shared worker pool instead of a thread per request ([#4](https://github.com/tqwewe/tephra/pull/4))

## [0.2.1](https://github.com/tqwewe/tephra/compare/v0.2.0...v0.2.1) - 2026-08-12

### Added

- handle SIGTERM for graceful shutdown and force-exit on a second signal

### Fixed

- bound outstanding AsyncClient requests and log connection-close paths

## [0.2.0](https://github.com/tqwewe/tephra/compare/v0.1.1...v0.2.0) - 2026-08-12

### Added

- add a server-side read limit for result caps and pagination
- add async multiplexing client and concurrent per-connection server

## [0.1.1](https://github.com/tqwewe/tephra/compare/v0.1.0...v0.1.1) - 2026-08-11

### Other

- apply cargo fmt
- add crate-level docs and per-crate READMEs
