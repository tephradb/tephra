# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.1](https://github.com/tqwewe/tephra/compare/tephra-v0.2.0...tephra-v0.2.1) - 2026-08-13

### Added

- take borrowed `Query` in read methods

### Other

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
