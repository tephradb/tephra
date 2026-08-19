# <img src="assets/logo.png" alt="" height="42" align="top"> tephra

An immutable event store with global ordering, built for the Dynamic Consistency Boundary.

Every event carries a type and a set of tags. A query reads exactly the events a decision
depends on, and the same query guards the append that records it, so the consistency boundary is
derived per decision rather than baked into an aggregate. One writer assigns every event a dense,
monotonic position, so the whole log is a single global order and the tag and type indexes are
derived from it and rebuildable.

**Documentation: [tephra.tqwewe.com]**

[tephra.tqwewe.com]: https://tephra.tqwewe.com

## Run the server

```sh
# Docker: listens on 0.0.0.0:9000, data in the tephra-data volume.
docker run -p 9000:9000 -v tephra-data:/data ghcr.io/tephradb/tephra:latest
```

Or install the binary (Linux `x86_64` / `aarch64`):

```sh
curl -fsSL https://tephra.tqwewe.com/install.sh | sh
```

Also `cargo install tephra-server`, `cargo binstall tephra-server`, or
`nix run github:tephradb/tephra`. See [Getting started].

[Getting started]: https://tephra.tqwewe.com/getting-started/

## Clients

Official clients over one protocol: [Rust], [Go], and [JavaScript]. See [Clients].

Prefer the engine in-process? `cargo add tephra` and see [Embedded].

[Rust]: https://crates.io/crates/tephra-client
[Go]: https://github.com/tephradb/tephra-go
[JavaScript]: https://www.npmjs.com/package/@tephradb/client
[Clients]: https://tephra.tqwewe.com/clients/
[Embedded]: https://tephra.tqwewe.com/embedded/

## Learn more

- [ARCHITECTURE.md] covers the design and the alternatives that were rejected.
- [ROADMAP.md] tracks what is done and what is next.

[ARCHITECTURE.md]: ARCHITECTURE.md
[ROADMAP.md]: ROADMAP.md

## License

Apache-2.0.

tephra was built with AI use and careful review. The underlying [seglog] crate was authored by [@tqwewe].

[seglog]: https://crates.io/crates/seglog
[@tqwewe]: https://github.com/tqwewe
