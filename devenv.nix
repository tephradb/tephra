{ pkgs, ... }:
{
  languages.rust.enable = true;

  # protoc for the dcbdb-proto codegen. The official protobuf Rust crates are version-locked
  # to a matching protoc, so this must stay in step with the `protobuf`/`protobuf-codegen`
  # pins in crates/dcbdb-proto/Cargo.toml (4.35.1-release <-> protoc 35.1).
  packages = [ pkgs.protobuf ];
}
