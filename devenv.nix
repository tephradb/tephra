{ pkgs, ... }:
{
  languages.rust.enable = true;

  # protoc for the tephra-proto codegen. The official protobuf Rust crates are version-locked
  # to a matching protoc, so this must stay in step with the `protobuf`/`protobuf-codegen`
  # pins in crates/tephra-proto/Cargo.toml (4.35.1-release <-> protoc 35.1).
  packages = [ pkgs.protobuf ];
}
