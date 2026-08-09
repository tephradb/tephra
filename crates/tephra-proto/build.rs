use std::env;
use std::path::PathBuf;

// Generates the Rust protobuf types from `proto/tephra.proto` using the official
// `protobuf-codegen`, which drives `protoc` (found via the `PROTOC` env var or the PATH).
// The crate is version-locked to a matching `protoc` (4.35.1-release <-> protoc 35.1), so
// the toolchain provides `protoc` 35.1 (see devenv.nix).
fn main() {
    println!("cargo:rerun-if-changed=proto/tephra.proto");

    let out_dir =
        PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR set by cargo")).join("generated");

    // Input is given relative to the include dir so protoc's output path (which strips the
    // include prefix) matches where the codegen expects the generated `.u.pb.rs` file.
    protobuf_codegen::CodeGen::new()
        .include("proto")
        .input("tephra.proto")
        .output_dir(&out_dir)
        .generate_and_compile()
        .expect("generate protobuf types");
}
