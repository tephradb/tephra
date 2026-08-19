{
  description = "Tephra: a DCB-compliant, immutable event store with global ordering";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    crane.url = "github:ipetkov/crane";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      crane,
      rust-overlay,
      flake-utils,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ (import rust-overlay) ];
        };
        inherit (pkgs) lib;

        # The fully static build targets musl, and is wired up for x86_64 Linux only.
        muslTarget = "x86_64-unknown-linux-musl";
        supportsStatic = system == "x86_64-linux";

        # A toolchain that also carries the musl target's std, so we can cross-compile the
        # static binary. On non-x86_64-linux the extra target is simply omitted.
        rustToolchain = pkgs.rust-bin.stable.latest.default.override {
          targets = lib.optionals supportsStatic [ muslTarget ];
        };
        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

        # Keep the Rust sources and manifests, plus the `.proto` files that
        # tephra-proto's build.rs feeds to protoc during codegen.
        src = lib.fileset.toSource {
          root = ./.;
          fileset = lib.fileset.unions [
            (craneLib.fileset.commonCargoSources ./.)
            (lib.fileset.fileFilter (file: file.hasExt "proto") ./.)
          ];
        };

        # Arguments shared by the dependency build and the crate builds.
        commonArgs = {
          inherit src;
          pname = "tephra";
          version = "0.1.0";
          strictDeps = true;

          # tephra-proto's build.rs drives protoc, located via PROTOC or PATH.
          nativeBuildInputs = [ pkgs.protobuf ];
          PROTOC = "${pkgs.protobuf}/bin/protoc";
        };

        # --- Default: dynamically linked against the host glibc ----------------------------
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        tephra-server = craneLib.buildPackage (
          commonArgs
          // {
            inherit cargoArtifacts;
            pname = "tephra-server";
            cargoExtraArgs = "--locked --package tephra-server";
            doCheck = false;

            meta = {
              description = "Synchronous TCP server exposing a tephra event store over the wire protocol";
              homepage = "https://github.com/tephradb/tephra";
              license = lib.licenses.asl20;
              mainProgram = "tephra-server";
            };
          }
        );

        # --- Static: fully static musl binary (no glibc, no libgcc_s) ----------------------
        # The musl target links libc statically by default; +crt-static makes it fully static,
        # and the musl cross toolchain provides the linker plus the static libc/CRT objects.
        muslCC = pkgs.pkgsCross.musl64.stdenv.cc;

        staticArgs = commonArgs // {
          CARGO_BUILD_TARGET = muslTarget;
          CARGO_BUILD_RUSTFLAGS = "-C target-feature=+crt-static";
          CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER = "${muslCC}/bin/${muslCC.targetPrefix}cc";

          # The protobuf crate compiles a C component (upb) via the `cc` crate. Build it with
          # the musl compiler so it links against musl, and drop glibc fortify (musl has no
          # __*_chk symbols, so _FORTIFY_SOURCE would leave undefined references).
          "CC_x86_64_unknown_linux_musl" = "${muslCC}/bin/${muslCC.targetPrefix}cc";
          hardeningDisable = [ "fortify" ];
        };

        cargoArtifactsStatic = craneLib.buildDepsOnly staticArgs;

        tephra-server-static = craneLib.buildPackage (
          staticArgs
          // {
            cargoArtifacts = cargoArtifactsStatic;
            pname = "tephra-server-static";
            cargoExtraArgs = "--locked --package tephra-server";
            doCheck = false;

            meta = {
              description = "Fully static (musl) tephra-server binary, runnable on any Linux and in FROM scratch images";
              homepage = "https://github.com/tephradb/tephra";
              license = lib.licenses.asl20;
              mainProgram = "tephra-server";
            };
          }
        );
      in
      {
        packages = {
          default = tephra-server;
          tephra-server = tephra-server;
        }
        // lib.optionalAttrs supportsStatic {
          tephra-server-static = tephra-server-static;
        };

        apps.default = flake-utils.lib.mkApp { drv = tephra-server; };

        checks = {
          inherit tephra-server;

          # `cargo fmt --check` over the whole workspace.
          fmt = craneLib.cargoFmt { inherit src; };

          # Workspace tests, reusing the cached dependency build.
          test = craneLib.cargoTest (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoTestExtraArgs = "--workspace";
            }
          );
        };

        devShells.default = craneLib.devShell {
          checks = self.checks.${system};
          packages = [ pkgs.protobuf ];
          PROTOC = "${pkgs.protobuf}/bin/protoc";
        };
      }
    );
}
