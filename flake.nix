{
  description = "Tephra: a DCB-compliant, immutable event store with global ordering";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    crane.url = "github:ipetkov/crane";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      crane,
      flake-utils,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
        inherit (pkgs) lib;

        craneLib = crane.mkLib pkgs;

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

        # Build every workspace dependency once, cached independently of the crates.
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
              homepage = "https://github.com/tqwewe/tephra";
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
