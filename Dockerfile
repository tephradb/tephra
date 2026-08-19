# syntax=docker/dockerfile:1

# Multi-stage, multi-variant build for the tephra-server binary.
#
# One shared base (toolchain + protoc + source) feeds two builders:
#   - builder:        glibc, for the debian-based images.
#   - builder-static: musl, fully static, for the scratch image.
#
# and three runtime stages:
#   - distroless (default): gcr.io/distroless/cc, dynamic glibc, published under the plain tags.
#   - debug:                debian-slim, the same glibc binary with a shell, -debug tags.
#   - static:               FROM scratch, the fully static musl binary, -static tags.
#
# The distroless stage is last, so a bare `docker build .` produces the primary image; the
# others are `docker build --target <name> .`. docker-bake.hcl selects them by target name.
#
# The tephra-proto crate generates its Rust types by driving protoc at build time, and the
# protobuf Rust crates are version-locked to a matching protoc (see crates/tephra-proto and
# devenv.nix). PROTOC_VERSION must therefore stay in step with that pin.

ARG RUST_VERSION=1.95
ARG DEBIAN_VERSION=bookworm
ARG PROTOC_VERSION=35.1

# --- Base: toolchain + protoc + source -----------------------------------------------------
FROM rust:${RUST_VERSION}-slim-${DEBIAN_VERSION} AS base
ARG PROTOC_VERSION
ARG TARGETARCH

# protoc, pinned to the version the protobuf crates are locked against. Downloaded from the
# release rather than apt so the version matches exactly across distributions. make builds the
# vendored C in tikv-jemalloc-sys (the default jemalloc allocator), which the slim image lacks.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl unzip make \
    && rm -rf /var/lib/apt/lists/* \
    && case "${TARGETARCH}" in \
         amd64) protoc_arch=x86_64 ;; \
         arm64) protoc_arch=aarch_64 ;; \
         *) echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
       esac \
    && curl -fsSL -o /tmp/protoc.zip \
       "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-${protoc_arch}.zip" \
    && unzip -q /tmp/protoc.zip -d /usr/local bin/protoc 'include/*' \
    && rm /tmp/protoc.zip
ENV PROTOC=/usr/local/bin/protoc

WORKDIR /build
COPY . .

# --- Builder (glibc) -----------------------------------------------------------------------
FROM base AS builder

# --locked builds against the committed Cargo.lock; the cache mounts keep the registry and
# target dir warm across builds without baking them into the image.
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=tephra-cargo-registry \
    --mount=type=cache,target=/build/target,id=tephra-target-gnu \
    cargo build --release --locked --bin tephra-server \
    && cp target/release/tephra-server /usr/local/bin/tephra-server

# The distroless runtime has no shell to mkdir/chown with, so stage the data directory here
# owned by distroless's nonroot uid (65532) and copy it across.
RUN install -d -o 65532 -g 65532 /data

# --- Builder (musl, fully static) ----------------------------------------------------------
# The musl target links libc statically by default, producing a dependency-free binary. The
# protobuf crate compiles a C component (upb) via the `cc` crate, so a musl C compiler is
# required and the `cc` crate is pointed at it.
FROM base AS builder-static
ARG TARGETARCH

RUN apt-get update \
    && apt-get install -y --no-install-recommends musl-tools musl-dev \
    && rm -rf /var/lib/apt/lists/* \
    && case "${TARGETARCH}" in \
         amd64) rust_target=x86_64-unknown-linux-musl ;; \
         arm64) rust_target=aarch64-unknown-linux-musl ;; \
         *) echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
       esac \
    && rustup target add "${rust_target}" \
    && echo "${rust_target}" > /rust_target

RUN --mount=type=cache,target=/usr/local/cargo/registry,id=tephra-cargo-registry \
    --mount=type=cache,target=/build/target,id=tephra-target-musl \
    rust_target="$(cat /rust_target)" \
    && export "CC_$(echo "${rust_target}" | tr - _)=musl-gcc" \
    && cargo build --release --locked --target "${rust_target}" --bin tephra-server \
    && cp "target/${rust_target}/release/tephra-server" /usr/local/bin/tephra-server

RUN install -d -o 65532 -g 65532 /data

# --- Debug runtime (debian-slim) -----------------------------------------------------------
# Same binary as the primary image, but with a shell so a running server can be inspected.
FROM debian:${DEBIAN_VERSION}-slim AS debug

RUN groupadd --system --gid 10001 tephra \
    && useradd --system --uid 10001 --gid tephra --home-dir /data --shell /usr/sbin/nologin tephra \
    && mkdir -p /data \
    && chown tephra:tephra /data

COPY --from=builder /usr/local/bin/tephra-server /usr/local/bin/tephra-server

USER tephra
WORKDIR /data
VOLUME ["/data"]
EXPOSE 9000

# Bind on all interfaces and store data under the volume by default. Both are TEPHRA__*
# settings, so they can be overridden at `docker run` time (as can any other tuning key).
ENV TEPHRA__BIND=0.0.0.0:9000 \
    TEPHRA__DATA_DIR=/data

# Exec form so no shell is needed; the probe reads TEPHRA__BIND from the container env.
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD ["tephra-server", "--healthcheck"]

ENTRYPOINT ["tephra-server"]

# --- Static runtime (scratch) --------------------------------------------------------------
# The fully static musl binary needs no libc, no dynamic loader, and no shell, so it runs on
# an empty image. USER is numeric because scratch has no /etc/passwd, and the entrypoint is an
# absolute path because there is no PATH to resolve against.
FROM scratch AS static

COPY --from=builder-static /usr/local/bin/tephra-server /usr/local/bin/tephra-server
COPY --from=builder-static --chown=65532:65532 /data /data

USER 65532:65532
WORKDIR /data
VOLUME ["/data"]
EXPOSE 9000

ENV TEPHRA__BIND=0.0.0.0:9000 \
    TEPHRA__DATA_DIR=/data

# Absolute path because scratch has no PATH; exec form because it has no shell.
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/tephra-server", "--healthcheck"]

ENTRYPOINT ["/usr/local/bin/tephra-server"]

# --- Primary runtime (distroless) ----------------------------------------------------------
# Last stage, so a bare `docker build .` yields this image. gcr.io/distroless/cc ships glibc +
# libgcc and nothing else: no shell, no package manager, non-root by default. The :nonroot tag
# defaults USER to 65532 and ships an /etc/passwd entry for it.
FROM gcr.io/distroless/cc-debian12:nonroot AS distroless

COPY --from=builder /usr/local/bin/tephra-server /usr/local/bin/tephra-server
COPY --from=builder --chown=65532:65532 /data /data

WORKDIR /data
VOLUME ["/data"]
EXPOSE 9000

ENV TEPHRA__BIND=0.0.0.0:9000 \
    TEPHRA__DATA_DIR=/data

# Exec form so no shell is needed; the probe reads TEPHRA__BIND from the container env.
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD ["tephra-server", "--healthcheck"]

ENTRYPOINT ["tephra-server"]
