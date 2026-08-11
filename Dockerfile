# syntax=docker/dockerfile:1

# Multi-stage, multi-variant build for the tephra-server binary.
#
# One shared glibc builder feeds two runtime stages:
#   - distroless (default): gcr.io/distroless/cc, published under the plain tags.
#   - debug:                debian-slim, the same binary with a shell for inspection, -debug tags.
#
# The distroless stage is last, so a bare `docker build .` produces the primary image; the slim
# image is `docker build --target debug .`. docker-bake.hcl selects the two by target name.
#
# The tephra-proto crate generates its Rust types by driving protoc at build time, and the
# protobuf Rust crates are version-locked to a matching protoc (see crates/tephra-proto and
# devenv.nix). PROTOC_VERSION must therefore stay in step with that pin.

ARG RUST_VERSION=1.95
ARG DEBIAN_VERSION=bookworm
ARG PROTOC_VERSION=35.1

# --- Builder -------------------------------------------------------------------------------
FROM rust:${RUST_VERSION}-slim-${DEBIAN_VERSION} AS builder
ARG PROTOC_VERSION
ARG TARGETARCH

# protoc, pinned to the version the protobuf crates are locked against. Downloaded from the
# release rather than apt so the version matches exactly across distributions.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl unzip \
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

# --locked builds against the committed Cargo.lock; the cache mounts keep the registry and
# target dir warm across builds without baking them into the image.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked --bin tephra-server \
    && cp target/release/tephra-server /usr/local/bin/tephra-server

# The distroless runtime has no shell to mkdir/chown with, so stage the data directory here
# owned by distroless's nonroot uid (65532) and copy it across.
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

ENTRYPOINT ["tephra-server"]

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

ENTRYPOINT ["tephra-server"]
