# Buildx bake definition for the tephra-server images.
#
# Three variants share one base stage:
#   - distroless: the primary image (gcr.io/distroless/cc), published under the plain tags.
#   - debug:      the debian-slim image, same binary with a shell, published under -debug tags.
#   - static:     a FROM scratch image with the fully static musl binary, -static tags.
#
# Local use:
#   docker buildx bake                 # all, for the host platform
#   docker buildx bake distroless      # just the primary image
#
# CI overrides the tags/labels/platforms; see .github/workflows/docker.yml. The
# `docker-metadata-action-*` targets are stubbed here so a local bake works standalone, and
# merged with richer values from docker/metadata-action in CI.

variable "RUST_VERSION" {
  default = "1.95"
}

variable "PROTOC_VERSION" {
  default = "35.1"
}

group "default" {
  targets = ["distroless", "debug", "static"]
}

target "_common" {
  context    = "."
  dockerfile = "Dockerfile"
  args = {
    RUST_VERSION   = RUST_VERSION
    PROTOC_VERSION = PROTOC_VERSION
  }
}

# Tag/label stubs, overridden by docker/metadata-action in CI (matched by target name).
target "docker-metadata-action-distroless" {
  tags = ["tephra:distroless"]
}

target "docker-metadata-action-slim" {
  tags = ["tephra:slim"]
}

target "docker-metadata-action-static" {
  tags = ["tephra:static"]
}

# Primary image.
target "distroless" {
  inherits = ["_common", "docker-metadata-action-distroless"]
  target   = "distroless"
}

# Debug image: same binary on debian-slim, so a shell is available to inspect a running server.
target "debug" {
  inherits = ["_common", "docker-metadata-action-slim"]
  target   = "debug"
}

# Static image: the fully static musl binary on a scratch base, for FROM scratch deployments.
target "static" {
  inherits = ["_common", "docker-metadata-action-static"]
  target   = "static"
}
