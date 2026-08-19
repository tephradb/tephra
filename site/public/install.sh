#!/bin/sh
# Install the tephra-server binary from a GitHub release.
#
#   curl -fsSL https://tephra.tqwewe.com/install.sh | sh
#
# It detects your architecture, downloads the matching prebuilt archive, verifies its
# checksum, and installs `tephra-server`. Prebuilt binaries are Linux (x86_64 / aarch64) only.
#
# Environment overrides:
#   TEPHRA_VERSION      release to install, e.g. v0.4.0 or 0.4.0 (default: the latest release)
#   TEPHRA_INSTALL_DIR  install directory (default: /usr/local/bin, falling back to ~/.local/bin)
#   TEPHRA_LIBC         musl (static, the default) or gnu (dynamically linked against glibc)

set -eu

REPO="tephradb/tephra"
BIN="tephra-server"

info() { echo "install: $*"; }
err() { echo "install: error: $*" >&2; exit 1; }

command -v curl >/dev/null 2>&1 || err "curl is required"
command -v tar >/dev/null 2>&1 || err "tar is required"

os="$(uname -s)"
[ "$os" = "Linux" ] || err "prebuilt binaries are Linux only (got '$os'); build from source instead"

arch="$(uname -m)"
case "$arch" in
    x86_64 | amd64) arch="x86_64" ;;
    aarch64 | arm64) arch="aarch64" ;;
    *) err "unsupported architecture '$arch'" ;;
esac

libc="${TEPHRA_LIBC:-musl}"
case "$libc" in
    musl | gnu) ;;
    *) err "TEPHRA_LIBC must be 'musl' or 'gnu' (got '$libc')" ;;
esac

target="${arch}-unknown-linux-${libc}"

# Resolve the version. The latest server release is the newest tag of the form vX.Y.Z; the
# per-crate releases (tephra-client-v..., seglog-v...) are filtered out by the leading digit.
version="${TEPHRA_VERSION:-}"
if [ -z "$version" ]; then
    info "resolving the latest release"
    version="$(
        curl -fsSL "https://api.github.com/repos/${REPO}/releases?per_page=100" |
            grep -o '"tag_name": *"v[0-9][^"]*"' |
            sed 's/.*"\(v[0-9][^"]*\)".*/\1/' |
            head -n 1
    )"
    [ -n "$version" ] || err "could not determine the latest release; set TEPHRA_VERSION"
fi
case "$version" in
    v*) tag="$version" ;;
    *) tag="v$version" ;;
esac

name="${BIN}-${tag}-${target}"
url="https://github.com/${REPO}/releases/download/${tag}/${name}.tar.gz"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

info "downloading ${name}.tar.gz"
curl -fsSL "$url" -o "$tmp/${name}.tar.gz" || err "download failed: $url"

if curl -fsSL "${url}.sha256" -o "$tmp/${name}.tar.gz.sha256" 2>/dev/null; then
    if command -v sha256sum >/dev/null 2>&1; then
        (cd "$tmp" && sha256sum -c "${name}.tar.gz.sha256" >/dev/null) ||
            err "checksum verification failed"
        info "checksum verified"
    else
        info "sha256sum not found; skipping checksum verification"
    fi
else
    info "no published checksum; skipping verification"
fi

tar -xzf "$tmp/${name}.tar.gz" -C "$tmp"
[ -f "$tmp/${name}/${BIN}" ] || err "archive did not contain ${BIN}"
chmod +x "$tmp/${name}/${BIN}"

dir="${TEPHRA_INSTALL_DIR:-/usr/local/bin}"
mkdir -p "$dir" 2>/dev/null || true
if [ -w "$dir" ]; then
    mv -f "$tmp/${name}/${BIN}" "$dir/${BIN}"
elif [ -n "${TEPHRA_INSTALL_DIR:-}" ]; then
    err "$dir is not writable; set a writable TEPHRA_INSTALL_DIR or run with sudo"
elif command -v sudo >/dev/null 2>&1; then
    info "$dir needs elevated permissions; using sudo"
    sudo mkdir -p "$dir" && sudo mv -f "$tmp/${name}/${BIN}" "$dir/${BIN}"
else
    dir="$HOME/.local/bin"
    info "no sudo available; falling back to $dir"
    mkdir -p "$dir"
    mv -f "$tmp/${name}/${BIN}" "$dir/${BIN}"
fi

info "installed ${BIN} ${tag} (${target}) to ${dir}/${BIN}"

case ":$PATH:" in
    *":$dir:"*) ;;
    *) info "note: $dir is not on your PATH; add it with: export PATH=\"$dir:\$PATH\"" ;;
esac

info "run '${BIN} --help' to get started"
