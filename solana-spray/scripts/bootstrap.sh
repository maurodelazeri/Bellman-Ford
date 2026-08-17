#!/usr/bin/env bash
# Fetch restate-server into vendor/ so this project runs from a fresh clone.
#
# Two sources, tried in order:
#   1. the official release tarball from GitHub
#   2. the official Docker image, if releases are unreachable (restricted
#      networks often allow a registry but not github.com)
#
# Nothing is installed system-wide. Delete vendor/ to undo.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENDOR_DIR="${VENDOR_DIR:-$PROJECT_ROOT/vendor/restate}"
RESTATE_VERSION="${RESTATE_VERSION:-1.7.3}"
IMAGE="${RESTATE_IMAGE:-docker.io/restatedev/restate:$RESTATE_VERSION}"

if [[ -x "$VENDOR_DIR/restate-server" ]]; then
  echo "already present: $("$VENDOR_DIR/restate-server" --version)"
  exit 0
fi

mkdir -p "$VENDOR_DIR"
ARCH="$(uname -m)"
case "$ARCH" in
  x86_64|amd64) TARGET="x86_64-unknown-linux-musl" ;;
  aarch64|arm64) TARGET="aarch64-unknown-linux-musl" ;;
  *) echo "unsupported architecture: $ARCH" >&2; exit 1 ;;
esac

TARBALL="restate-server-$TARGET.tar.xz"
URL="https://github.com/restatedev/restate/releases/download/v$RESTATE_VERSION/$TARBALL"

echo "==> trying release tarball: $URL"
if curl -fsSL --max-time 300 "$URL" -o "$VENDOR_DIR/$TARBALL" 2>/dev/null; then
  # The archive wraps its contents in a version-named directory; flatten it so
  # the binary always lands at a predictable path.
  tar -xJf "$VENDOR_DIR/$TARBALL" -C "$VENDOR_DIR" --strip-components=1
  rm -f "$VENDOR_DIR/$TARBALL"
  chmod +x "$VENDOR_DIR"/restate* 2>/dev/null || true
  if [[ -x "$VENDOR_DIR/restate-server" ]]; then
    echo "installed: $("$VENDOR_DIR/restate-server" --version)"
    exit 0
  fi
  echo "tarball did not contain restate-server; falling through" >&2
fi

echo "==> release download unavailable, falling back to the container image"
if ! command -v docker >/dev/null 2>&1; then
  echo "docker not available either; fetch restate-server manually into $VENDOR_DIR" >&2
  exit 1
fi
if ! docker info >/dev/null 2>&1; then
  echo "the docker daemon is not running; start it, or fetch restate-server" >&2
  echo "manually into $VENDOR_DIR" >&2
  exit 1
fi

docker pull "$IMAGE"
CID="$(docker create "$IMAGE")"
trap 'docker rm -f "$CID" >/dev/null 2>&1 || true' EXIT
for b in restate restate-server restatectl; do
  docker cp "$CID:/usr/local/bin/$b" "$VENDOR_DIR/$b"
done
chmod +x "$VENDOR_DIR"/restate*
echo "installed: $("$VENDOR_DIR/restate-server" --version)"
