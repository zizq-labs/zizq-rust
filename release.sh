#!/usr/bin/env bash

# Build a release artifact for the Zizq Rust client.
#
# Produces:
#   target/release/zizq-<version>.crate
#   target/release/zizq-<version>.crate.sha256
#
# Usage:
#   ./release.sh                          # build only
#   ./release.sh --check                  # verify fmt + clippy + tests pass first
#   ./release.sh --allow-dirty            # forward flags to cargo package
#   ./release.sh --check --allow-dirty    # both at once

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Read version from Cargo's source of truth. `-p zizq` is required in
# a workspace with multiple potential members.
VERSION="$(cargo pkgid -p zizq | sed 's/.*[#@]//')"
CRATE="zizq-${VERSION}.crate"
OUT_DIR="target/release"

echo "==> Zizq Rust Client v${VERSION}"

# Optional pre-flight checks.
if [[ "${1:-}" == "--check" ]]; then
    echo "    Running fmt check..."
    cargo fmt --all --check

    echo "    Running clippy..."
    cargo clippy --all-targets --all-features -- -D warnings

    echo "    Running tests..."
    cargo test
    shift
fi

# Build the .crate file (cargo emits it to target/package/). Any
# remaining arguments are forwarded — pass --allow-dirty locally when
# iterating with uncommitted changes. CI runs from a clean checkout so
# no flag is needed there.
echo "    Packaging..."
cargo package -p zizq "$@"

# Mirror the convention in our other clients by surfacing the artifact under
# target/release/ alongside its checksum.
mkdir -p "$OUT_DIR"
cp "target/package/${CRATE}" "${OUT_DIR}/${CRATE}"

# Checksum.
echo "    Computing checksum..."
(cd "$OUT_DIR" && shasum -a 256 "$CRATE" > "${CRATE}.sha256")

echo "==> Done."
echo "    ${OUT_DIR}/${CRATE}"
echo "    ${OUT_DIR}/${CRATE}.sha256"
