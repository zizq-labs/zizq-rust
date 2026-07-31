#!/usr/bin/env bash

# Build release artifacts for the Zizq Rust client.
#
# Produces (both crates go out in lockstep):
#   target/release/zizq-<version>.crate
#   target/release/zizq-<version>.crate.sha256
#   target/release/zizq-derive-<version>.crate
#   target/release/zizq-derive-<version>.crate.sha256
#
# Usage:
#   ./release.sh                          # build only
#   ./release.sh --check                  # verify fmt + clippy + tests pass first
#   ./release.sh --allow-dirty            # forward flags to cargo package
#   ./release.sh --check --allow-dirty    # both at once

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Read version from Cargo's source of truth. Both crates share a
# version (see `[workspace.package] version` in `Cargo.toml`).
VERSION="$(cargo pkgid -p zizq | sed 's/.*[#@]//')"
CRATE="zizq-${VERSION}.crate"
MACRO_CRATE="zizq-derive-${VERSION}.crate"
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

# Package both crates in one invocation. Passing `-p` for each member
# (rather than `--workspace`) is deliberate — it stays explicit about
# what's being packaged even if the workspace grows dev-only members
# in future. `cargo package` topologically resolves inter-workspace
# deps via a local `target/package/tmp-registry`, so `zizq` verifies
# against the just-packaged `zizq-derive` without needing anything
# on crates.io (this was stabilised alongside the `package-workspace`
# Cargo feature — see https://github.com/rust-lang/cargo/issues/10948).
echo "    Packaging..."
cargo package -p zizq-derive -p zizq "$@"

# Surface both artifacts under target/release/ alongside their sha256s.
mkdir -p "$OUT_DIR"
cp "target/package/${CRATE}" "${OUT_DIR}/${CRATE}"
cp "target/package/${MACRO_CRATE}" "${OUT_DIR}/${MACRO_CRATE}"

echo "    Computing checksums..."
(cd "$OUT_DIR" && shasum -a 256 "$CRATE" > "${CRATE}.sha256")
(cd "$OUT_DIR" && shasum -a 256 "$MACRO_CRATE" > "${MACRO_CRATE}.sha256")

echo "==> Done."
echo "    ${OUT_DIR}/${CRATE}"
echo "    ${OUT_DIR}/${CRATE}.sha256"
echo "    ${OUT_DIR}/${MACRO_CRATE}"
echo "    ${OUT_DIR}/${MACRO_CRATE}.sha256"
