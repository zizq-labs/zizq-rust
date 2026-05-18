#!/usr/bin/env bash

# Run the Rust client integration tests against a real Zizq server.
#
# Usage:
#   ./run.sh --binary /path/to/zizq --crate /path/to/zizq-0.1.0.crate
#
# The server is started on a random OS-assigned port (--port 0) and the
# actual bound address is parsed from its JSON log output. The test
# receives ZIZQ_URL as an environment variable so it doesn't need to
# know about server lifecycle.
#
# The test runs in an isolated temp directory and builds against the
# packaged .crate artifact, not the local source tree.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

BINARY=""
CRATE=""
LICENSE_KEY=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --binary)      BINARY="$(cd "$(dirname "$2")" && pwd)/$(basename "$2")"; shift 2 ;;
        --crate)       CRATE="$(cd "$(dirname "$2")" && pwd)/$(basename "$2")"; shift 2 ;;
        --license-key) LICENSE_KEY="$2"; shift 2 ;;
        *)             echo "Unknown arg: $1"; exit 1 ;;
    esac
done

if [[ -z "$BINARY" || -z "$CRATE" ]]; then
    echo "Usage: ./run.sh --binary /path/to/zizq --crate /path/to/zizq-x.y.z.crate [--license-key KEY]"
    exit 1
fi

if [[ ! -x "$BINARY" ]]; then
    echo "Error: binary not found or not executable: $BINARY"
    exit 1
fi

if [[ ! -f "$CRATE" ]]; then
    echo "Error: crate not found: $CRATE"
    exit 1
fi

# --- Set up isolated work directory ---

WORKDIR="$(mktemp -d)"
SERVER_ROOT="$(mktemp -d)"

cleanup() {
    if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$WORKDIR" "$SERVER_ROOT"
}
trap cleanup EXIT

echo "==> Setting up integration test (Rust $(rustc --version | awk '{print $2}'))"

# --- Unpack the packaged crate ---
#
# Extract the .crate (a gzipped tarball) into the work directory and
# rename it to a fixed `zizq/` path, which the integration crate's
# manifest depends on via `path = "zizq"`. The tests therefore build
# against the exact published artifact, not the local source tree.

cp -R "$SCRIPT_DIR/Cargo.toml" "$SCRIPT_DIR/src" "$SCRIPT_DIR/tests" "$WORKDIR/"

echo "    Unpacking crate..."
tar -xzf "$CRATE" -C "$WORKDIR"
mv "$WORKDIR"/zizq-*/ "$WORKDIR/zizq"

cd "$WORKDIR"

# --- Start the server ---

echo "    Starting Zizq server..."

# Start zizq with port 0 (OS-assigned) and JSON logging so we can
# parse the actual bound address from the log output.
SERVER_LOG="$(mktemp)"
SERVER_ARGS=(serve --port 0 --no-admin --root-dir "$SERVER_ROOT" --log-format json --log-level info)
if [[ -n "$LICENSE_KEY" ]]; then
    SERVER_ARGS+=(--license-key "$LICENSE_KEY")
fi

"$BINARY" "${SERVER_ARGS[@]}" > "$SERVER_LOG" 2>&1 &
SERVER_PID=$!

# Wait for the "listening" log line with api="primary" and extract the
# address. The key `api: "primary"` is a stable machine-readable field;
# the message text may change.
ZIZQ_URL=""
DEADLINE=$((SECONDS + 10))
while [[ $SECONDS -lt $DEADLINE ]]; do
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "Error: server exited unexpectedly:"
        cat "$SERVER_LOG"
        exit 1
    fi

    LINE="$(grep '"api":"primary"' "$SERVER_LOG" 2>/dev/null || true)"
    if [[ -n "$LINE" ]]; then
        ADDR="$(echo "$LINE" | jq -r '.fields.addr')"
        SCHEME="$(echo "$LINE" | jq -r '.fields.scheme')"
        ZIZQ_URL="${SCHEME}://${ADDR}"
        break
    fi

    sleep 0.1
done

if [[ -z "$ZIZQ_URL" ]]; then
    echo "Error: timed out waiting for server to start."
    cat "$SERVER_LOG"
    exit 1
fi

echo "    Server listening on ${ZIZQ_URL}"

# --- Run tests ---
#
# `--test-threads=1` runs the scenarios sequentially: each one wipes
# the server's job set on entry, which would race under parallelism.

echo "    Running integration tests..."
ZIZQ_URL="$ZIZQ_URL" cargo test -- --test-threads=1
