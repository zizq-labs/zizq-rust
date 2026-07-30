#!/usr/bin/env bash
set -euo pipefail

# Bump the workspace version.
#
# Usage:
#   ./bump-version.sh           # increment patch (e.g. 0.3.1 -> 0.3.2)
#   ./bump-version.sh 0.4.0     # set an explicit version
#
# The workspace uses `[workspace.package] version = "..."` and each
# member inherits via `version.workspace = true`, so a single edit
# bumps every crate.
#
# Updates:
#   - Cargo.toml (workspace-level version)
#   - Cargo.lock (via cargo update)
#   - zizq/CHANGELOG.md (adds a new section header for the new version)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Read current version from the workspace Cargo.toml.
CURRENT=$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)"/\1/')

if [ -z "$CURRENT" ]; then
    echo "Error: could not read current version from Cargo.toml"
    exit 1
fi

if [ $# -ge 1 ]; then
    NEW="$1"
else
    # Increment patch version.
    IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT"
    PATCH=$((PATCH + 1))
    NEW="${MAJOR}.${MINOR}.${PATCH}"
fi

if [ "$NEW" = "$CURRENT" ]; then
    echo "Already at version ${CURRENT}."
    exit 0
fi

echo "Bumping version: ${CURRENT} -> ${NEW}"

# Update workspace-level version.
sed -i "0,/^version = \"${CURRENT}\"/s//version = \"${NEW}\"/" Cargo.toml
echo "  Updated Cargo.toml (workspace)"

# Update Cargo.lock. If cargo update fails, regenerate the lockfile.
cargo update -p zizq --quiet 2>/dev/null || cargo generate-lockfile --quiet 2>/dev/null || true
echo "  Updated Cargo.lock"

# Add new CHANGELOG section if it doesn't already exist.
CHANGELOG="zizq/CHANGELOG.md"
if [ -f "$CHANGELOG" ] && ! grep -q "^## ${NEW}" "$CHANGELOG" 2>/dev/null; then
    sed -i "0,/^## /s//## ${NEW}\n\n\n## /" "$CHANGELOG"
    echo "  Added ${CHANGELOG} section for ${NEW}"
fi

echo "Done. Version is now ${NEW}."
echo ""
echo "Next steps:"
echo "  1. Edit ${CHANGELOG} with release notes"
echo "  2. Commit: git add -A && git commit -m \"Bump version to ${NEW}\""
