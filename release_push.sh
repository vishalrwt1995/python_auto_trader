#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MSG="${1:-release $(date '+%Y-%m-%d %H:%M:%S')}"

cd "$ROOT_DIR"

"$ROOT_DIR/backup_snapshot.sh"
"$ROOT_DIR/clasp24.sh" push
"$ROOT_DIR/clasp24.sh" version "$MSG"

echo "Release completed: $MSG"
echo "Use '$ROOT_DIR/clasp24.sh versions' to view immutable versions."
