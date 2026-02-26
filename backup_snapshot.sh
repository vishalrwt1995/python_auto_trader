#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STAMP="$(date +%Y%m%d_%H%M%S)"
DEST_DIR="$ROOT_DIR/backups/$STAMP"

mkdir -p "$DEST_DIR"

copy_if_exists() {
  local src="$1"
  if [ -f "$src" ]; then
    cp "$src" "$DEST_DIR/"
  fi
}

copy_if_exists "$ROOT_DIR/appsscript.json"
copy_if_exists "$ROOT_DIR/.clasp.json"
copy_if_exists "$ROOT_DIR/.nvmrc"
copy_if_exists "$ROOT_DIR/clasp24.sh"

for f in "$ROOT_DIR"/*.gs; do
  [ -e "$f" ] || continue
  cp "$f" "$DEST_DIR/"
done

if command -v shasum >/dev/null 2>&1; then
  (
    cd "$DEST_DIR"
    shasum -a 256 * > CHECKSUMS.sha256 2>/dev/null || true
  )
fi

cat > "$DEST_DIR/README.txt" <<TXT
Snapshot created: $STAMP
Source folder: $ROOT_DIR
Files: Apps Script source + clasp/project metadata
TXT

echo "Snapshot created: $DEST_DIR"
