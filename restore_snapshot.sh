#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKUP_ID="${1:-}"

if [ -z "$BACKUP_ID" ]; then
  echo "Usage: $0 <backup-folder-name>"
  echo "Available backups:"
  ls -1 "$ROOT_DIR/backups" 2>/dev/null || true
  exit 1
fi

SRC_DIR="$ROOT_DIR/backups/$BACKUP_ID"
if [ ! -d "$SRC_DIR" ]; then
  echo "Backup not found: $SRC_DIR" >&2
  exit 1
fi

for f in "$SRC_DIR"/*.gs; do
  [ -e "$f" ] || continue
  cp "$f" "$ROOT_DIR/"
done

for name in appsscript.json .clasp.json .nvmrc clasp24.sh; do
  if [ -f "$SRC_DIR/$name" ]; then
    cp "$SRC_DIR/$name" "$ROOT_DIR/$name"
  fi
done

chmod +x "$ROOT_DIR"/*.sh 2>/dev/null || true

echo "Restored from snapshot: $SRC_DIR"
