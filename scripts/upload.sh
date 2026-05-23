#!/usr/bin/env bash
# =============================================================================
# upload.sh — Upload Overture tile output to Cloudflare R2 public bucket
#
# Two modes:
#
#   1. Per-peel (24/7 driver, --peel-idx N): uploads
#      <workdir>/tiles/peel_<N>/{z}/{x}/{y}.parquet to
#      r2:overture-tiles/tiles/{z}/{x}/{y}.parquet (stripping the peel_<N>/
#      prefix), then re-uploads <workdir>/driver-state/tiles_index.json with
#      Cache-Control: no-store, no-cache, must-revalidate, max-age=0 so SPA
#      clients always see the freshest peel data without serving stale index.
#
#   2. Legacy global (no --peel-idx): walks the entire
#      <workdir>/tiles/{z}/{x}/{y}.parquet tree plus
#      <workdir>/tiles/_v13_pass*.json manifests. Used for one-shot pipeline
#      runs and for the legacy v12 *.parquet.gz layout during the migration
#      window.
#
# Tool choice: rclone — handles Content-Encoding / Cache-Control headers
# natively via --header-upload, preserves metadata correctly on re-runs, and
# is idempotent (checksum-compared).
#
# Requirements:
#   - rclone installed (brew install rclone on Mac)
#   - R2 credentials in env vars (never hardcoded):
#       R2_ACCESS_KEY_ID
#       R2_SECRET_ACCESS_KEY
#       R2_ACCOUNT_ID
#
# Usage:
#   # Per-peel (driver invokes this after each peel):
#   R2_* env vars set; bash upload.sh --peel-idx 18 [--workdir /Volumes/SSD-2TB/overture]
#
#   # Legacy / one-shot:
#   R2_* env vars set; bash upload.sh [--workdir /Volumes/SSD-2TB/overture]
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BUCKET="overture-tiles"
WORKDIR="${OVERTURE_WORKDIR:-/Volumes/SSD-2TB/overture}"
PEEL_IDX=""
# Cache-Control applied to the global index file (and ONLY the index file).
# Tile parquet files are content-addressed by (z, x, y) and may be re-emitted
# with different bytes on the next cycle — clients revalidate via ETag.
INDEX_CACHE_CONTROL="no-store, no-cache, must-revalidate, max-age=0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workdir)
      WORKDIR="$2"; shift 2 ;;
    --peel-idx)
      PEEL_IDX="$2"; shift 2 ;;
    *)
      echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

# ---------------------------------------------------------------------------
# Validate credentials
# ---------------------------------------------------------------------------

if [[ -z "${R2_ACCESS_KEY_ID:-}" ]]; then
  echo "ERROR: R2_ACCESS_KEY_ID is not set." >&2; exit 1
fi
if [[ -z "${R2_SECRET_ACCESS_KEY:-}" ]]; then
  echo "ERROR: R2_SECRET_ACCESS_KEY is not set." >&2; exit 1
fi
if [[ -z "${R2_ACCOUNT_ID:-}" ]]; then
  echo "ERROR: R2_ACCOUNT_ID is not set." >&2; exit 1
fi

R2_ENDPOINT="https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

# ---------------------------------------------------------------------------
# Write a temporary rclone config (avoid polluting the user's global config)
# ---------------------------------------------------------------------------

RCLONE_CONFIG=$(mktemp)
trap 'rm -f "$RCLONE_CONFIG"' EXIT

cat > "$RCLONE_CONFIG" <<EOF
[r2]
type = s3
provider = Cloudflare
access_key_id = ${R2_ACCESS_KEY_ID}
secret_access_key = ${R2_SECRET_ACCESS_KEY}
endpoint = ${R2_ENDPOINT}
acl = public-read
EOF

# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------

echo "Uploading tiles from ${WORKDIR} to r2:${BUCKET}"
echo "Endpoint: ${R2_ENDPOINT}"
[ -n "$PEEL_IDX" ] && echo "Mode: per-peel (peel_idx=$PEEL_IDX)" || echo "Mode: legacy global"
echo ""

# ---------------------------------------------------------------------------
# Per-peel mode (24/7 driver)
# ---------------------------------------------------------------------------
#
# rclone copy <src-dir> <dst-dir> recursively syncs the CONTENTS of src-dir
# into dst-dir, preserving the relative directory structure. So:
#
#   src = $WORKDIR/tiles/peel_18/
#         └── 14/8567/5145.parquet
#   dst = r2:overture-tiles/tiles/
#         └── 14/8567/5145.parquet           (peel_18/ prefix stripped)
#
# The peel directory's own _manifest.json is uploaded too (handy for debug
# / cycle-end finalize); it's small and rare-read so no cache headers
# applied beyond rclone defaults.

if [ -n "$PEEL_IDX" ]; then
  PEEL_DIR_NAME=$(printf 'peel_%02d' "$PEEL_IDX")
  PEEL_TILE_DIR="$WORKDIR/tiles/$PEEL_DIR_NAME"

  if [ ! -d "$PEEL_TILE_DIR" ]; then
    echo "ERROR: per-peel tile dir not found at $PEEL_TILE_DIR" >&2
    exit 1
  fi

  FILE_COUNT=$(find "$PEEL_TILE_DIR" -name "*.parquet" -o -name "*.json" | wc -l | tr -d ' ')
  TOTAL_BYTES=$(find "$PEEL_TILE_DIR" \( -name "*.parquet" -o -name "*.json" \) -exec stat -f%z {} + 2>/dev/null | awk '{s+=$1} END {print s+0}' || echo 0)
  TOTAL_MB=$(echo "scale=1; $TOTAL_BYTES / 1048576" | bc 2>/dev/null || echo 0)
  echo "Per-peel upload: $FILE_COUNT files, ${TOTAL_MB} MB (peel=$PEEL_DIR_NAME)"

  # Upload tile parquets — peel_<idx>/ prefix is stripped by rclone copy semantics.
  rclone copy "$PEEL_TILE_DIR" "r2:${BUCKET}/tiles" \
    --config "$RCLONE_CONFIG" \
    --include "*.parquet" \
    --include "_manifest.json" \
    --checksum \
    --transfers 8 \
    --s3-chunk-size 64M \
    --progress \
    --stats 10s

  # Re-upload the global index with no-cache headers. The driver re-writes
  # driver-state/tiles_index.json before invoking this script (via
  # tile_v13_index.update_global_index), so the file on disk is already
  # current for this peel.
  INDEX_PATH="$WORKDIR/driver-state/tiles_index.json"
  if [ -f "$INDEX_PATH" ]; then
    rclone copyto "$INDEX_PATH" "r2:${BUCKET}/tiles/tiles_index.json" \
      --config "$RCLONE_CONFIG" \
      --header-upload "Cache-Control: ${INDEX_CACHE_CONTROL}" \
      --header-upload "Content-Type: application/json" \
      --checksum
    echo "Global index uploaded with Cache-Control: ${INDEX_CACHE_CONTROL}"
  else
    echo "WARN: $INDEX_PATH missing — driver should have written it before upload" >&2
  fi

  MANIFEST_URL="https://pub-${R2_ACCOUNT_ID}.r2.dev/${BUCKET}/tiles/tiles_index.json"
  echo ""
  echo "====================================================================="
  echo "Per-peel upload complete (peel=$PEEL_DIR_NAME)"
  echo "  Files:        ${FILE_COUNT}"
  echo "  Total size:   ${TOTAL_MB} MB"
  echo "  Index URL:    ${MANIFEST_URL}"
  echo "====================================================================="
  exit 0
fi

# ---------------------------------------------------------------------------
# Legacy global mode (--all-peels one-shot or v12 migration)
# ---------------------------------------------------------------------------

# v13 source: <workdir>/tiles/{z}/{x}/{y}.parquet (zstd-3, no gzip wrapper)
# Anchor on the tiles/ subdir so we never accidentally upload raw/ or staging.
TILES_DIR="$WORKDIR/tiles"

if [ ! -d "$TILES_DIR" ]; then
  echo "ERROR: tiles dir not found at $TILES_DIR" >&2
  exit 1
fi

# Count files before upload for summary. Includes both v13 (.parquet under
# tiles/{z}/{x}/{y}/) and legacy v12 (*.parquet.gz under tiles/<theme>/) so
# the during-migration mixed state still reports something sensible.
FILE_COUNT=$(find "$TILES_DIR" \( -name "*.parquet" -o -name "*.parquet.gz" -o -name "_v13_pass*.json" -o -name "manifest.json" \) | wc -l | tr -d ' ')
TOTAL_BYTES=$(find "$TILES_DIR" \( -name "*.parquet" -o -name "*.parquet.gz" -o -name "_v13_pass*.json" -o -name "manifest.json" \) -exec stat -f%z {} + 2>/dev/null | awk '{s+=$1} END {print s+0}' || echo 0)
TOTAL_MB=$(echo "scale=1; $TOTAL_BYTES / 1048576" | bc)

# v13 tiles are zstd-compressed parquet — they are NOT gzip-encoded HTTP
# bodies, so no Content-Encoding header. Legacy v12 .parquet.gz files DO
# need Content-Encoding:gzip; upload them in a separate pass.
rclone copy "$TILES_DIR" "r2:${BUCKET}/tiles" \
  --config "$RCLONE_CONFIG" \
  --include "*.parquet" \
  --include "_v13_pass*.json" \
  --checksum \
  --transfers 8 \
  --s3-chunk-size 64M \
  --progress \
  --stats 10s

# Legacy v12 .parquet.gz, only present during the v12->v13 migration window.
# rclone exits 0 with no work to do if no files match — safe to always run.
rclone copy "$TILES_DIR" "r2:${BUCKET}/tiles" \
  --config "$RCLONE_CONFIG" \
  --include "*.parquet.gz" \
  --header-upload "Content-Encoding:gzip" \
  --checksum \
  --transfers 8 \
  --s3-chunk-size 64M \
  --progress \
  --stats 10s

# Top-level manifest.json (if pass3 / a downstream emit step writes one).
if [ -f "$WORKDIR/manifest.json" ]; then
  rclone copy "$WORKDIR/manifest.json" "r2:${BUCKET}" \
    --config "$RCLONE_CONFIG" \
    --checksum \
    --header-upload "Content-Type:application/json" \
    --progress
fi

# v13 24/7 driver writes the global index under driver-state/; honor that
# in legacy mode too with the same no-cache headers.
INDEX_PATH="$WORKDIR/driver-state/tiles_index.json"
if [ -f "$INDEX_PATH" ]; then
  rclone copyto "$INDEX_PATH" "r2:${BUCKET}/tiles/tiles_index.json" \
    --config "$RCLONE_CONFIG" \
    --header-upload "Cache-Control: ${INDEX_CACHE_CONTROL}" \
    --header-upload "Content-Type: application/json" \
    --checksum
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

MANIFEST_URL="https://pub-${R2_ACCOUNT_ID}.r2.dev/${BUCKET}/manifest.json"

echo ""
echo "====================================================================="
echo "Upload complete"
echo "  Files:        ${FILE_COUNT}"
echo "  Total size:   ${TOTAL_MB} MB"
echo "  Manifest URL: ${MANIFEST_URL}"
echo "====================================================================="

# =============================================================================
# FALLBACK: aws s3 sync (uncomment if rclone is unavailable)
#
# Requires: aws CLI v2 (brew install awscli)
# Note: --content-encoding is not a native aws s3 sync flag; you must set it
# via a per-object copy or via a bucket lifecycle rule. The rclone path above
# handles this correctly with --header-upload.
#
# AWS_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID" \
# AWS_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY" \
#   aws s3 sync "$WORKDIR" "s3://${BUCKET}" \
#     --endpoint-url "$R2_ENDPOINT" \
#     --exclude "*" \
#     --include "*.parquet.gz" \
#     --include "manifest.json" \
#     --no-progress
# =============================================================================
