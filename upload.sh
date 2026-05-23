#!/usr/bin/env bash
# =============================================================================
# upload.sh — Upload Overture tile output to Cloudflare R2 public bucket
#
# Tool choice: rclone (preferred over aws s3 sync) because rclone handles
# Content-Encoding headers natively via --header-upload, preserves metadata
# correctly on re-runs, and is idempotent: files are compared by checksum and
# only re-uploaded when changed.  aws s3 sync can be used as a fallback if
# rclone is unavailable (see commented section at the bottom).
#
# Requirements:
#   - rclone installed (brew install rclone on Mac)
#   - R2 credentials in env vars (never hardcoded):
#       R2_ACCESS_KEY_ID
#       R2_SECRET_ACCESS_KEY
#       R2_ACCOUNT_ID
#
# Usage:
#   R2_ACCESS_KEY_ID=xxx R2_SECRET_ACCESS_KEY=yyy R2_ACCOUNT_ID=zzz \
#       bash upload.sh [--workdir /Volumes/SSD/overture]
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BUCKET="overture-tiles"
WORKDIR="${OVERTURE_WORKDIR:-/Volumes/SSD/overture}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workdir)
      WORKDIR="$2"; shift 2 ;;
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
echo ""

# Count files before upload for summary
FILE_COUNT=$(find "$WORKDIR" -name "*.parquet.gz" -o -name "manifest.json" | wc -l | tr -d ' ')
TOTAL_BYTES=$(find "$WORKDIR" \( -name "*.parquet.gz" -o -name "manifest.json" \) -exec stat -f%z {} + 2>/dev/null | awk '{s+=$1} END {print s+0}' || echo 0)
TOTAL_MB=$(echo "scale=1; $TOTAL_BYTES / 1048576" | bc)

rclone copy "$WORKDIR" "r2:${BUCKET}" \
  --config "$RCLONE_CONFIG" \
  --include "*.parquet.gz" \
  --include "manifest.json" \
  --header-upload "Content-Encoding:gzip" \
  --checksum \
  --transfers 8 \
  --s3-chunk-size 64M \
  --progress \
  --stats 10s

# Upload manifest without Content-Encoding:gzip (it is plain JSON, not gzipped)
rclone copy "$WORKDIR/manifest.json" "r2:${BUCKET}" \
  --config "$RCLONE_CONFIG" \
  --checksum \
  --header-upload "Content-Type:application/json" \
  --progress

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
