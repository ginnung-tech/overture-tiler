#!/bin/bash
# Sequentially process themes (smallest -> largest) through the tiling steps.
#
# Three modes, dispatched by env var:
#
#   default (no flag):
#     exec tile_v13_driver.py — the 24/7 driver. Processes one peel at a
#     time eastward from lng=0, uploads each peel to Cloudflare R2, deletes
#     local data, and starts the next cycle. Requires OVERTURE_RELEASE,
#     SENTRY_DSN_OVERTURE, and R2_* env vars.
#
#   RUN_V13_GLOBAL=1:
#     One-shot v13 pipeline (pass1 -> pass1.5 -> pass2 -> pass3). Used for
#     offline backfill / single-release rebuilds. Reads from local
#     raw/<theme>/ — download.py must have run first.
#
#   RUN_LEGACY=1:
#     v11/v12 per-theme pipeline. Backwards-compat only; superseded by v13.
#
# v13 architecture: single fast SSD (USB 4 / TB4, APFS) holds raw + staging +
# tiles. Internal NVMe is reserved for OS + macOS swap.
#
# See infrastructure/scripts/overture-tiler/README.md for the full design.

set -u

WORKDIR="${OVERTURE_WORKDIR:-/Volumes/SSD-2TB/overture}"
export OVERTURE_WORKDIR="$WORKDIR"

THREADS=4
MEMORY=6GB   # per-worker DuckDB budget — matches the v13 24/7 driver

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"

THEMES=(water land segments)   # land_use raw was deleted; redownload separately if needed

PY=python3

# Mode selection.
RUN_LEGACY="${RUN_LEGACY:-0}"           # v11/v12 per-theme
RUN_V13_GLOBAL="${RUN_V13_GLOBAL:-0}"   # v13 one-shot global pass1..pass3

prep_tiles_dir() {
    local theme="$1"
    mkdir -p "$WORKDIR/tiles/$theme"
}

cleanup_theme() {
    local theme="$1"
    local logf="$LOG_DIR/${theme}_cleanup.log"
    {
        echo "=== [$(date -u +%FT%TZ)] cleanup theme=$theme START ==="
        echo "df before:"; df -h "$WORKDIR" / 2>/dev/null

        # 1. Delete raw (it's downloaded; can be re-fetched if needed).
        if [ -e "$WORKDIR/raw/$theme" ]; then
            echo "rm -rf $WORKDIR/raw/$theme  ($(du -sh "$WORKDIR/raw/$theme" 2>/dev/null | cut -f1))"
            rm -rf "$WORKDIR/raw/$theme"
        fi

        # 2. Delete staging (the per-file intermediates + coarse buckets).
        if [ -d "$WORKDIR/.tile-staging/$theme" ]; then
            echo "rm -rf $WORKDIR/.tile-staging/$theme  ($(du -sh "$WORKDIR/.tile-staging/$theme" | cut -f1))"
            rm -rf "$WORKDIR/.tile-staging/$theme"
        fi

        echo "df after:"; df -h "$WORKDIR" / 2>/dev/null
        echo "=== [$(date -u +%FT%TZ)] cleanup theme=$theme DONE ==="
    } >> "$logf" 2>&1
}

run_step() {
    local theme="$1"; local step="$2"; shift 2
    local logf="$LOG_DIR/${theme}_step${step}.log"
    echo "=== [$(date -u +%FT%TZ)] theme=$theme step=$step START ===" | tee -a "$logf"
    echo "+ $*" | tee -a "$logf"
    if "$@" >> "$logf" 2>&1; then
        echo "=== [$(date -u +%FT%TZ)] theme=$theme step=$step OK ===" | tee -a "$logf"
        return 0
    else
        local rc=$?
        echo "!!! [$(date -u +%FT%TZ)] theme=$theme step=$step FAILED rc=$rc ===" | tee -a "$logf"
        return $rc
    fi
}

if [ "$RUN_LEGACY" = "1" ]; then
    echo ">>> [$(date -u +%FT%TZ)] RUN_LEGACY=1 — running v12 per-theme pipeline"
    for theme in "${THEMES[@]}"; do
        echo ">>> [$(date -u +%FT%TZ)] starting theme=$theme"
        prep_tiles_dir "$theme"
        run_step "$theme" 1   "$PY" "$SCRIPT_DIR/tile.py"             --theme "$theme" --threads "$THREADS" --pass1-only       || { echo "abort theme=$theme at step 1"; continue; }
        run_step "$theme" 1_5 "$PY" "$SCRIPT_DIR/tile_v12_pass1_5.py" --theme "$theme" --workers "$THREADS"                    || { echo "abort theme=$theme at step 1.5"; continue; }
        run_step "$theme" 2   "$PY" "$SCRIPT_DIR/tile_v12_pass2.py"   --theme "$theme" --workers "$THREADS" --memory-limit "$MEMORY" || { echo "abort theme=$theme at step 2"; continue; }
        echo "<<< [$(date -u +%FT%TZ)] finished theme=$theme — running cleanup"
        cleanup_theme "$theme"
        echo "<<< [$(date -u +%FT%TZ)] cleanup done for theme=$theme"
    done
    echo "=== [$(date -u +%FT%TZ)] LEGACY RUN DONE ==="
    exit 0
fi

if [ "$RUN_V13_GLOBAL" = "1" ]; then
    # v13 global one-shot: all themes are partitioned + collated + emitted +
    # merged across all themes in one sweep. Reads from <workdir>/raw/<theme>/
    # — download.py must have run first. Used for offline backfill /
    # single-release rebuilds, NOT the 24/7 steady-state path.
    echo ">>> [$(date -u +%FT%TZ)] RUN_V13_GLOBAL=1 — running v13 one-shot pipeline (themes=${THEMES[*]})"

    # Pass 1: per-theme Mercator z=6 partitioning
    run_step "v13" 1   "$PY" "$SCRIPT_DIR/tile_v13_pass1.py"        --workers "$THREADS"                            || { echo "abort at v13 pass1"; exit 1; }
    # Pass 1.5: collate per-bucket across themes
    run_step "v13" 1_5 "$PY" "$SCRIPT_DIR/tile_v13_pass1_5.py"      --workers "$THREADS"                            || { echo "abort at v13 pass1.5"; exit 1; }
    # Pass 2: peel-sharded leaf emit (z=14)
    run_step "v13" 2   "$PY" "$SCRIPT_DIR/tile_v13_pass2.py"        --workers "$THREADS" --memory-limit "$MEMORY"   || { echo "abort at v13 pass2"; exit 1; }
    # Pass 3: bottom-up merge + adaptive z=15
    run_step "v13" 3   "$PY" "$SCRIPT_DIR/tile_v13_pass3_merge.py"  --workers "$THREADS"                            || { echo "abort at v13 pass3"; exit 1; }
    echo "=== [$(date -u +%FT%TZ)] V13 GLOBAL RUN DONE ==="
    exit 0
fi

# Default: hand off to the 24/7 driver.
echo ">>> [$(date -u +%FT%TZ)] starting v13 24/7 driver — see logs/driver.log"
exec "$PY" "$SCRIPT_DIR/tile_v13_driver.py" "$@" 2>&1 | tee -a "$LOG_DIR/driver.log"
