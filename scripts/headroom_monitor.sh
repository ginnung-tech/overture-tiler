#!/bin/bash
# headroom_monitor.sh — log internal-NVMe free-space every minute.
#
# v13_SPEC.md §4.9: cheap insurance for the next disaster post-mortem.
# The new fast SSD makes ENOSPC on internal much less likely (tiles
# write to external directly), but DuckDB still spills to /tmp on
# internal and macOS swap lives there. If headroom collapses during a
# run, this log answers "exactly when, and at what point in the
# pipeline".
#
# Usage:
#   nohup ./headroom_monitor.sh > logs/headroom.log 2>&1 &
#   # or:  ./headroom_monitor.sh --interval 60 --log-dir logs/
#
# Stop cleanly with SIGTERM/SIGINT; no flush is needed (df output is
# line-buffered through tee).

set -u

INTERVAL=60
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"

while [ $# -gt 0 ]; do
    case "$1" in
        --interval) INTERVAL="$2"; shift 2 ;;
        --log-dir)  LOG_DIR="$2"; shift 2 ;;
        --help|-h)
            echo "Usage: $0 [--interval N_SECONDS] [--log-dir DIR]"
            echo "  Logs df -h / every interval seconds to <log-dir>/headroom.log"
            exit 0 ;;
        *)
            echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/headroom.log"

shutdown=0
trap 'shutdown=1' TERM INT

echo "[$(date -u +%FT%TZ)] headroom monitor starting (interval=${INTERVAL}s, log=$LOG_FILE)"

while [ "$shutdown" -eq 0 ]; do
    {
        echo "=== $(date -u +%FT%TZ) ==="
        df -h /
    } >> "$LOG_FILE" 2>&1
    # Sleep in slices so SIGTERM is responsive.
    slept=0
    while [ "$slept" -lt "$INTERVAL" ] && [ "$shutdown" -eq 0 ]; do
        sleep 5
        slept=$((slept + 5))
    done
done

echo "[$(date -u +%FT%TZ)] headroom monitor exiting cleanly"
