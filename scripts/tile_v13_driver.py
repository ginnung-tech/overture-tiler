#!/usr/bin/env python3
"""tile_v13_driver.py — 24/7 Overture tiler driver for the Mac mini.

Processes one peel at a time in eastward order starting at lng=0°,
streaming each peel's slice of Overture data from S3, tiling it,
uploading to Cloudflare R2, and deleting the local data before moving
on. After all 36 peels of a cycle finish, the cycle counter advances
and the next cycle starts at lng=0° again — running 24/7.

Two stages run concurrently, one peel apart:

  Stage A (prefetch, I/O bound):
      pass1_per_peel.run_one_peel(N+1)
        → s3://overture/... → staging/peel_<N+1>/combined/z6_*.parquet

  Stage B (tile + upload, CPU bound):
      pass2.run_one_peel(N)
        → tiles/peel_<N>/{z}/{x}/{y}.parquet
      pass3_local.run_one_peel(N)
        → manifest mutated in place; sparse regions merged up to z=6
      bash upload.sh --peel-idx N
        → uploads tiles/peel_<N>/ to r2:overture-tiles/tiles/ (prefix
          stripped) + driver-state/tiles_index.json with no-cache headers
      tile_v13_index.update_global_index(N)
        → driver-state/tiles_index.json rewritten on disk
      LOCAL CLEANUP: rm -rf staging/peel_<N>, tiles/peel_<N>, duckdb-tmp/peel_<N>

When both stages finish, the prefetched peel becomes the next active
peel and the cycle advances.

Run
---

    # First-time setup
    export OVERTURE_WORKDIR=/Volumes/SSD-2TB/overture
    export SENTRY_DSN_OVERTURE=<dsn>
    export OVERTURE_RELEASE=2026-04-15.0   # or set per-cycle from S3 listing
    export R2_ACCESS_KEY_ID=... R2_SECRET_ACCESS_KEY=... R2_ACCOUNT_ID=...

    # Run forever
    python3 tile_v13_driver.py

    # Run N peels then stop (smoke test)
    python3 tile_v13_driver.py --max-peels 1

    # Start at a specific peel
    python3 tile_v13_driver.py --max-peels 1 --start-peel-idx 18 --no-prefetch
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import tile_v13_pass1_per_peel as pass1_per_peel  # noqa: E402
import tile_v13_pass2 as pass2  # noqa: E402
import tile_v13_pass3_global_finalize as finalize  # noqa: E402
import tile_v13_pass3_local as pass3_local  # noqa: E402
from tile_v13_helpers import (  # noqa: E402
    DEFAULT_MEMORY_LIMIT,
    PEEL_WIDTH_DEG_DEFAULT,
    TILE_BUDGET_DEFAULT,
    Z_MAX_DEFAULT,
    driver_state_dir,
    driver_state_path,
    duckdb_tmp_dir_peel,
    eastward_peel_order_from_zero,
    global_index_path,
    peel_dir_name,
    peel_lng_range,
    peel_manifest_path,
    resolve_workdir,
    staging_peel_dir,
    tiles_peel_root,
)
from tile_v13_index import update_global_index  # noqa: E402
from tile_v13_sentry import init_sentry, log_event, peel_span, phase_span  # noqa: E402


# How often the host-pressure breadcrumb fires.
PRESSURE_SAMPLE_SEC = 60

# Self-throttle thresholds.
PAUSE_PREFETCH_FREE_GB_BELOW = 100
PAUSE_PREFETCH_SWAP_GB_ABOVE = 24


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# Driver state persistence
# ---------------------------------------------------------------------------

def _load_state(workdir: Path) -> dict:
    p = driver_state_path(workdir)
    if not p.exists():
        return {
            "cycle": 0,
            "completed_peels_in_cycle": [],
            "last_completed_at": None,
            "started_at": _iso_now(),
            "release": None,
        }
    return json.loads(p.read_text(encoding="utf-8"))


def _save_state(workdir: Path, state: dict) -> None:
    p = driver_state_path(workdir)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    os.replace(tmp, p)


# ---------------------------------------------------------------------------
# Resource pressure monitoring (macOS-first, degrades gracefully)
# ---------------------------------------------------------------------------

def _host_pressure_snapshot(workdir: Path) -> dict:
    """Return {swap_used_mb, workdir_free_gb, duckdb_tmp_bytes, ...}."""
    out: dict = {"sampled_at": _iso_now()}

    # df: workdir free space (cross-platform)
    try:
        usage = shutil.disk_usage(workdir)
        out["workdir_free_gb"] = round(usage.free / (1024**3), 2)
        out["workdir_used_gb"] = round((usage.total - usage.free) / (1024**3), 2)
    except Exception as e:
        out["workdir_df_error"] = repr(e)

    # vm_stat: macOS only. Capture page-out + swap pressure.
    if sys.platform == "darwin":
        try:
            res = subprocess.run(
                ["vm_stat"], capture_output=True, text=True, timeout=5, check=True,
            )
            for line in res.stdout.splitlines():
                if "swapouts" in line or "Pageouts" in line:
                    out["vm_stat_line"] = line.strip()
                    break
        except Exception as e:
            out["vm_stat_error"] = repr(e)

    # Swap totals via sysctl (macOS).
    if sys.platform == "darwin":
        try:
            res = subprocess.run(
                ["sysctl", "-n", "vm.swapusage"],
                capture_output=True, text=True, timeout=5, check=True,
            )
            # Format: "total = 4096.00M  used = 1234.56M  free = 2861.44M (encrypted)"
            line = res.stdout.strip()
            out["vm_swapusage"] = line
            for token in line.split():
                if token.endswith("M") and "=" not in token:
                    pass  # consumed by the parse below
            tokens = line.replace("=", " ").split()
            for i, tok in enumerate(tokens):
                if tok == "total" and i + 1 < len(tokens):
                    out["swap_total_mb"] = float(tokens[i + 1].rstrip("M"))
                if tok == "used" and i + 1 < len(tokens):
                    out["swap_used_mb"] = float(tokens[i + 1].rstrip("M"))
        except Exception as e:
            out["swap_error"] = repr(e)

    # DuckDB temp directory size.
    duckdb_tmp_root = workdir / "duckdb-tmp"
    if duckdb_tmp_root.exists():
        try:
            total = 0
            for root, _dirs, files in os.walk(duckdb_tmp_root):
                for f in files:
                    try:
                        total += (Path(root) / f).stat().st_size
                    except OSError:
                        pass
            out["duckdb_tmp_bytes"] = total
        except Exception as e:
            out["duckdb_tmp_error"] = repr(e)

    return out


_pressure_stop = threading.Event()
_last_pressure: dict = {}


def _pressure_loop(workdir: Path, cycle_getter) -> None:
    """Periodically emit `tiler.host_pressure`. Runs in a daemon thread."""
    while not _pressure_stop.is_set():
        snap = _host_pressure_snapshot(workdir)
        snap["cycle"] = cycle_getter()
        global _last_pressure
        _last_pressure = snap
        log_event("tiler.host_pressure", component="driver", **snap)
        _pressure_stop.wait(PRESSURE_SAMPLE_SEC)


def _should_pause_prefetch() -> tuple[bool, str | None]:
    p = _last_pressure
    if not p:
        return False, None
    if p.get("workdir_free_gb", float("inf")) < PAUSE_PREFETCH_FREE_GB_BELOW:
        return True, f"workdir_free_gb={p['workdir_free_gb']} < {PAUSE_PREFETCH_FREE_GB_BELOW}"
    swap_used = p.get("swap_used_mb", 0)
    if swap_used / 1024.0 > PAUSE_PREFETCH_SWAP_GB_ABOVE:
        return True, f"swap_used_mb={swap_used} > {PAUSE_PREFETCH_SWAP_GB_ABOVE * 1024}"
    return False, None


# ---------------------------------------------------------------------------
# Stage A — prefetch (S3 read + combined buckets)
# ---------------------------------------------------------------------------

def stage_a(workdir: Path, peel_idx: int, release: str, cycle: int) -> dict:
    with phase_span("pass1_per_peel", peel_idx=peel_idx, cycle=cycle) as ctx:
        result = pass1_per_peel.run_one_peel(
            workdir=workdir,
            peel_idx=peel_idx,
            release=release,
            cycle=cycle,
        )
        ctx.update(result)
        return result


# ---------------------------------------------------------------------------
# Stage B — tile + upload + index + cleanup
# ---------------------------------------------------------------------------

def _run_upload(workdir: Path, peel_idx: int, cycle: int) -> dict:
    """Invoke upload.sh --peel-idx N as a subprocess."""
    script = Path(__file__).resolve().parent / "upload.sh"
    if not script.exists():
        raise FileNotFoundError(f"upload.sh not found at {script}")
    cmd = ["bash", str(script), "--peel-idx", str(peel_idx), "--workdir", str(workdir)]
    t0 = time.time()
    res = subprocess.run(cmd, capture_output=True, text=True)
    duration = round(time.time() - t0, 2)
    if res.returncode != 0:
        log_event(
            "tiler.upload_failed",
            level="error",
            component="upload",
            cycle=cycle,
            **{"peel.idx": peel_idx},
            returncode=res.returncode,
            stderr_tail=res.stderr[-2000:] if res.stderr else "",
        )
        raise RuntimeError(f"upload.sh failed (rc={res.returncode}) for peel {peel_idx}")
    return {"duration_sec": duration, "stdout_tail": res.stdout[-500:]}


def _read_peel_manifest(workdir: Path, peel_idx: int) -> list[dict]:
    p = peel_manifest_path(workdir, peel_idx)
    if not p.exists():
        return []
    return json.loads(p.read_text(encoding="utf-8"))


def _cleanup_peel(workdir: Path, peel_idx: int) -> dict:
    """rm -rf the peel's staging, tiles, and duckdb-tmp directories."""
    counters = {"bytes_freed": 0, "dirs_removed": []}
    for d in (
        staging_peel_dir(workdir, peel_idx),
        tiles_peel_root(workdir, peel_idx),
        duckdb_tmp_dir_peel(workdir, peel_idx),
    ):
        if not d.exists():
            continue
        # Best-effort byte count before deletion.
        size = 0
        try:
            for root, _dirs, files in os.walk(d):
                for f in files:
                    try:
                        size += (Path(root) / f).stat().st_size
                    except OSError:
                        pass
        except Exception:
            pass
        counters["bytes_freed"] += size
        try:
            shutil.rmtree(d)
            counters["dirs_removed"].append(str(d))
        except Exception as e:
            log_event(
                "tiler.cleanup_failed",
                level="error",
                component="driver",
                **{"peel.idx": peel_idx},
                path=str(d), error=repr(e),
            )
            raise
    return counters


def _persist_peel_manifest_to_state(workdir: Path, peel_idx: int) -> None:
    """Copy `tiles/peel_<idx>/_manifest.json` into `driver-state/per_peel_manifests/`
    so the global finalize step can read it after local cleanup."""
    src = peel_manifest_path(workdir, peel_idx)
    if not src.exists():
        return
    dst_dir = driver_state_dir(workdir) / "per_peel_manifests"
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / f"{peel_dir_name(peel_idx)}.json"
    shutil.copy2(src, dst)


def stage_b(
    workdir: Path,
    peel_idx: int,
    cycle: int,
    z_max: int,
    tile_budget: int,
    workers: int,
    memory_limit: str,
) -> dict:
    counters: dict = {}

    # pass2 — leaves
    with phase_span("pass2", peel_idx=peel_idx, cycle=cycle) as p:
        r = pass2.run_one_peel(
            workdir=workdir, peel_idx=peel_idx,
            z_max=z_max, tile_budget=tile_budget,
            workers=workers, memory_limit=memory_limit,
        )
        p.update(r); counters["pass2"] = r

    # pass3 local — Phase A + B (within-peel)
    with phase_span("pass3_local", peel_idx=peel_idx, cycle=cycle) as p:
        r = pass3_local.run_one_peel(
            workdir=workdir, peel_idx=peel_idx,
            z_max=z_max, tile_budget=tile_budget,
            workers=workers, memory_limit=memory_limit,
            cycle=cycle,
        )
        p.update(r); counters["pass3_local"] = r

    # Index update — must happen BEFORE upload so the on-disk index reflects
    # this peel's tiles when upload.sh re-uploads it with no-cache headers.
    with phase_span("index_update", peel_idx=peel_idx, cycle=cycle) as p:
        entries = _read_peel_manifest(workdir, peel_idx)
        out_path = update_global_index(workdir, peel_idx, cycle, entries)
        p["new_peel_entries"] = len(entries)
        p["index_path"] = str(out_path)
        counters["index"] = {"new_entries": len(entries)}

    # Stash a copy of the per-peel manifest for the eventual cycle-end finalizer.
    _persist_peel_manifest_to_state(workdir, peel_idx)

    # Upload
    with phase_span("upload", peel_idx=peel_idx, cycle=cycle) as p:
        r = _run_upload(workdir, peel_idx, cycle)
        p.update(r); counters["upload"] = r

    # Local cleanup — gates the next peel's prefetch from filling the disk.
    with phase_span("cleanup", peel_idx=peel_idx, cycle=cycle) as p:
        r = _cleanup_peel(workdir, peel_idx)
        p.update(r); counters["cleanup"] = r

    return counters


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def _resolve_release() -> str:
    rel = os.environ.get("OVERTURE_RELEASE")
    if not rel:
        raise RuntimeError(
            "OVERTURE_RELEASE not set. Pin a specific Overture release (e.g. "
            "'2026-04-15.0') in the env. Auto-discovery from S3 is a future "
            "enhancement; for now the operator pins per cycle."
        )
    return rel


def _setup_signal_handlers(stop_flag: threading.Event) -> None:
    def handler(signum, _frame):
        log_event("tiler.shutdown_requested", component="driver", signum=int(signum))
        stop_flag.set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, handler)
        except Exception:
            pass  # not all signals available on all platforms (Windows etc.)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--workdir", default=None)
    p.add_argument("--max-peels", type=int, default=None,
                   help="Stop after N peels (smoke tests). Default: run forever.")
    p.add_argument("--start-peel-idx", type=int, default=None,
                   help="Override starting peel_idx. Default: eastward-from-0 order, "
                        "resuming from driver_state.json.")
    p.add_argument("--no-prefetch", action="store_true",
                   help="Disable Stage A overlap (single-stream mode for smoke tests).")
    p.add_argument("--force-rerun", action="store_true",
                   help="Re-process peels even if checkpoint says they're done.")
    p.add_argument("--workers", type=int, default=4,
                   help="Within-peel bucket workers for pass2/pass3-local (default 4).")
    p.add_argument("--memory-limit", default=DEFAULT_MEMORY_LIMIT)
    p.add_argument("--z-max", type=int, default=Z_MAX_DEFAULT)
    p.add_argument("--tile-budget-bytes", type=int, default=TILE_BUDGET_DEFAULT)
    p.add_argument("--peel-width-deg", type=int, default=PEEL_WIDTH_DEG_DEFAULT)
    args = p.parse_args()

    workdir = resolve_workdir(args.workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    driver_state_dir(workdir).mkdir(parents=True, exist_ok=True)

    init_sentry("driver")
    log_event(
        "tiler.startup",
        component="driver",
        git_sha=os.environ.get("OVERTURE_RELEASE_TAG", ""),
        python_version=sys.version.split()[0],
        workdir=str(workdir),
    )

    state = _load_state(workdir)
    cycle = int(state.get("cycle", 0))

    # Resolve release. For first cycle, take from env. Persist into state.
    release = state.get("release") or _resolve_release()
    state["release"] = release
    _save_state(workdir, state)

    # Peel order: eastward from lng=0°, deduplicated against the cycle's
    # already-completed peels.
    full_order = eastward_peel_order_from_zero(args.peel_width_deg)
    completed = set(state.get("completed_peels_in_cycle", []))
    if args.start_peel_idx is not None:
        # CLI override starts a fresh sequence from this peel.
        idx_start = full_order.index(args.start_peel_idx)
        peel_order = full_order[idx_start:]
        completed = set()
    elif args.force_rerun:
        peel_order = full_order
        completed = set()
    else:
        peel_order = [p_ for p_ in full_order if p_ not in completed]
    if args.max_peels is not None:
        peel_order = peel_order[: args.max_peels]

    log_event(
        "tiler.cycle_start",
        component="driver",
        cycle=cycle, release=release,
        peels_planned=len(peel_order),
        completed_already=len(completed),
    )

    # Pressure-monitoring thread
    stop_flag = threading.Event()
    _setup_signal_handlers(stop_flag)
    pressure_t = threading.Thread(
        target=_pressure_loop,
        args=(workdir, lambda: cycle),
        daemon=True,
    )
    pressure_t.start()

    # Two-worker pool: one for Stage A, one for Stage B
    pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="tiler")

    # Prime stage A for the first peel (unless --no-prefetch).
    a_future = None
    if peel_order and not args.no_prefetch:
        first = peel_order[0]
        a_future = pool.submit(stage_a, workdir, first, release, cycle)

    try:
        for i, peel_idx in enumerate(peel_order):
            if stop_flag.is_set():
                log_event("tiler.shutdown_acknowledged", component="driver",
                          processed=i, remaining=len(peel_order) - i)
                break

            lng_lo, lng_hi = peel_lng_range(peel_idx, args.peel_width_deg)

            with peel_span(peel_idx=peel_idx, lng_lo=lng_lo, lng_hi=lng_hi, cycle=cycle) as pctx:
                # Resolve Stage A for THIS peel — either the prefetched future
                # or run synchronously when prefetch is disabled.
                if args.no_prefetch or a_future is None:
                    stage_a_result = stage_a(workdir, peel_idx, release, cycle)
                else:
                    stage_a_result = a_future.result()
                    a_future = None
                pctx["pass1_buckets"] = stage_a_result.get("combined_buckets", 0)

                # Kick off Stage A for the NEXT peel before starting Stage B.
                # Throttled by host-pressure observer.
                if (
                    not args.no_prefetch
                    and i + 1 < len(peel_order)
                ):
                    next_idx = peel_order[i + 1]
                    pause, reason = _should_pause_prefetch()
                    if pause:
                        log_event(
                            "tiler.prefetch_paused",
                            level="warning",
                            component="driver",
                            cycle=cycle,
                            **{"peel.idx": peel_idx},
                            next_peel_idx=next_idx,
                            reason=reason,
                        )
                        a_future = None  # Stage A runs synchronously next iteration
                    else:
                        a_future = pool.submit(stage_a, workdir, next_idx, release, cycle)

                # Stage B for the current peel.
                stage_b_result = stage_b(
                    workdir, peel_idx, cycle,
                    z_max=args.z_max,
                    tile_budget=args.tile_budget_bytes,
                    workers=args.workers,
                    memory_limit=args.memory_limit,
                )
                pctx["final_tile_count"] = stage_b_result.get("pass3_local", {}).get(
                    "final_tile_count", 0
                )
                pctx["bytes_freed"] = stage_b_result.get("cleanup", {}).get("bytes_freed", 0)

            # Persist progress.
            state["completed_peels_in_cycle"] = sorted(
                set(state.get("completed_peels_in_cycle", [])) | {peel_idx}
            )
            state["last_completed_at"] = _iso_now()
            _save_state(workdir, state)

        # Cycle done? If we processed everything in the planned order, advance.
        if not stop_flag.is_set() and len(state["completed_peels_in_cycle"]) >= len(full_order):
            finalize_result = finalize.run(workdir, cycle=cycle)
            log_event(
                "tiler.cycle_done",
                component="driver",
                cycle=cycle,
                completed_at=_iso_now(),
                finalize_stub=finalize_result.get("stub", False),
            )
            cycle += 1
            state["cycle"] = cycle
            state["completed_peels_in_cycle"] = []
            _save_state(workdir, state)
    finally:
        _pressure_stop.set()
        pool.shutdown(wait=False, cancel_futures=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
