#!/usr/bin/env python3
"""tile_v13_pass2.py — peel-sharded leaf emit at z=z_max.

Stage 2b / pass 2 of the v13 pipeline (see `v13_SPEC.md`).

Reads pass1.5's combined-theme buckets at:

    <workdir>/.tile-staging/v13/combined/z6_{x}_{y}.parquet

For every 10° lng peel (36 peels at PEEL_WIDTH_DEG=10), find all z=6
buckets whose Mercator extent intersects the peel and process them
sequentially per worker. Workers run different peels in parallel.

For each bucket: load it once into TEMP TABLE _bucket. For each z=14
leaf inside the bucket extent, COPY filtered rows out to:

    <workdir>/tiles/{z}/{x}/{y}.parquet

Empty leaves: don't write a file; record in checkpoint as `empty`.
Oversize leaves (>20 MB): mark for adaptive z=15 subdivision in pass3.

Per-bucket lock-batching pattern reused from v12 pass2: collect updates
locally, flush them under one lock per bucket. Replaces the ~1.5M
per-tile lock acquisitions of the v11 path.

Manifest record per non-empty leaf includes a per-theme breakdown so
later consumers can ship a flat manifest with theme counts without
re-opening parquet.

Run after pass1.5 has emitted its `_DONE` marker.

Run
---

    python tile_v13_pass2.py [--workdir /Volumes/SSD/overture] [--workers N]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _sql_safety import q_float, q_path  # noqa: E402
from tile_v13_helpers import (  # noqa: E402
    DEFAULT_INTERNAL_THREADS,
    DEFAULT_MEMORY_LIMIT,
    LAT_BOUNDS,
    LNG_BOUNDS,
    PEEL_WIDTH_DEG_DEFAULT,
    THEMES,
    TILE_BUDGET_DEFAULT,
    Z_BUCKET,
    Z_MAX_DEFAULT,
    Bbox,
    combined_bucket_dir,
    combined_bucket_dir_peel,
    combined_bucket_path,
    combined_bucket_path_peel,
    duckdb_tmp_dir_peel,
    lng_range_to_z6_keys,
    new_con,
    peel_dir_name,
    peel_lng_range,
    peel_manifest_path,
    quadkey_extent,
    resolve_workdir,
    tile_path,
    tile_path_peel,
    tiles_peel_root,
)

PASS1_5_DONE_MARKER = "_DONE"

CHECKPOINT_FILENAME = "_v13_pass2_checkpoint.json"
MANIFEST_FILENAME = "_v13_pass2_manifest.json"
CHECKPOINT_SAVE_INTERVAL = 50  # save every N completed buckets
PROGRESS_INTERVAL_SEC = 30


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# Checkpoint / manifest
# ---------------------------------------------------------------------------

def _checkpoint_path(workdir: Path) -> Path:
    return workdir / "tiles" / CHECKPOINT_FILENAME


def _manifest_path(workdir: Path) -> Path:
    return workdir / "tiles" / MANIFEST_FILENAME


def _load_checkpoint(workdir: Path) -> dict[str, dict]:
    """{ "z:x:y": {status, size_bytes, feature_count, theme_counts, oversize, at} }."""
    p = _checkpoint_path(workdir)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return {f"{e['z']}:{e['x']}:{e['y']}": e for e in data}
    except Exception as e:
        print(f"[warn] checkpoint read error: {e} — starting fresh", flush=True)
        return {}


def _save_checkpoint(workdir: Path, entries: dict[str, dict]) -> None:
    p = _checkpoint_path(workdir)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(list(entries.values()), indent=2), encoding="utf-8")
    tmp.replace(p)


def _save_manifest(workdir: Path, entries: dict[str, dict]) -> None:
    """Manifest: subset of checkpoint with status='done'."""
    p = _manifest_path(workdir)
    p.parent.mkdir(parents=True, exist_ok=True)
    rows = [e for e in entries.values() if e.get("status") == "done"]
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    tmp.replace(p)


# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------

def _peel_lng_range(peel_idx: int, peel_width_deg: int) -> tuple[float, float]:
    lo = LNG_BOUNDS[0] + peel_idx * peel_width_deg
    hi = lo + peel_width_deg
    return lo, hi


def _z6_to_zmax_leaves(x6: int, y6: int, z_max: int) -> list[tuple[int, int]]:
    """Enumerate all (x, y) leaves at z_max inside the z=6 tile (x6, y6).

    A z=6 tile contains 4^(z_max - 6) leaves. At z_max=14 that's 4^8 = 65536
    leaves per bucket — fine to enumerate as Python ints.
    """
    if z_max < Z_BUCKET:
        raise ValueError(f"z_max ({z_max}) must be >= Z_BUCKET ({Z_BUCKET})")
    delta = z_max - Z_BUCKET
    span = 1 << delta
    x_lo = x6 << delta
    y_lo = y6 << delta
    return [
        (x_lo + dx, y_lo + dy)
        for dx in range(span)
        for dy in range(span)
    ]


# ---------------------------------------------------------------------------
# Per-bucket worker
# ---------------------------------------------------------------------------

def _resolve_bucket_path(workdir: Path, peel_idx: int | None, x6: int, y6: int) -> Path:
    """Pick legacy vs per-peel combined-bucket source for `_process_one_bucket`."""
    if peel_idx is None:
        return combined_bucket_path(workdir, x6, y6)
    return combined_bucket_path_peel(workdir, peel_idx, x6, y6)


def _resolve_tile_path(workdir: Path, peel_idx: int | None, z: int, x: int, y: int) -> Path:
    """Pick legacy vs per-peel tile output path."""
    if peel_idx is None:
        return tile_path(workdir, z, x, y)
    return tile_path_peel(workdir, peel_idx, z, x, y)


def _process_one_bucket(
    workdir: Path,
    x6: int,
    y6: int,
    z_max: int,
    tile_budget: int,
    memory_limit: str,
    internal_threads: int,
    checkpoint: dict[str, dict],
    checkpoint_lock: threading.Lock,
    no_skip_existing: bool,
    peel_idx: int | None = None,
    temp_dir: Path | None = None,
) -> tuple[int, int, int, int]:
    """Process all z_max leaves inside z=6 bucket (x6, y6).

    `peel_idx`: when set, source bucket + output tile paths are scoped under
    `staging/peel_<idx>/combined/` and `tiles/peel_<idx>/{z}/{x}/{y}.parquet`
    respectively (the 24/7 driver's per-peel mode). When None, the legacy
    global paths are used (`combined/` and `tiles/{z}/{x}/{y}.parquet`).

    `temp_dir`: optional DuckDB `temp_directory` override — the driver passes
    `duckdb_tmp_dir_peel(workdir, peel_idx)` so query spill lives alongside
    the rest of the peel's data and is wiped on per-peel cleanup.

    Returns (n_written, n_empty, n_oversize, n_failed). Oversize counts
    leaves above tile_budget; pass3 will subdivide those to z=z_max+1.
    """
    bucket_path = _resolve_bucket_path(workdir, peel_idx, x6, y6)
    if not bucket_path.exists():
        return 0, 0, 0, 0

    leaves = _z6_to_zmax_leaves(x6, y6, z_max)

    # Filter via checkpoint + filesystem skip BEFORE opening DuckDB.
    todo: list[tuple[int, int]] = []
    with checkpoint_lock:
        for (lx, ly) in leaves:
            key = f"{z_max}:{lx}:{ly}"
            entry = checkpoint.get(key)
            if entry and entry.get("status") in ("done", "empty"):
                continue
            if not no_skip_existing:
                # Stage 3 §4.4 idempotency: skip if the file already exists,
                # even with no checkpoint entry.
                if _resolve_tile_path(workdir, peel_idx, z_max, lx, ly).exists():
                    # Don't fabricate a checkpoint entry here; pass3-merge
                    # will pick up the loose tile via filesystem walk.
                    continue
            todo.append((lx, ly))
    if not todo:
        return 0, 0, 0, 0

    con = new_con(
        internal_threads=internal_threads,
        memory_limit=memory_limit,
        temp_dir=temp_dir,
    )
    n_written = 0
    n_empty = 0
    n_oversize = 0
    n_failed = 0
    local_updates: dict[str, dict] = {}

    try:
        # Load the combined bucket once.
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _bucket AS
            SELECT * FROM read_parquet('{q_path(bucket_path)}')
        """)

        for (lx, ly) in todo:
            ext = quadkey_extent(z_max, lx, ly)
            out_path = _resolve_tile_path(workdir, peel_idx, z_max, lx, ly)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = out_path.with_suffix(".parquet.tmp")

            # Filter rows whose bbox overlaps the leaf extent. The combined
            # bucket carries the original Overture `bbox` STRUCT
            # (xmin/ymin/xmax/ymax in WGS84 degrees).
            try:
                con.execute(f"""
                    COPY (
                        SELECT * FROM _bucket
                        WHERE bbox.xmin < {q_float(ext.max_lng)}
                          AND bbox.xmax > {q_float(ext.min_lng)}
                          AND bbox.ymin < {q_float(ext.max_lat)}
                          AND bbox.ymax > {q_float(ext.min_lat)}
                    ) TO '{q_path(tmp)}' (
                        FORMAT 'parquet',
                        COMPRESSION 'zstd',
                        COMPRESSION_LEVEL 3
                    )
                """)
            except Exception as e:
                print(
                    f"  FAIL leaf z={z_max} x={lx} y={ly}: {e}",
                    flush=True,
                )
                n_failed += 1
                local_updates[f"{z_max}:{lx}:{ly}"] = {
                    "z": z_max, "x": lx, "y": ly,
                    "status": "failed",
                    "size_bytes": 0,
                    "feature_count": 0,
                    "theme_counts": {},
                    "oversize": False,
                    "bbox": ext.as_list(),
                    "at": _iso_now(),
                }
                if tmp.exists():
                    try:
                        tmp.unlink()
                    except Exception:
                        pass
                continue

            if not tmp.exists():
                # Empty leaf — no file written.
                n_empty += 1
                local_updates[f"{z_max}:{lx}:{ly}"] = {
                    "z": z_max, "x": lx, "y": ly,
                    "status": "empty",
                    "size_bytes": 0,
                    "feature_count": 0,
                    "theme_counts": {},
                    "oversize": False,
                    "bbox": ext.as_list(),
                    "at": _iso_now(),
                }
                continue

            # Count rows + per-theme breakdown.
            cnt_row = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{q_path(tmp)}')"
            ).fetchone()
            feature_count = int(cnt_row[0]) if cnt_row else 0
            if feature_count == 0:
                tmp.unlink()
                n_empty += 1
                local_updates[f"{z_max}:{lx}:{ly}"] = {
                    "z": z_max, "x": lx, "y": ly,
                    "status": "empty",
                    "size_bytes": 0,
                    "feature_count": 0,
                    "theme_counts": {},
                    "oversize": False,
                    "bbox": ext.as_list(),
                    "at": _iso_now(),
                }
                continue

            theme_counts: dict[str, int] = {}
            try:
                rows = con.execute(
                    f"""
                        SELECT theme, COUNT(*)
                        FROM read_parquet('{q_path(tmp)}')
                        GROUP BY theme
                    """
                ).fetchall()
                for theme_name, n in rows:
                    theme_counts[str(theme_name)] = int(n)
            except Exception:
                # If 'theme' column ever goes missing, fall back to total.
                theme_counts = {"unknown": feature_count}

            size_bytes = tmp.stat().st_size
            tmp.replace(out_path)

            oversize = size_bytes > tile_budget
            if oversize:
                n_oversize += 1
            n_written += 1

            local_updates[f"{z_max}:{lx}:{ly}"] = {
                "z": z_max, "x": lx, "y": ly,
                "status": "done",
                "size_bytes": size_bytes,
                "feature_count": feature_count,
                "theme_counts": theme_counts,
                "oversize": oversize,
                "bbox": ext.as_list(),
                "at": _iso_now(),
            }
    finally:
        try:
            con.execute("DROP TABLE IF EXISTS _bucket")
            con.close()
        except Exception:
            pass

    # SINGLE per-bucket lock acquisition. Replaces the per-leaf locks of v11
    # — that contention dominated v12-pass2 wall time on dense regions.
    with checkpoint_lock:
        checkpoint.update(local_updates)

    return n_written, n_empty, n_oversize, n_failed


# ---------------------------------------------------------------------------
# Peel scheduling
# ---------------------------------------------------------------------------

def _buckets_for_peel(
    peel_idx: int,
    peel_width_deg: int,
    bucket_filter: set[tuple[int, int]] | None,
) -> list[tuple[int, int]]:
    lng_lo, lng_hi = _peel_lng_range(peel_idx, peel_width_deg)
    keys = lng_range_to_z6_keys(lng_lo, lng_hi, LAT_BOUNDS)
    if bucket_filter is not None:
        keys = [k for k in keys if k in bucket_filter]
    return keys


def _existing_combined_buckets(workdir: Path) -> set[tuple[int, int]]:
    """Walk the (global) combined dir and return (x, y) for every z6_X_Y.parquet."""
    return _existing_combined_buckets_in(combined_bucket_dir(workdir))


def _existing_combined_buckets_peel(workdir: Path, peel_idx: int) -> set[tuple[int, int]]:
    """Same as :func:`_existing_combined_buckets` for one peel's staging dir."""
    return _existing_combined_buckets_in(combined_bucket_dir_peel(workdir, peel_idx))


def _existing_combined_buckets_in(root: Path) -> set[tuple[int, int]]:
    """Common impl: walk `root` for `z6_<x>_<y>.parquet` files."""
    out: set[tuple[int, int]] = set()
    if not root.exists():
        return out
    for p in root.iterdir():
        if not p.is_file() or not p.name.startswith("z6_") or p.suffix != ".parquet":
            continue
        try:
            _z, x_s, y_s = p.stem.split("_")  # z6_<x>_<y>
            out.add((int(x_s), int(y_s)))
        except Exception:
            continue
    return out


# ---------------------------------------------------------------------------
# Per-peel single-shot entrypoint (24/7 driver)
# ---------------------------------------------------------------------------

def _save_peel_manifest(workdir: Path, peel_idx: int, entries: list[dict]) -> Path:
    """Write `tiles/peel_<idx>/_manifest.json` — pass3-local + driver consume this.

    Sorted by (z, x, y) for deterministic diffs. Only `status=='done'` rows
    are included (empty / failed rows stay in the in-memory checkpoint only).
    """
    p = peel_manifest_path(workdir, peel_idx)
    p.parent.mkdir(parents=True, exist_ok=True)
    rows = sorted(
        (e for e in entries if e.get("status") == "done"),
        key=lambda e: (e["z"], e["x"], e["y"]),
    )
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    tmp.replace(p)
    return p


def run_one_peel(
    workdir: Path,
    peel_idx: int,
    z_max: int = Z_MAX_DEFAULT,
    tile_budget: int = TILE_BUDGET_DEFAULT,
    workers: int = 4,
    memory_limit: str = DEFAULT_MEMORY_LIMIT,
    internal_threads: int = DEFAULT_INTERNAL_THREADS,
    peel_width_deg: int = PEEL_WIDTH_DEG_DEFAULT,
    no_skip_existing: bool = False,
) -> dict:
    """Top-level callable for the 24/7 driver's Stage B (pass2 portion).

    Reads `staging/peel_<idx>/combined/z6_*.parquet`, emits leaves to
    `tiles/peel_<idx>/{z_max}/{x}/{y}.parquet`, parallelises across z=6
    buckets inside the peel (default `workers=4` = perf cores).

    Returns a counters dict: `{tiles_written, tiles_empty, tiles_oversize,
    tiles_failed, buckets_processed, manifest_path, duration_sec}`.
    """
    t0 = time.time()

    bucket_set = _existing_combined_buckets_peel(workdir, peel_idx)
    if not bucket_set:
        raise FileNotFoundError(
            f"no combined buckets at {combined_bucket_dir_peel(workdir, peel_idx)} "
            f"(did pass1-per-peel finish for peel {peel_idx}?)"
        )

    # Buckets that intersect this peel's lng range. With per-peel staging
    # the set should already be a subset, but we still intersect to be
    # defensive against stale/partial staging dirs.
    lng_lo, lng_hi = peel_lng_range(peel_idx, peel_width_deg)
    candidate_keys = lng_range_to_z6_keys(lng_lo, lng_hi, LAT_BOUNDS)
    bucks = [k for k in candidate_keys if k in bucket_set]

    temp_dir = duckdb_tmp_dir_peel(workdir, peel_idx)
    temp_dir.mkdir(parents=True, exist_ok=True)

    # In-memory checkpoint scoped to this peel. The driver persists peel
    # progress via driver_state.json + the per-peel manifest below; the
    # `_v13_pass2_checkpoint.json` shape is reserved for the legacy global
    # mode, so we don't touch it here.
    checkpoint: dict[str, dict] = {}
    checkpoint_lock = threading.Lock()

    n_written = 0
    n_empty = 0
    n_oversize = 0
    n_failed = 0

    def _run_bucket(args: tuple[int, int]) -> tuple[int, int, int, int]:
        x6, y6 = args
        return _process_one_bucket(
            workdir, x6, y6,
            z_max, tile_budget,
            memory_limit, internal_threads,
            checkpoint, checkpoint_lock,
            no_skip_existing,
            peel_idx=peel_idx,
            temp_dir=temp_dir,
        )

    # Within-peel bucket parallelism: workers process different z=6 buckets
    # of the same peel concurrently. Mac mini has 4 perf cores → workers=4
    # by default. Each worker opens its own DuckDB connection at
    # memory_limit, so 4 × memory_limit must fit the host budget.
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for (w, e, o, f) in pool.map(_run_bucket, bucks):
            n_written += w
            n_empty += e
            n_oversize += o
            n_failed += f

    manifest_path = _save_peel_manifest(workdir, peel_idx, list(checkpoint.values()))

    return {
        "peel.idx": peel_idx,
        "tiles_written": n_written,
        "tiles_empty": n_empty,
        "tiles_oversize": n_oversize,
        "tiles_failed": n_failed,
        "buckets_processed": len(bucks),
        "manifest_path": str(manifest_path),
        "duration_sec": round(time.time() - t0, 2),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--workdir", default=None)
    p.add_argument("--peel-idx", type=int, default=None,
                   help="Run one peel only (24/7 driver mode). Source = "
                        "staging/peel_<idx>/combined/, output = tiles/peel_<idx>/...")
    p.add_argument("--workers", type=int, default=4,
                   help="In legacy mode: parallel peel workers. In --peel-idx "
                        "mode: parallel z=6 bucket workers within the peel. "
                        "Default 4 = perf cores on Mac mini.")
    p.add_argument("--memory-limit", default=DEFAULT_MEMORY_LIMIT,
                   help=f"DuckDB memory_limit per worker (default {DEFAULT_MEMORY_LIMIT}).")
    p.add_argument("--internal-threads", type=int, default=DEFAULT_INTERNAL_THREADS)
    p.add_argument("--peel-width-deg", type=int, default=PEEL_WIDTH_DEG_DEFAULT,
                   help=f"Lng peel width in degrees (default {PEEL_WIDTH_DEG_DEFAULT}).")
    p.add_argument("--z-max", type=int, default=Z_MAX_DEFAULT,
                   help=f"Leaf zoom level (default {Z_MAX_DEFAULT}).")
    p.add_argument("--tile-budget-bytes", type=int, default=TILE_BUDGET_DEFAULT,
                   help=f"Oversize threshold (default {TILE_BUDGET_DEFAULT} = 20 MiB).")
    p.add_argument("--no-skip-existing", action="store_true",
                   help="Re-render leaves even if the tile file already exists.")
    args = p.parse_args()

    workdir = resolve_workdir(args.workdir)

    # --peel-idx → single-peel mode (24/7 driver path). Returns immediately
    # after run_one_peel finishes; no global combined dir / no global manifest.
    if args.peel_idx is not None:
        result = run_one_peel(
            workdir=workdir,
            peel_idx=args.peel_idx,
            z_max=args.z_max,
            tile_budget=args.tile_budget_bytes,
            workers=args.workers,
            memory_limit=args.memory_limit,
            internal_threads=args.internal_threads,
            peel_width_deg=args.peel_width_deg,
            no_skip_existing=args.no_skip_existing,
        )
        print(
            f"v13 Pass 2 (peel {args.peel_idx}, {peel_dir_name(args.peel_idx)}) done in "
            f"{result['duration_sec']}s: "
            f"{result['tiles_written']:,} written, {result['tiles_empty']:,} empty, "
            f"{result['tiles_oversize']} oversize, {result['tiles_failed']} failed",
            flush=True,
        )
        return

    print(f"v13 Pass 2 (peel-sharded leaf emit, legacy global mode)  workdir={workdir}", flush=True)
    print(f"  workers       = {args.workers}", flush=True)
    print(f"  memory_limit  = {args.memory_limit} per worker", flush=True)
    print(f"  peel_width    = {args.peel_width_deg}°", flush=True)
    print(f"  z_max         = {args.z_max}", flush=True)
    print(f"  tile_budget   = {args.tile_budget_bytes/1e6:.1f} MB", flush=True)

    combined_done = (combined_bucket_dir(workdir) / PASS1_5_DONE_MARKER).exists()
    if not combined_done:
        print(
            f"[warn] no _DONE marker at {combined_bucket_dir(workdir)}: pass1.5 may not have completed",
            flush=True,
        )

    bucket_set = _existing_combined_buckets(workdir)
    print(f"  combined buckets on disk = {len(bucket_set)}", flush=True)
    if not bucket_set:
        sys.exit(f"[error] no combined buckets found under {combined_bucket_dir(workdir)}")

    n_peels = (LNG_BOUNDS[1] - LNG_BOUNDS[0]) // args.peel_width_deg
    n_peels = int(n_peels)
    print(f"  peels         = {n_peels}", flush=True)

    checkpoint = _load_checkpoint(workdir)
    done_count = sum(1 for e in checkpoint.values() if e.get("status") == "done")
    empty_count = sum(1 for e in checkpoint.values() if e.get("status") == "empty")
    print(f"  checkpoint    = {done_count:,} done, {empty_count:,} empty (skip)", flush=True)

    checkpoint_lock = threading.Lock()
    save_counter = [0]

    def run_peel(peel_idx: int) -> tuple[int, int, int, int, int]:
        """Process every bucket in one peel sequentially."""
        bucks = _buckets_for_peel(peel_idx, args.peel_width_deg, bucket_set)
        n_written = 0
        n_empty = 0
        n_oversize = 0
        n_failed = 0
        for (x6, y6) in bucks:
            w, e, o, f = _process_one_bucket(
                workdir, x6, y6,
                args.z_max, args.tile_budget_bytes,
                args.memory_limit, args.internal_threads,
                checkpoint, checkpoint_lock,
                args.no_skip_existing,
            )
            n_written += w
            n_empty += e
            n_oversize += o
            n_failed += f
            with checkpoint_lock:
                save_counter[0] += 1
                if save_counter[0] % CHECKPOINT_SAVE_INTERVAL == 0:
                    _save_checkpoint(workdir, checkpoint)
        return peel_idx, n_written, n_empty, n_oversize, n_failed

    print(
        f"\nPass 2: {n_peels} peels × ~{len(bucket_set)/n_peels:.0f} buckets each, "
        f"{args.workers} parallel peel workers...\n",
        flush=True,
    )

    t0 = time.time()
    last_progress = t0
    total_written = 0
    total_empty = 0
    total_oversize = 0
    total_failed = 0
    completed_peels = 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(run_peel, pi) for pi in range(n_peels)]
        for fut in as_completed(futures):
            try:
                pi, w, e, o, f = fut.result()
                total_written += w
                total_empty += e
                total_oversize += o
                total_failed += f
            except Exception as exc:
                print(f"  peel exception: {exc}", flush=True)
                total_failed += 1
            completed_peels += 1
            now = time.time()
            if now - last_progress >= PROGRESS_INTERVAL_SEC or completed_peels == n_peels:
                rate = completed_peels / max(0.1, now - t0)
                eta = (n_peels - completed_peels) / max(0.001, rate)
                print(
                    f"--- progress: {completed_peels}/{n_peels} peels  "
                    f"written={total_written:,} empty={total_empty:,} "
                    f"oversize={total_oversize} failed={total_failed}  "
                    f"eta={eta/60:.1f} min",
                    flush=True,
                )
                last_progress = now

    _save_checkpoint(workdir, checkpoint)
    _save_manifest(workdir, checkpoint)

    elapsed = time.time() - t0
    print(
        f"\nv13 Pass 2 done in {elapsed/60:.1f} min: "
        f"{total_written:,} tiles written, {total_empty:,} empty, "
        f"{total_oversize} oversize (>{args.tile_budget_bytes/1e6:.0f} MB — pass3 will subdivide), "
        f"{total_failed} failed",
        flush=True,
    )


if __name__ == "__main__":
    main()
