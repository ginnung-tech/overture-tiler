"""tile_v12_pass2.py — batch-by-bucket Pass 2.

Replaces v11's Pass 2 (per-fine-cell DuckDB query against shared coarse
buckets), which hit ~24 cells/sec aggregate / 400ms per cell because every
cell paid the open-parquet + scan-metadata + range-filter + write + hash +
checkpoint-lock tax. Extrapolated to 18+ hours for infrastructure.

v13 architecture: per WORKER, take ONE coarse bucket. Inside one DuckDB
connection:

  1. Load the bucket's parquet into TEMP TABLE _bucket once (~750 KB → ms).
  2. Compute distinct fine cells the bucket touches via a single UNNEST +
     DISTINCT scan on _bucket (small in-memory, fast).
  3. For each fine cell: COPY (SELECT * EXCLUDE (...) FROM _bucket WHERE
     range filter) TO 'tile.parquet.gz'. Each query is in-memory on the
     small table, so per-cell cost drops from ~400ms (file-reopen tax) to
     <1ms.
  4. DROP TABLE _bucket; close connection.

100 fine sub-cells per coarse bucket. 17K buckets / 10 worker threads =
1700 buckets/worker, ~100ms/bucket processing → ~3 min total Pass 2.

Concurrency: ThreadPoolExecutor with N threads. DuckDB releases the GIL
during query execution, so threads truly parallelize.

Checkpoint, manifest, and oversize subdivide reuse v11 helpers from tile.py.

Run after v12 Pass 1.5 has populated _coarse:

    python tile_v12_pass2.py --theme infrastructure
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

# SQL-safety helpers — see _sql_safety.py for the why.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from _sql_safety import q_int, q_path, q_path_list  # noqa: E402

# Reuse from v11 tile.py
from tile import (  # noqa: E402
    MAX_TILE_BYTES,
    MIN_CELL_DEG,
    OVERTURE_RELEASE,
    START_CELL_DEG,
    Bbox,
    TileRecord,
    _COARSE_DIR_NAME,
    _COARSE_DONE_MARKER,
    _FINE_PER_COARSE_SIDE,
    _new_con,
    cell_to_address,
    coarse_bucket_paths,
    coarse_dir_for_theme,
    fine_cells_for_coarse,
    list_nonempty_coarse_cells,
    load_existing_manifest,
    load_tile_checkpoint,
    resolve_workdir,
    save_tile_checkpoint,
    serialize_manifest,
    staging_dir_for_theme,
    tile_filename,
    write_manifest,
)

CHECKPOINT_SAVE_INTERVAL = 200


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _process_coarse_bucket(
    coarse_lon: int,
    coarse_lat: int,
    bucket_paths: list[str],
    tiles_theme_dir: Path,
    region_bbox: Bbox | None,
    tile_checkpoint: dict[str, dict],
    checkpoint_lock: threading.Lock,
    manifest_records: list[TileRecord],
    theme: str,
    memory_limit: str = "2GB",
) -> tuple[int, int, int]:
    """Process all fine cells inside one coarse bucket. Returns (n_tiles, n_empty, n_failed)."""
    if not bucket_paths:
        return 0, 0, 0

    fine_cells = fine_cells_for_coarse(coarse_lon, coarse_lat, region_bbox)
    if not fine_cells:
        return 0, 0, 0

    # Filter out cells already done/empty in checkpoint.
    # ONE lock acquisition for the entire scan (no per-cell append into the
    # shared manifest list inside the loop). The shared dicts get a single
    # write at the end of the function. Replaces ~1.5M per-cell lock taps.
    todo: list[tuple[int, int]] = []
    local_manifest: list[TileRecord] = []
    with checkpoint_lock:
        for (cl, ca) in fine_cells:
            addr = cell_to_address(cl, ca)
            key = f"{addr.z}:{addr.x}:{addr.y}"
            entry = tile_checkpoint.get(key)
            if entry and entry["status"] in ("done", "empty"):
                if entry["status"] == "done":
                    local_manifest.append(TileRecord(
                        theme=theme,
                        z=addr.z, x=addr.x, y=addr.y,
                        bbox=addr.bbox.as_list(),
                        size_bytes=entry["size_bytes"],
                        feature_count=entry["feature_count"],
                    ))
                continue
            todo.append((cl, ca))

    if not todo:
        if local_manifest:
            with checkpoint_lock:
                manifest_records.extend(local_manifest)
        return 0, 0, 0

    files_lit = q_path_list(bucket_paths)
    con = _new_con(internal_threads=2, memory_limit=memory_limit)
    n_tiles = 0
    n_empty = 0
    n_failed = 0
    # Per-bucket local accumulators — drained into shared state ONCE below.
    local_updates: dict[str, dict] = {}
    try:
        # Load entire bucket into a temp table ONCE.
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _bucket AS
            SELECT * FROM read_parquet({files_lit}, union_by_name=True)
        """)

        for (cl, ca) in todo:
            addr = cell_to_address(cl, ca)
            key = f"{addr.z}:{addr.x}:{addr.y}"
            tile_name = tile_filename(addr.z, addr.x, addr.y)
            tile_path = tiles_theme_dir / tile_name

            # Write directly to the final path — DuckDB COPY overwrites if the
            # file exists, so the .tmp rename dance from v11 is gone. Saves
            # one filesystem op per tile (~1M ops). Pass 2 is checkpoint-
            # restartable, so a partial file on crash is fine: the next run
            # has no checkpoint entry, retries the cell, COPY overwrites.
            try:
                cl_q, ca_q = q_int(cl), q_int(ca)
                con.execute(f"""
                    COPY (
                        SELECT * EXCLUDE (_lon_lo, _lon_hi, _lat_lo, _lat_hi)
                        FROM _bucket
                        WHERE _lon_lo <= {cl_q} AND _lon_hi >= {cl_q}
                          AND _lat_lo <= {ca_q} AND _lat_hi >= {ca_q}
                    ) TO '{q_path(tile_path)}' (FORMAT 'parquet', COMPRESSION 'zstd', COMPRESSION_LEVEL 3)
                """)
            except Exception as e:
                print(f"  FAIL {theme}/{tile_name}: {e}", flush=True)
                n_failed += 1
                local_updates[key] = {
                    "z": addr.z, "x": addr.x, "y": addr.y,
                    "status": "failed", "feature_count": 0, "size_bytes": 0,
                    "sha256": None, "at": _iso_now(),
                }
                continue

            if not tile_path.exists():
                local_updates[key] = {
                    "z": addr.z, "x": addr.x, "y": addr.y,
                    "status": "empty", "feature_count": 0, "size_bytes": 0,
                    "sha256": None, "at": _iso_now(),
                }
                n_empty += 1
                continue

            size_bytes = tile_path.stat().st_size
            count_row = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{q_path(tile_path)}')"
            ).fetchone()
            feature_count = int(count_row[0]) if count_row else 0

            if feature_count == 0:
                tile_path.unlink()
                local_updates[key] = {
                    "z": addr.z, "x": addr.x, "y": addr.y,
                    "status": "empty", "feature_count": 0, "size_bytes": 0,
                    "sha256": None, "at": _iso_now(),
                }
                n_empty += 1
                continue

            # SHA256 deliberately skipped — costs ~5ms/tile × ~1M tiles =
            # ~80 min of CPU that nothing reads. If dedup ever needs a
            # content fingerprint, derive it from (size_bytes, mtime).

            # Oversize warning (no recursive subdivide here — accept warning)
            if size_bytes > MAX_TILE_BYTES:
                print(
                    f"  WARN {theme}/{tile_name} {size_bytes/1e6:.1f} MB > 20 MB "
                    f"at min cell ({MIN_CELL_DEG}°) — accepting",
                    flush=True,
                )

            local_updates[key] = {
                "z": addr.z, "x": addr.x, "y": addr.y,
                "status": "done",
                "feature_count": feature_count,
                "size_bytes": size_bytes,
                "sha256": None, "at": _iso_now(),
            }
            local_manifest.append(TileRecord(
                theme=theme,
                z=addr.z, x=addr.x, y=addr.y,
                bbox=addr.bbox.as_list(),
                size_bytes=size_bytes,
                feature_count=feature_count,
            ))
            n_tiles += 1
    finally:
        try:
            con.execute("DROP TABLE IF EXISTS _bucket")
            con.close()
        except Exception:
            pass

    # SINGLE per-bucket lock acquisition. Replaces the ~1.5M per-tile
    # acquisitions in the v11 path — that contention was the dead-flat
    # 1.0 bucket/s ceiling observed on the 2026-05-05 infrastructure run
    # (workers starved on the lock; CPU/RAM/disk all idle).
    with checkpoint_lock:
        tile_checkpoint.update(local_updates)
        manifest_records.extend(local_manifest)

    return n_tiles, n_empty, n_failed


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--theme", required=True)
    p.add_argument("--workdir", default=None)
    p.add_argument("--workers", type=int, default=None,
                   help="parallel threads (default: cpu_count - 2). On low-RAM "
                        "machines (8 GB Mac mini), pass --workers 4 with "
                        "--memory-limit 2GB so total stays under physical RAM.")
    p.add_argument("--memory-limit", default="2GB",
                   help="DuckDB memory_limit per worker connection (default 2GB). "
                        "workers × memory-limit ≤ physical RAM is the rule of "
                        "thumb; rely on swap only as safety margin.")
    p.add_argument("--bbox", default=None,
                   help="restrict to lon_lo,lat_lo,lon_hi,lat_hi degrees")
    p.add_argument("--release", default=None)
    args = p.parse_args()

    workdir = resolve_workdir(args.workdir)
    n_workers = args.workers or max(1, (os.cpu_count() or 4) - 2)
    memory_limit = args.memory_limit
    print(f"v12 Pass 2 (batch-by-bucket)  theme={args.theme}")
    print(f"  workdir = {workdir}")
    print(f"  workers = {n_workers}")
    print(f"  memory_limit = {memory_limit} per worker")

    coarse_dir = coarse_dir_for_theme(workdir, args.theme)
    if not (coarse_dir / _COARSE_DONE_MARKER).exists():
        print(f"[error] no _DONE marker at {coarse_dir} — run v12 Pass 1.5 first",
              file=sys.stderr)
        sys.exit(1)

    coarse_cells = list_nonempty_coarse_cells(coarse_dir)
    print(f"  coarse buckets = {len(coarse_cells):,}")
    if not coarse_cells:
        print("nothing to do.")
        return

    region_bbox: Bbox | None = None
    if args.bbox:
        parts = [float(x) for x in args.bbox.split(",")]
        if len(parts) != 4:
            raise SystemExit("--bbox needs 4 comma-separated values")
        region_bbox = Bbox(
            min_lon=parts[0], min_lat=parts[1],
            max_lon=parts[2], max_lat=parts[3],
        )

    tiles_theme_dir = workdir / "tiles" / args.theme
    tiles_theme_dir.mkdir(parents=True, exist_ok=True)

    tile_checkpoint = load_tile_checkpoint(tiles_theme_dir)
    done_count = sum(1 for e in tile_checkpoint.values() if e["status"] == "done")
    empty_count = sum(1 for e in tile_checkpoint.values() if e["status"] == "empty")
    print(f"  checkpoint: {done_count:,} done, {empty_count:,} empty (will skip)")

    manifest_records: list[TileRecord] = []
    existing = load_existing_manifest(workdir)
    for r in existing:
        if r.theme != args.theme:
            manifest_records.append(r)

    checkpoint_lock = threading.Lock()
    save_counter = [0]

    def task(args_tuple):
        coarse_lon, coarse_lat = args_tuple
        bucket = coarse_bucket_paths(coarse_dir, coarse_lon, coarse_lat)
        return _process_coarse_bucket(
            coarse_lon, coarse_lat, bucket, tiles_theme_dir, region_bbox,
            tile_checkpoint, checkpoint_lock, manifest_records, args.theme,
            memory_limit=memory_limit,
        )

    print()
    print(f"Pass 2: processing {len(coarse_cells):,} coarse buckets ({n_workers} threads)...",
          flush=True)
    t0 = time.time()
    last_progress = t0
    progress_interval = 30

    total_tiles = 0
    total_empty = 0
    total_failed = 0
    completed = 0

    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = [pool.submit(task, c) for c in coarse_cells]
        for fut in as_completed(futures):
            try:
                n_t, n_e, n_f = fut.result()
                total_tiles += n_t
                total_empty += n_e
                total_failed += n_f
            except Exception as e:
                print(f"  worker exception: {e}", flush=True)
                total_failed += 1
            completed += 1
            with checkpoint_lock:
                save_counter[0] += 1
                if save_counter[0] % CHECKPOINT_SAVE_INTERVAL == 0:
                    save_tile_checkpoint(tiles_theme_dir, tile_checkpoint)
            now = time.time()
            if now - last_progress >= progress_interval or completed == len(coarse_cells):
                rate = completed / max(0.1, now - t0)
                eta = (len(coarse_cells) - completed) / max(0.001, rate)
                print(
                    f"--- progress: {completed:,}/{len(coarse_cells):,} buckets "
                    f"({100*completed/len(coarse_cells):.1f}%)  "
                    f"rate={rate:.1f} bucket/s  "
                    f"tiles={total_tiles:,} empty={total_empty:,} failed={total_failed}  "
                    f"eta={eta/60:.1f} min",
                    flush=True,
                )
                last_progress = now

    elapsed = time.time() - t0
    save_tile_checkpoint(tiles_theme_dir, tile_checkpoint)
    write_manifest(workdir, manifest_records)
    print()
    print(f"Pass 2 done in {elapsed/60:.1f} min: {total_tiles:,} tiles, "
          f"{total_empty:,} empty, {total_failed} failed")


if __name__ == "__main__":
    main()
