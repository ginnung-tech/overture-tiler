#!/usr/bin/env python3
"""tile_v13_pass1.py — partition each Overture theme by Mercator z=6 quadkey.

Stage 2a / pass 1 of the v13 pipeline (see `v13_SPEC.md`). For every
theme listed in `THEMES`, read the raw parquet under `<workdir>/raw/<theme>/`
and write per-bucket partitions to:

    <workdir>/.tile-staging/v13/per_theme/<theme>/mercator_x=X/mercator_y=Y/*.parquet

Each row carries an added `theme` column with the source theme name. A
`_DONE` marker is written per-theme on success.

Bucket address derivation (v13 v1 simplification)
------------------------------------------------

Each row is mapped to *one* z=6 bucket via the Mercator projection of its
bbox centroid. Multi-z=6 features (long polylines / large polygons) are
written to the bucket of their centroid only. This keeps pass1 a pure
DuckDB COPY ... PARTITION_BY with no UNNEST step, which dominated v11
pass1.5 wall time.

Cross-bucket features show up correctly in pass2 because pass2 emits
*every* z=14 leaf inside a bucket, with feature filtering by lng/lat
extent — a feature's geometry can extend beyond the bucket extent and
still be clipped into other leaves *within* its centroid bucket. Tiles
near a bucket boundary may miss a sliver of the feature whose geometry
extends into the adjacent bucket; v13 v1 accepts this as the cost of the
single-pass partition. v2 can cross-join centroid bucket + neighbour
buckets via a small UNNEST if visual artefacts appear at z=6 boundaries.

Run
---

    python tile_v13_pass1.py [--workdir /Volumes/SSD/overture] [--workers N]
"""
from __future__ import annotations

import argparse
import multiprocessing as mp
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Allow `from tile_v13_helpers import ...` when running as a script.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from _sql_safety import q_int, q_path, q_path_list  # noqa: E402
from tile_v13_helpers import (  # noqa: E402
    DEFAULT_INTERNAL_THREADS,
    DEFAULT_MEMORY_LIMIT,
    THEMES,
    Z_BUCKET,
    new_con,
    per_theme_partition_dir,
    raw_theme_dir,
    resolve_workdir,
)


def _safe_theme(theme: str) -> str:
    """Validate `theme` against the hard-coded THEMES list.

    Used before interpolating the theme value as a SQL string literal.
    """
    if theme not in THEMES:
        raise ValueError(f"unknown theme: {theme!r}")
    return theme

PASS1_DONE_MARKER = "_DONE"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _raw_files_for_theme(workdir: Path, theme: str) -> list[Path]:
    """List `*.parquet` under `<workdir>/raw/<theme>/` (recursive)."""
    root = raw_theme_dir(workdir, theme)
    if not root.exists():
        return []
    return sorted(p for p in root.rglob("*.parquet") if p.is_file())


def _theme_done(workdir: Path, theme: str) -> bool:
    return (per_theme_partition_dir(workdir, theme) / PASS1_DONE_MARKER).exists()


def _partition_one_theme(
    workdir_str: str,
    theme: str,
    memory_limit: str,
    internal_threads: int,
) -> tuple[str, bool, str | None, int]:
    """Partition one theme by Mercator z=6 (mercator_x, mercator_y).

    Picklable for multiprocessing. Returns (theme, ok, error, n_buckets).
    """
    workdir = Path(workdir_str)
    out_dir = per_theme_partition_dir(workdir, theme)
    raw_files = _raw_files_for_theme(workdir, theme)
    if not raw_files:
        return theme, False, f"no raw files under {raw_theme_dir(workdir, theme)}", 0

    # Wipe any partial output so we never mix stale rows with a fresh run.
    if out_dir.exists():
        shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    files_lit = q_path_list(raw_files)
    n = 1 << Z_BUCKET  # 64
    safe_theme = _safe_theme(theme)  # whitelist-checked; safe to interpolate

    # The Mercator projection in pure SQL: clamp lat to ±85.0511287798066,
    # then standard slippy-map x = floor((lng+180)/360 * n), y from the
    # log-tan formula. Computed from the bbox centroid.
    sql = f"""
        COPY (
            WITH base AS (
                SELECT
                    *,
                    '{safe_theme}' AS theme,
                    (bbox.xmin + bbox.xmax) / 2.0 AS _cx,
                    (bbox.ymin + bbox.ymax) / 2.0 AS _cy_raw
                FROM read_parquet({files_lit}, union_by_name=True)
                WHERE bbox IS NOT NULL
            ),
            projected AS (
                SELECT
                    *,
                    GREATEST(LEAST(_cy_raw, 85.0511287798066), -85.0511287798066) AS _cy
                FROM base
            ),
            keyed AS (
                SELECT
                    * EXCLUDE (_cx, _cy, _cy_raw),
                    GREATEST(LEAST(CAST(floor((_cx + 180.0) / 360.0 * {q_int(n)}) AS INTEGER), {q_int(n - 1)}), 0) AS mercator_x,
                    GREATEST(LEAST(
                        CAST(floor(
                            (1.0 - ln(tan(radians(_cy)) + 1.0 / cos(radians(_cy))) / pi()) / 2.0 * {q_int(n)}
                        ) AS INTEGER),
                        {q_int(n - 1)}
                    ), 0) AS mercator_y
                FROM projected
            )
            SELECT * FROM keyed
        ) TO '{q_path(out_dir)}' (
            FORMAT 'parquet',
            PARTITION_BY (mercator_x, mercator_y),
            COMPRESSION 'zstd',
            COMPRESSION_LEVEL 3,
            OVERWRITE_OR_IGNORE TRUE
        )
    """

    con = new_con(internal_threads=internal_threads, memory_limit=memory_limit)
    try:
        con.execute(sql)
    except Exception as e:
        return theme, False, str(e), 0
    finally:
        try:
            con.close()
        except Exception:
            pass

    # Walk the output dir to count how many buckets actually got data.
    n_buckets = 0
    for x_dir in out_dir.iterdir():
        if not x_dir.is_dir() or not x_dir.name.startswith("mercator_x="):
            continue
        for y_dir in x_dir.iterdir():
            if y_dir.is_dir() and y_dir.name.startswith("mercator_y="):
                n_buckets += 1

    (out_dir / PASS1_DONE_MARKER).write_text(_iso_now(), encoding="utf-8")
    return theme, True, None, n_buckets


def _worker_entry(args_tuple):
    """Module-level entry — picklable for multiprocessing.Pool."""
    workdir_str, theme, memory_limit, internal_threads = args_tuple
    return _partition_one_theme(workdir_str, theme, memory_limit, internal_threads)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--workdir", default=None,
                   help=f"Workdir root. Default OVERTURE_WORKDIR or platform default.")
    p.add_argument("--workers", type=int, default=None,
                   help="Parallel theme-partition processes (default: min(len(themes), cpu-2)).")
    p.add_argument("--memory-limit", default=DEFAULT_MEMORY_LIMIT,
                   help=f"DuckDB memory_limit per worker (default {DEFAULT_MEMORY_LIMIT}).")
    p.add_argument("--internal-threads", type=int, default=DEFAULT_INTERNAL_THREADS,
                   help=f"DuckDB internal threads per worker (default {DEFAULT_INTERNAL_THREADS}).")
    p.add_argument("--themes", default=None,
                   help="Comma-separated subset of THEMES to process (default all six).")
    p.add_argument("--force", action="store_true",
                   help="Re-partition themes even if their _DONE marker exists.")
    args = p.parse_args()

    workdir = resolve_workdir(args.workdir)
    themes = THEMES if args.themes is None else [t.strip() for t in args.themes.split(",") if t.strip()]
    for t in themes:
        if t not in THEMES:
            sys.exit(f"[error] unknown theme '{t}' (known: {','.join(THEMES)})")

    print(f"v13 Pass 1 (Mercator z={Z_BUCKET} partition)  workdir={workdir}", flush=True)
    print(f"  themes  = {themes}", flush=True)
    print(f"  memory  = {args.memory_limit} per worker", flush=True)

    todo: list[str] = []
    for theme in themes:
        if not args.force and _theme_done(workdir, theme):
            print(f"  skip {theme}: _DONE marker present", flush=True)
            continue
        if not _raw_files_for_theme(workdir, theme):
            print(f"  skip {theme}: no raw files under {raw_theme_dir(workdir, theme)}", flush=True)
            continue
        todo.append(theme)
    if not todo:
        print("nothing to do.", flush=True)
        return

    n_workers = args.workers or max(1, min(len(todo), (os.cpu_count() or 4) - 2))
    n_workers = min(n_workers, len(todo))
    print(f"  workers = {n_workers}", flush=True)

    tasks = [(str(workdir), t, args.memory_limit, args.internal_threads) for t in todo]
    t0 = time.time()
    last_progress = t0
    progress_interval = 30

    with mp.Pool(processes=n_workers) as pool:
        for i, (theme, ok, err, n_buckets) in enumerate(
            pool.imap_unordered(_worker_entry, tasks), start=1
        ):
            elapsed = time.time() - t0
            if ok:
                print(
                    f"  [{i}/{len(tasks)}] OK   {theme}: {n_buckets} buckets  "
                    f"elapsed={elapsed/60:.1f} min",
                    flush=True,
                )
            else:
                print(
                    f"  [{i}/{len(tasks)}] FAIL {theme}: {err}",
                    flush=True,
                )
            now = time.time()
            if now - last_progress >= progress_interval:
                print(f"--- progress: {i}/{len(tasks)} themes in {(now-t0)/60:.1f} min", flush=True)
                last_progress = now

    print(f"v13 Pass 1 done in {(time.time()-t0)/60:.1f} min", flush=True)


if __name__ == "__main__":
    main()
