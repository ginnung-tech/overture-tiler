#!/usr/bin/env python3
"""tile_v13_pass1_5.py — collate per-theme buckets into combined-theme parquets.

Stage 2a / pass 1.5 of the v13 pipeline (see `v13_SPEC.md`). Reads pass1's
per-theme partitioned output:

    <workdir>/.tile-staging/v13/per_theme/<theme>/mercator_x=X/mercator_y=Y/*.parquet

For every (x, y) bucket that has data in *any* theme, write one combined
parquet that union's all six themes' rows for that bucket:

    <workdir>/.tile-staging/v13/combined/z6_{x}_{y}.parquet

The `theme` column (added by pass1) is preserved; the union schema is
established via DuckDB's `read_parquet(..., union_by_name=True)`. After
each successful combined write, the source per-theme partitions for that
bucket are dropped (gated on `--keep-intermediate`).

Run after pass1 has emitted a `_DONE` marker for each theme.

Run
---

    python tile_v13_pass1_5.py [--workdir /Volumes/SSD/overture] [--workers N]
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

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _sql_safety import q_path, q_path_list  # noqa: E402
from tile_v13_helpers import (  # noqa: E402
    DEFAULT_INTERNAL_THREADS,
    DEFAULT_MEMORY_LIMIT,
    THEMES,
    Z_BUCKET,
    combined_bucket_dir,
    combined_bucket_path,
    new_con,
    per_theme_partition_dir,
    resolve_workdir,
)

PASS1_5_DONE_MARKER = "_DONE"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _bucket_dir_for_theme(workdir: Path, theme: str, x: int, y: int) -> Path:
    return per_theme_partition_dir(workdir, theme) / f"mercator_x={x}" / f"mercator_y={y}"


def _list_buckets_for_theme(workdir: Path, theme: str) -> set[tuple[int, int]]:
    """Set of (x, y) buckets pass1 produced for `theme`."""
    root = per_theme_partition_dir(workdir, theme)
    out: set[tuple[int, int]] = set()
    if not root.exists():
        return out
    for x_dir in root.iterdir():
        if not x_dir.is_dir() or not x_dir.name.startswith("mercator_x="):
            continue
        try:
            x = int(x_dir.name.split("=", 1)[1])
        except ValueError:
            continue
        for y_dir in x_dir.iterdir():
            if not y_dir.is_dir() or not y_dir.name.startswith("mercator_y="):
                continue
            try:
                y = int(y_dir.name.split("=", 1)[1])
            except ValueError:
                continue
            # Only count buckets that contain at least one parquet file.
            if any(p.suffix == ".parquet" for p in y_dir.iterdir()):
                out.add((x, y))
    return out


def _all_themes_done(workdir: Path) -> tuple[bool, list[str]]:
    """Return (all_done, missing_themes)."""
    missing = [
        t for t in THEMES
        if not (per_theme_partition_dir(workdir, t) / "_DONE").exists()
    ]
    return (not missing, missing)


def _combine_one_bucket(
    workdir_str: str,
    x: int,
    y: int,
    memory_limit: str,
    internal_threads: int,
    keep_intermediate: bool,
) -> tuple[int, int, bool, str | None, int, int]:
    """Combine all theme partitions for bucket (x, y).

    Picklable for multiprocessing. Returns
    (x, y, ok, error, n_input_files, n_rows).
    """
    workdir = Path(workdir_str)
    out_path = combined_bucket_path(workdir, x, y)

    # Collect every theme's parquet files for this bucket.
    src_files: list[Path] = []
    src_dirs: list[Path] = []  # for cleanup if keep_intermediate is False
    for theme in THEMES:
        bd = _bucket_dir_for_theme(workdir, theme, x, y)
        if not bd.exists():
            continue
        theme_files = sorted(p for p in bd.iterdir() if p.suffix == ".parquet")
        if not theme_files:
            continue
        src_files.extend(theme_files)
        src_dirs.append(bd)

    if not src_files:
        return x, y, True, None, 0, 0

    out_path.parent.mkdir(parents=True, exist_ok=True)
    files_lit = q_path_list(src_files)
    tmp = out_path.with_suffix(".parquet.tmp")

    sql = f"""
        COPY (
            SELECT * FROM read_parquet({files_lit}, union_by_name=True)
        ) TO '{q_path(tmp)}' (
            FORMAT 'parquet',
            COMPRESSION 'zstd',
            COMPRESSION_LEVEL 3
        )
    """

    con = new_con(internal_threads=internal_threads, memory_limit=memory_limit)
    n_rows = 0
    try:
        con.execute(sql)
        if not tmp.exists():
            return x, y, False, "combined output missing after COPY", len(src_files), 0
        row = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{q_path(tmp)}')"
        ).fetchone()
        n_rows = int(row[0]) if row else 0
    except Exception as e:
        if tmp.exists():
            try:
                tmp.unlink()
            except Exception:
                pass
        return x, y, False, str(e), len(src_files), 0
    finally:
        try:
            con.close()
        except Exception:
            pass

    # Atomic rename.
    tmp.replace(out_path)

    if not keep_intermediate:
        for d in src_dirs:
            try:
                shutil.rmtree(d, ignore_errors=True)
            except Exception:
                pass

    return x, y, True, None, len(src_files), n_rows


def _worker_entry(args_tuple):
    return _combine_one_bucket(*args_tuple)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--workdir", default=None)
    p.add_argument("--workers", type=int, default=None,
                   help="Parallel bucket-combine processes (default: cpu-2).")
    p.add_argument("--memory-limit", default=DEFAULT_MEMORY_LIMIT,
                   help=f"DuckDB memory_limit per worker (default {DEFAULT_MEMORY_LIMIT}).")
    p.add_argument("--internal-threads", type=int, default=DEFAULT_INTERNAL_THREADS)
    p.add_argument("--keep-intermediate", action="store_true",
                   help="Keep per-theme bucket partitions after combining.")
    p.add_argument("--force", action="store_true",
                   help="Re-combine buckets even if combined file exists.")
    args = p.parse_args()

    workdir = resolve_workdir(args.workdir)
    print(f"v13 Pass 1.5 (collate per-bucket across themes)  workdir={workdir}", flush=True)

    all_done, missing = _all_themes_done(workdir)
    if not all_done:
        # Allow proceeding if pass1 only ran a subset; we just won't have
        # cross-theme content for the missing ones. Warn loudly (per the
        # "loud failure" rule).
        print(
            f"  WARN: pass1 _DONE missing for themes: {missing} — "
            f"combined buckets will only contain themes that completed.",
            flush=True,
        )

    # Union of all bucket keys across all themes.
    all_keys: set[tuple[int, int]] = set()
    for theme in THEMES:
        keys = _list_buckets_for_theme(workdir, theme)
        all_keys.update(keys)
        print(f"  {theme}: {len(keys)} buckets", flush=True)

    if not all_keys:
        print("[error] no per-theme buckets found", file=sys.stderr)
        sys.exit(1)

    out_dir = combined_bucket_dir(workdir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Skip already-combined buckets unless --force.
    todo: list[tuple[int, int]] = []
    skipped = 0
    for (x, y) in sorted(all_keys):
        if not args.force and combined_bucket_path(workdir, x, y).exists():
            skipped += 1
            continue
        todo.append((x, y))

    print(
        f"  todo    = {len(todo)} buckets  (skipped {skipped} already combined)",
        flush=True,
    )
    print(f"  output  = {out_dir}", flush=True)

    if not todo:
        # Still write the marker so pass2 can rely on it.
        (out_dir / PASS1_5_DONE_MARKER).write_text(_iso_now(), encoding="utf-8")
        print("nothing to do; marker written.", flush=True)
        return

    n_workers = args.workers or max(1, (os.cpu_count() or 4) - 2)
    n_workers = max(1, min(n_workers, len(todo)))
    print(f"  workers = {n_workers}", flush=True)

    tasks = [
        (str(workdir), x, y, args.memory_limit, args.internal_threads, args.keep_intermediate)
        for (x, y) in todo
    ]

    t0 = time.time()
    last_progress = t0
    progress_interval = 30
    n_ok = 0
    n_fail = 0
    n_rows_total = 0

    with mp.Pool(processes=n_workers) as pool:
        for i, (x, y, ok, err, _n_files, n_rows) in enumerate(
            pool.imap_unordered(_worker_entry, tasks), start=1
        ):
            if ok:
                n_ok += 1
                n_rows_total += n_rows
            else:
                n_fail += 1
                print(f"  FAIL bucket ({x},{y}): {err}", flush=True)

            now = time.time()
            if now - last_progress >= progress_interval or i == len(tasks):
                rate = i / max(0.1, now - t0)
                eta = (len(tasks) - i) / max(0.001, rate)
                print(
                    f"--- progress: {i}/{len(tasks)} buckets "
                    f"({100*i/len(tasks):.1f}%)  rate={rate:.1f} bucket/s  "
                    f"ok={n_ok} fail={n_fail} rows={n_rows_total:,}  "
                    f"eta={eta/60:.1f} min",
                    flush=True,
                )
                last_progress = now

    elapsed = time.time() - t0
    if n_fail == 0:
        (out_dir / PASS1_5_DONE_MARKER).write_text(_iso_now(), encoding="utf-8")
        print(
            f"v13 Pass 1.5 done in {elapsed/60:.1f} min: "
            f"{n_ok} buckets, {n_rows_total:,} rows",
            flush=True,
        )
    else:
        print(
            f"v13 Pass 1.5 finished with errors in {elapsed/60:.1f} min: "
            f"{n_ok} ok, {n_fail} failed — _DONE marker NOT written",
            flush=True,
        )
        sys.exit(2)


if __name__ == "__main__":
    main()
