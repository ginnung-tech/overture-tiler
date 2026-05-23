#!/usr/bin/env python3
"""tile_v13_pass1_per_peel.py — read one peel's slice from Overture S3.

Replaces the global pass1 + pass1.5 chain for the 24/7 driver's prefetch
stage. For peel `N` with lng range `[lng_lo, lng_hi)`:

  1. For each of the 6 Overture themes, run ONE DuckDB COPY that:
     - reads parquet directly from `s3://overturemaps-us-west-2/release/<rel>/theme=<grp>/type=<t>/*.parquet`
     - prunes row groups via predicate pushdown on bbox
     - adds a `theme` column
     - computes Mercator z=6 `(x, y)` from the bbox centroid
     - PARTITION_BY (mercator_x, mercator_y) into
       `<workdir>/staging/peel_<idx>/per_theme/<theme>/mercator_x=X/mercator_y=Y/*.parquet`

  2. For each `(x, y)` bucket present, collate all themes' files into one
     `<workdir>/staging/peel_<idx>/combined/z6_{x}_{y}.parquet` (zstd-3,
     union_by_name=True).

  3. Drop the per-theme intermediate (pass2 reads only `combined/`).

  4. Write `<workdir>/staging/peel_<idx>/_DONE`.

Driven by the 24/7 driver's Stage A. Single DuckDB connection — runs
concurrently with Stage B (pass2 + pass3 + upload) on a *different* peel.

Run
---

    python tile_v13_pass1_per_peel.py --peel-idx 18 [--workdir ...] [--release 2026-04-15.0]

Requires `OVERTURE_RELEASE` env var or `--release` flag.
"""
from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _sql_safety import q_int, q_path, q_path_list  # noqa: E402
from tile_v13_helpers import (  # noqa: E402
    DEFAULT_MEMORY_LIMIT,
    PEEL_WIDTH_DEG_DEFAULT,
    THEMES,
    Z_BUCKET,
    combined_bucket_dir_peel,
    combined_bucket_path_peel,
    duckdb_tmp_dir_peel,
    new_con,
    peel_dir_name,
    peel_lng_range,
    per_theme_partition_dir_peel,
    resolve_workdir,
    staging_peel_dir,
)
from tile_v13_sentry import init_sentry, log_event, phase_span

DONE_MARKER = "_DONE"

# Overture S3 layout. Mirrors download.py's THEME_S3_PREFIXES.
THEME_S3_PREFIXES: dict[str, str] = {
    "buildings":      "release/{release}/theme=buildings/type=building/",
    "segments":       "release/{release}/theme=transportation/type=segment/",
    "land_use":       "release/{release}/theme=base/type=land_use/",
    "water":          "release/{release}/theme=base/type=water/",
    "land":           "release/{release}/theme=base/type=land/",
    "infrastructure": "release/{release}/theme=base/type=infrastructure/",
}
S3_BUCKET = "overturemaps-us-west-2"

# Defensive — release tags are interpolated into the S3 URL passed to DuckDB.
_RELEASE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}(\.\d+)?$")

# Prefetch-stage memory budget. Larger than the per-worker pass2 budget
# (6 GB) because there's only one prefetch connection at a time.
PREFETCH_MEMORY_LIMIT = "8GB"
# Use both perf cores for S3 fetches (single connection => no over-subscription).
PREFETCH_INTERNAL_THREADS = 2


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _safe_release(s: str) -> str:
    if not _RELEASE_RE.match(s):
        raise ValueError(f"unsafe release tag: {s!r}")
    return s


def _safe_theme(theme: str) -> str:
    if theme not in THEMES:
        raise ValueError(f"unknown theme: {theme!r}")
    return theme


def _configure_httpfs(con) -> None:
    """Install + load the httpfs extension and configure anonymous S3 reads.

    Overture's bucket is public and supports anonymous access; setting an
    empty access key prevents DuckDB from probing the AWS credential chain
    (which fails on a clean Mac mini without `~/.aws/credentials`).
    """
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_region = 'us-west-2'")
    con.execute("SET s3_use_ssl = true")
    # Anonymous read — empty credentials trigger the unsigned-request path.
    con.execute("SET s3_access_key_id = ''")
    con.execute("SET s3_secret_access_key = ''")


def _partition_one_theme_from_s3(
    con,
    theme: str,
    release: str,
    lng_lo: float,
    lng_hi: float,
    out_dir: Path,
) -> int:
    """Read one theme's parquet from S3, bbox-filter, partition by z=6.

    Returns the number of (x, y) buckets that received data.
    """
    safe_theme = _safe_theme(theme)
    safe_release = _safe_release(release)

    s3_glob = f"s3://{S3_BUCKET}/{THEME_S3_PREFIXES[safe_theme].format(release=safe_release)}*.parquet"
    n = 1 << Z_BUCKET  # 64

    if out_dir.exists():
        shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    # The WHERE on bbox.xmin/xmax is the predicate pushdown that makes this
    # economical — DuckDB skips row groups whose bbox stats fall outside the
    # peel's lng range. Lat is unrestricted (peels span full Mercator lat).
    # Bbox-centroid Mercator projection mirrors tile_v13_pass1._partition_one_theme.
    sql = f"""
        COPY (
            WITH base AS (
                SELECT
                    *,
                    '{safe_theme}' AS theme,
                    (bbox.xmin + bbox.xmax) / 2.0 AS _cx,
                    (bbox.ymin + bbox.ymax) / 2.0 AS _cy_raw
                FROM read_parquet('{s3_glob}', union_by_name=True, hive_partitioning=false)
                WHERE bbox IS NOT NULL
                  AND bbox.xmin < {lng_hi}
                  AND bbox.xmax > {lng_lo}
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
    con.execute(sql)

    # Count populated buckets (also confirms COPY produced output).
    n_buckets = 0
    for x_dir in out_dir.iterdir() if out_dir.exists() else []:
        if not x_dir.is_dir() or not x_dir.name.startswith("mercator_x="):
            continue
        for y_dir in x_dir.iterdir():
            if y_dir.is_dir() and y_dir.name.startswith("mercator_y="):
                if any(p.suffix == ".parquet" for p in y_dir.iterdir()):
                    n_buckets += 1
    return n_buckets


def _list_buckets_for_peel(workdir: Path, peel_idx: int) -> dict[tuple[int, int], list[Path]]:
    """Walk per_theme/<theme>/mercator_x=X/mercator_y=Y/*.parquet and group by (x, y)."""
    out: dict[tuple[int, int], list[Path]] = {}
    for theme in THEMES:
        root = per_theme_partition_dir_peel(workdir, peel_idx, theme)
        if not root.exists():
            continue
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
                files = sorted(p for p in y_dir.iterdir() if p.suffix == ".parquet")
                if files:
                    out.setdefault((x, y), []).extend(files)
    return out


def _combine_one_bucket(con, workdir: Path, peel_idx: int, x: int, y: int, files: list[Path]) -> int:
    """Union all themes' files for one (x, y) bucket into a single combined parquet.

    Returns the row count of the combined output (for logging).
    """
    out_path = combined_bucket_path_peel(workdir, peel_idx, x, y)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(".parquet.tmp")
    if tmp.exists():
        tmp.unlink()

    files_lit = q_path_list(files)
    sql = f"""
        COPY (
            SELECT * FROM read_parquet({files_lit}, union_by_name=True)
        ) TO '{q_path(tmp)}' (
            FORMAT 'parquet',
            COMPRESSION 'zstd',
            COMPRESSION_LEVEL 3
        )
    """
    con.execute(sql)
    if not tmp.exists():
        raise RuntimeError(f"combined output missing after COPY: peel={peel_idx} ({x},{y})")
    row = con.execute(f"SELECT COUNT(*) FROM read_parquet('{q_path(tmp)}')").fetchone()
    n_rows = int(row[0]) if row else 0
    tmp.replace(out_path)
    return n_rows


def run_one_peel(
    workdir: Path,
    peel_idx: int,
    release: str,
    cycle: int = 0,
    peel_width_deg: int = PEEL_WIDTH_DEG_DEFAULT,
    memory_limit: str = PREFETCH_MEMORY_LIMIT,
    internal_threads: int = PREFETCH_INTERNAL_THREADS,
) -> dict:
    """Top-level callable. The driver invokes this for Stage A of each peel.

    Returns a counters dict suitable for stuffing into the phase_span context
    (themes_rows, themes_buckets, combined_buckets, combined_rows, duration_sec).
    """
    safe_release = _safe_release(release)
    lng_lo, lng_hi = peel_lng_range(peel_idx, peel_width_deg)
    staging_root = staging_peel_dir(workdir, peel_idx)

    # Wipe any partial output from a prior interrupted run for this peel.
    # Cleanup-after-upload normally handles this, but resume from a crash
    # mid-pass1 could leave a half-built tree.
    if staging_root.exists():
        shutil.rmtree(staging_root, ignore_errors=True)
    staging_root.mkdir(parents=True, exist_ok=True)

    duckdb_tmp = duckdb_tmp_dir_peel(workdir, peel_idx)
    duckdb_tmp.mkdir(parents=True, exist_ok=True)

    counters: dict = {
        "peel.idx": peel_idx,
        "release": safe_release,
        "themes_rows": {},
        "themes_buckets": {},
    }

    con = new_con(
        internal_threads=internal_threads,
        memory_limit=memory_limit,
        temp_dir=duckdb_tmp,
    )
    try:
        _configure_httpfs(con)

        # Phase 1: per-theme S3 → local partition
        for theme in THEMES:
            t0 = time.time()
            out_dir = per_theme_partition_dir_peel(workdir, peel_idx, theme)
            with phase_span(f"pass1_per_peel_theme_{theme}", peel_idx=peel_idx, cycle=cycle) as p:
                n_buckets = _partition_one_theme_from_s3(
                    con, theme, safe_release, lng_lo, lng_hi, out_dir,
                )
                p["theme"] = theme
                p["n_buckets"] = n_buckets
            counters["themes_buckets"][theme] = n_buckets
            log_event(
                "tiler.pass1_per_peel_theme_done",
                component="pass1_per_peel",
                cycle=cycle,
                theme=theme,
                n_buckets=n_buckets,
                duration_sec=round(time.time() - t0, 2),
                **{"peel.idx": peel_idx},
            )

        # Phase 2: collate themes per bucket
        bucket_files = _list_buckets_for_peel(workdir, peel_idx)
        combined_rows = 0
        combined_root = combined_bucket_dir_peel(workdir, peel_idx)
        combined_root.mkdir(parents=True, exist_ok=True)
        for (x, y), files in bucket_files.items():
            n_rows = _combine_one_bucket(con, workdir, peel_idx, x, y, files)
            combined_rows += n_rows
        counters["combined_buckets"] = len(bucket_files)
        counters["combined_rows"] = combined_rows
    finally:
        try:
            con.close()
        except Exception:
            pass

    # Drop the per_theme intermediate — pass2 reads `combined/` only.
    for theme in THEMES:
        d = per_theme_partition_dir_peel(workdir, peel_idx, theme)
        if d.exists():
            shutil.rmtree(d, ignore_errors=True)

    (staging_root / DONE_MARKER).write_text(_iso_now(), encoding="utf-8")
    return counters


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--workdir", default=None)
    p.add_argument("--peel-idx", type=int, required=True, help="0..35 (with default peel_width_deg=10).")
    p.add_argument("--cycle", type=int, default=0)
    p.add_argument("--release", default=None,
                   help="Overture release tag (e.g. 2026-04-15.0). Falls back to OVERTURE_RELEASE env.")
    p.add_argument("--peel-width-deg", type=int, default=PEEL_WIDTH_DEG_DEFAULT)
    p.add_argument("--memory-limit", default=PREFETCH_MEMORY_LIMIT)
    p.add_argument("--internal-threads", type=int, default=PREFETCH_INTERNAL_THREADS)
    args = p.parse_args()

    workdir = resolve_workdir(args.workdir)
    release = args.release or os.environ.get("OVERTURE_RELEASE")
    if not release:
        print("error: --release or OVERTURE_RELEASE env var required", file=sys.stderr)
        return 2

    init_sentry("pass1_per_peel")

    t0 = time.time()
    print(
        f"v13 pass1-per-peel: workdir={workdir} peel={args.peel_idx} "
        f"({peel_dir_name(args.peel_idx)}) release={release}",
        flush=True,
    )
    counters = run_one_peel(
        workdir=workdir,
        peel_idx=args.peel_idx,
        release=release,
        cycle=args.cycle,
        peel_width_deg=args.peel_width_deg,
        memory_limit=args.memory_limit,
        internal_threads=args.internal_threads,
    )
    counters["duration_sec"] = round(time.time() - t0, 2)
    log_event(
        "tiler.pass1_per_peel_done",
        component="pass1_per_peel",
        cycle=args.cycle,
        **counters,
    )
    print(f"pass1-per-peel done in {counters['duration_sec']}s: {counters['combined_buckets']} buckets, "
          f"{counters['combined_rows']} rows", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
