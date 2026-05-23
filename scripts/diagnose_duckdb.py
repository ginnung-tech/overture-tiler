"""Three-step DuckDB perf diagnostic against one infrastructure parquet.

Goal: pinpoint which stage is the single-thread bottleneck — pure scan,
UNNEST cell expansion, or PARTITION_BY. We measure each in isolation.

Run from the overture-tiler dir:
    python diagnose_duckdb.py
"""
from __future__ import annotations

import os
import shutil
import sys
import time
from pathlib import Path

import duckdb

# SQL-safety helpers — see _sql_safety.py for the why.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from _sql_safety import q_int, q_mem_limit, q_path  # noqa: E402

INPUT = Path("D:/overture/raw/infrastructure/part-00000-7c2f2f04-8b08-5a77-bd65-35dc9bdcc463-c000.zstd.parquet")
OUT_DIR = Path("D:/overture/.diagnose")
THREADS = 10
MEM_LIMIT = "20GB"


def _new_con():
    con = duckdb.connect()
    con.execute(f"SET threads = {q_int(THREADS)}")
    con.execute(f"SET memory_limit = '{q_mem_limit(MEM_LIMIT)}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory = '{q_path(OUT_DIR / 'duckdb-temp')}'")
    return con


def cleanup():
    if OUT_DIR.exists():
        shutil.rmtree(OUT_DIR, ignore_errors=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "duckdb-temp").mkdir(parents=True, exist_ok=True)


def stage(name, fn):
    cleanup()
    print(f"\n=== {name} ===", flush=True)
    t0 = time.time()
    try:
        result = fn()
        elapsed = time.time() - t0
        print(f"  elapsed: {elapsed:.1f}s", flush=True)
        if result is not None:
            print(f"  result:  {result}", flush=True)
    except Exception as e:
        elapsed = time.time() - t0
        print(f"  FAILED after {elapsed:.1f}s: {e}", flush=True)


def stage_a_count_only():
    """Pure scan: COUNT(*). No projection, no expansion. Tests parquet-decode speed."""
    con = _new_con()
    try:
        row = con.execute(f"SELECT COUNT(*) FROM read_parquet('{q_path(INPUT)}')").fetchone()
        return f"{row[0]:,} rows"
    finally:
        con.close()


def stage_b_scan_to_parquet():
    """Pure scan + write: COPY full file to one parquet. Tests scan + write throughput, no UNNEST/partitioning."""
    out = OUT_DIR / "stage_b.parquet"
    con = _new_con()
    try:
        con.execute(f"COPY (SELECT * FROM read_parquet('{q_path(INPUT)}') WHERE bbox IS NOT NULL) TO '{q_path(out)}' (FORMAT 'parquet', COMPRESSION 'gzip')")
        return f"wrote {out.stat().st_size/1e6:.1f} MB"
    finally:
        con.close()


def stage_c_with_cell_bounds():
    """Scan + add cell-bounds columns (no UNNEST). Tests added math overhead."""
    out = OUT_DIR / "stage_c.parquet"
    con = _new_con()
    try:
        sql = f"""
            COPY (
                SELECT
                    *,
                    GREATEST(CAST(floor((bbox.xmin + 180) / 0.1) AS INTEGER), 0) AS _lon_lo,
                    LEAST(CAST(floor((bbox.xmax + 180) / 0.1) AS INTEGER), 3599) AS _lon_hi,
                    GREATEST(CAST(floor((bbox.ymin +  90) / 0.1) AS INTEGER), 0) AS _lat_lo,
                    LEAST(CAST(floor((bbox.ymax +  90) / 0.1) AS INTEGER), 1799) AS _lat_hi
                FROM read_parquet('{q_path(INPUT)}')
                WHERE bbox IS NOT NULL
            ) TO '{q_path(out)}' (FORMAT 'parquet', COMPRESSION 'gzip')
        """
        con.execute(sql)
        return f"wrote {out.stat().st_size/1e6:.1f} MB"
    finally:
        con.close()


def stage_d_with_unnest_no_partition():
    """Scan + cell-bounds + UNNEST cross-join. Tests UNNEST throughput in isolation."""
    out = OUT_DIR / "stage_d.parquet"
    con = _new_con()
    try:
        sql = f"""
            COPY (
                WITH base AS (
                    SELECT
                        *,
                        GREATEST(CAST(floor((bbox.xmin + 180) / 0.1) AS INTEGER), 0) AS _lon_lo,
                        LEAST(CAST(floor((bbox.xmax + 180) / 0.1) AS INTEGER), 3599) AS _lon_hi,
                        GREATEST(CAST(floor((bbox.ymin +  90) / 0.1) AS INTEGER), 0) AS _lat_lo,
                        LEAST(CAST(floor((bbox.ymax +  90) / 0.1) AS INTEGER), 1799) AS _lat_hi
                    FROM read_parquet('{q_path(INPUT)}')
                    WHERE bbox IS NOT NULL
                ),
                filtered AS (
                    SELECT * FROM base
                    WHERE (_lon_hi - _lon_lo) <= 200 AND (_lat_hi - _lat_lo) <= 200
                )
                SELECT * EXCLUDE (_lon_lo, _lon_hi, _lat_lo, _lat_hi),
                       cell_lon, cell_lat
                FROM filtered,
                     UNNEST(range(filtered._lon_lo, filtered._lon_hi + 1)) AS lon_t(cell_lon),
                     UNNEST(range(filtered._lat_lo, filtered._lat_hi + 1)) AS lat_t(cell_lat)
            ) TO '{q_path(out)}' (FORMAT 'parquet', COMPRESSION 'gzip')
        """
        con.execute(sql)
        return f"wrote {out.stat().st_size/1e6:.1f} MB"
    finally:
        con.close()


def stage_e_full_partition_by():
    """The full v9 pipeline: scan + cell-bounds + UNNEST + PARTITION_BY. Reproduces production."""
    out = OUT_DIR / "stage_e_partitions"
    out.mkdir(parents=True, exist_ok=True)
    con = _new_con()
    try:
        sql = f"""
            COPY (
                WITH base AS (
                    SELECT
                        *,
                        GREATEST(CAST(floor((bbox.xmin + 180) / 0.1) AS INTEGER), 0) AS _lon_lo,
                        LEAST(CAST(floor((bbox.xmax + 180) / 0.1) AS INTEGER), 3599) AS _lon_hi,
                        GREATEST(CAST(floor((bbox.ymin +  90) / 0.1) AS INTEGER), 0) AS _lat_lo,
                        LEAST(CAST(floor((bbox.ymax +  90) / 0.1) AS INTEGER), 1799) AS _lat_hi
                    FROM read_parquet('{q_path(INPUT)}')
                    WHERE bbox IS NOT NULL
                ),
                filtered AS (
                    SELECT * FROM base
                    WHERE (_lon_hi - _lon_lo) <= 200 AND (_lat_hi - _lat_lo) <= 200
                )
                SELECT * EXCLUDE (_lon_lo, _lon_hi, _lat_lo, _lat_hi),
                       cell_lon, cell_lat
                FROM filtered,
                     UNNEST(range(filtered._lon_lo, filtered._lon_hi + 1)) AS lon_t(cell_lon),
                     UNNEST(range(filtered._lat_lo, filtered._lat_hi + 1)) AS lat_t(cell_lat)
            ) TO '{q_path(out)}' (FORMAT 'parquet', PARTITION_BY (cell_lon, cell_lat), COMPRESSION 'gzip', OVERWRITE_OR_IGNORE TRUE)
        """
        con.execute(sql)
        # Count partition dirs.
        n_lon_dirs = sum(1 for p in out.iterdir() if p.is_dir() and p.name.startswith("cell_lon="))
        n_files = sum(1 for _ in out.rglob("*.parquet"))
        return f"{n_lon_dirs} cell_lon dirs, {n_files} parquet fragments"
    finally:
        con.close()


if __name__ == "__main__":
    print(f"Input:   {INPUT}  ({INPUT.stat().st_size/1e6:.0f} MB)")
    print(f"Threads: {THREADS}")
    print(f"Memory:  {MEM_LIMIT}")
    print(f"Spill:   {OUT_DIR / 'duckdb-temp'}")

    stage("A: COUNT(*) only (pure scan)", stage_a_count_only)
    stage("B: COPY full file to one parquet (scan + write)", stage_b_scan_to_parquet)
    stage("C: + cell-bounds columns (math, no UNNEST)", stage_c_with_cell_bounds)
    stage("D: + UNNEST cell expansion (NO partitioning)", stage_d_with_unnest_no_partition)
    stage("E: + PARTITION_BY (full v9 pipeline)", stage_e_full_partition_by)

    cleanup()
    print("\nDone.", flush=True)
