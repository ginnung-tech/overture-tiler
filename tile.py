"""
Overture Maps tiler — phase 2 of the two-phase pipeline.

Reads raw Parquet files from a local workdir (populated by download.py) and
tiles them into <=20 MB gzipped Parquet files using an adaptive quadtree.

File-first architecture (v8):

The previous cell-first design opened each input parquet ~once per overlapping
cell. For dense themes (buildings: 914K non-empty cells × ~10 files per cell ≈
9M file opens), per-cell decompression overhead dominated — each COPY had to
decompress whole row groups (Overture's row groups are 100K-1M rows wide) just
to extract a tiny output. Result: 12% CPU utilization, ~3 cell/s aggregate,
3-day ETA on buildings.

The file-first approach uses DuckDB's COPY ... PARTITION_BY to read each input
parquet ONCE and write per-cell partial parquets in a single query.

Pass 1 — file partitioning (parallel across input files):
    For each input parquet, expand bbox-spanning features into one row per
    overlapping 0.1° cell, then COPY ... PARTITION_BY (cell_lon, cell_lat) to:
        <workdir>/.tile-staging/<theme>/<file_hash>/cell_lon=X/cell_lat=Y/
            data_0.parquet.gz
    Each input file is opened exactly once. N parallel workers each process
    a slice of the file list with their own DuckDB connection.

Pass 2 — per-cell merge (parallel across cells):
    Enumerate cells from the staging directory layout (no separate enumeration
    pass needed — the directory structure IS the cell set). For each cell,
    read all partial parquets contributing to it, drop the synthetic
    cell_lon/cell_lat columns, and COPY to:
        <workdir>/tiles/<theme>/<z>_<x>_<y>.parquet.gz
    If the merged tile exceeds MAX_TILE_BYTES and we're above MIN_CELL_DEG,
    materialize the cell-scoped data into a per-thread TEMP TABLE and recurse
    via the existing in-memory subdivision pattern. No input parquet is ever
    re-opened during recursion.

After Pass 2 the staging directory is removed (gate behind --keep-staging
to retain it for debugging).

Input layout:
    <workdir>/raw/<theme>/**/*.parquet  — downloaded by download.py

Staging layout (transient):
    <workdir>/.tile-staging/<theme>/<file_hash>/cell_lon=X/cell_lat=Y/*.parquet

Output layout:
    <workdir>/tiles/<theme>/<z>_<x>_<y>.parquet.gz

Checkpoint file:
    <workdir>/tiles/<theme>/_tiles.json
    One entry per visited cell:
        {
            "z": 0, "x": 3, "y": 7,
            "status": "done" | "empty" | "failed" | "in_progress",
            "feature_count": 1234,
            "size_bytes": 1048576,
            "sha256": "abc123...",   -- populated on write
            "at": "2026-05-03T14:32:00Z"
        }
    Resumable: re-running skips "done" and "empty" cells, retries "failed"
    and "in_progress". Pass 1 also resumes per-file (skip if staging dir for
    that file_hash already has output).

Manifest (global index):
    <workdir>/manifest.json
    Written at the end of the tile phase, derived from _tiles.json entries
    with status="done" (empty cells excluded). One row per tile:
        {theme, z, x, y, bbox, size_bytes, feature_count}

Usage:
    python tile.py --theme buildings [--workdir D:/overture] [--threads N]
                   [--bbox MINLON,MINLAT,MAXLON,MAXLAT] [--dry-run]
                   [--keep-staging]

Workdir resolution order:
    1. --workdir CLI flag
    2. OVERTURE_WORKDIR env var
    3. D:\\overture (Windows) or /Volumes/SSD/overture (Mac)

Verified file counts + sizes for release 2026-04-15.0 (probed 2026-05-03):
    buildings      (theme=buildings/type=building):         512 files, ~269 GB
    segments       (theme=transportation/type=segment):     128 files,  ~60 GB
    land_use       (theme=base/type=land_use):               32 files,  ~20 GB
    water          (theme=base/type=water):                  32 files,  ~53 GB
    land           (theme=base/type=land):                   32 files,  ~37 GB
    infrastructure (theme=base/type=infrastructure):         16 files,  ~13 GB

Disk-space note: Pass 1 staging can grow to roughly the input theme size
(buildings ~270 GB). The script prints a warning at startup; ensure the
workdir drive has at least theme_size + 50 GB free before kicking off
buildings.

Dependencies: duckdb (with spatial extension), Python 3.11+, gzip, json (stdlib)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

# SQL-safety helpers — see _sql_safety.py for the why. Every DuckDB SQL
# fragment built via f-string in this module routes its interpolated values
# through one of these whitelist-validators.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from _sql_safety import q_float, q_int, q_mem_limit, q_path, q_path_list  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OVERTURE_RELEASE = os.environ.get("OVERTURE_RELEASE", "2026-04-15.0")

# Quadtree parameters (degrees)
START_CELL_DEG: float = 0.1      # ~10 km initial grid
MIN_CELL_DEG: float = 0.001     # ~100 m minimum (accept oversize below this)
MAX_TILE_BYTES: int = 20 * 1024 * 1024  # 20 MB gzipped

# World bounds
WORLD_BOUNDS = (-180.0, -90.0, 180.0, 90.0)

ALL_THEMES = ["buildings", "segments", "land_use", "water", "land", "infrastructure"]

TILES_CHECKPOINT_FILENAME = "_tiles.json"
CHECKPOINT_SAVE_INTERVAL = 200  # save checkpoint every N completed cells (across all threads)

# Per-rectangle clamp: any feature bbox spanning >200 cells per dimension
# (20° × 20° at 0.1°) is dropped during Pass 1. Almost certainly a degenerate
# or planet-aggregated record; expanding it cross-joins to a useless mass of
# cells.
MAX_RECT_SPAN = 200

# Approximate input theme sizes for the disk-space warning.
THEME_SIZE_GB = {
    "buildings": 270,
    "segments": 60,
    "water": 53,
    "land": 37,
    "land_use": 20,
    "infrastructure": 13,
}


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Bbox:
    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float

    def area_deg(self) -> float:
        return (self.max_lon - self.min_lon) * (self.max_lat - self.min_lat)

    def quadrants(self) -> list["Bbox"]:
        """Split into 4 equal quadrants (SW, SE, NW, NE)."""
        mid_lon = (self.min_lon + self.max_lon) / 2
        mid_lat = (self.min_lat + self.max_lat) / 2
        return [
            Bbox(self.min_lon, self.min_lat, mid_lon, mid_lat),   # SW
            Bbox(mid_lon, self.min_lat, self.max_lon, mid_lat),   # SE
            Bbox(self.min_lon, mid_lat, mid_lon, self.max_lat),   # NW
            Bbox(mid_lon, mid_lat, self.max_lon, self.max_lat),   # NE
        ]

    def width_deg(self) -> float:
        return self.max_lon - self.min_lon

    def as_list(self) -> list[float]:
        return [self.min_lon, self.min_lat, self.max_lon, self.max_lat]


@dataclass
class TileAddress:
    """Quadtree address: z = depth (0 = root), x/y = column/row at that depth.

    Index convention (preserved from prior tile.py / tiler.py):
        y indexes longitude (0..3600 across the world at z=0)
        x indexes latitude  (0..1800 across the world at z=0)
    """
    z: int
    x: int
    y: int
    bbox: Bbox


@dataclass
class TileRecord:
    theme: str
    z: int
    x: int
    y: int
    bbox: list[float]
    size_bytes: int
    feature_count: int


# ---------------------------------------------------------------------------
# Cell <-> address mapping
# ---------------------------------------------------------------------------

def cell_to_address(cell_lon: int, cell_lat: int) -> TileAddress:
    """Map (cell_lon, cell_lat) integer indices back to a z=0 TileAddress.

    PARTITION_BY emits cell_lon (longitude index) and cell_lat (latitude
    index). Our pre-existing TileAddress convention is reversed: x = latitude
    index, y = longitude index. Map them through here.
    """
    lon = WORLD_BOUNDS[0] + cell_lon * START_CELL_DEG
    lat = WORLD_BOUNDS[1] + cell_lat * START_CELL_DEG
    return TileAddress(
        z=0,
        x=cell_lat,
        y=cell_lon,
        bbox=Bbox(
            min_lon=lon,
            min_lat=lat,
            max_lon=min(lon + START_CELL_DEG, WORLD_BOUNDS[2]),
            max_lat=min(lat + START_CELL_DEG, WORLD_BOUNDS[3]),
        ),
    )


def child_address(parent: TileAddress, child_bbox: Bbox, child_index: int) -> TileAddress:
    """
    Compute the z/x/y for a child quadrant.

    Child index layout (same order as Bbox.quadrants()):
        0 = SW, 1 = SE, 2 = NW, 3 = NE
    """
    x_offset = 1 if child_index in (1, 3) else 0  # east quadrants
    y_offset = 1 if child_index in (2, 3) else 0  # north quadrants
    return TileAddress(
        z=parent.z + 1,
        x=parent.x * 2 + x_offset,
        y=parent.y * 2 + y_offset,
        bbox=child_bbox,
    )


# ---------------------------------------------------------------------------
# DuckDB helpers
# ---------------------------------------------------------------------------

_PER_CONNECTION_MEMORY_LIMIT = "4GB"        # Pass 2 worker connections (10 × 4 = 40 GB worst case, staggered)
_PASS1_FILE_MEMORY_LIMIT = "28GB"           # Pass 1: ONE connection per file, near full physical (~32 GB)
_PASS1_5_MEMORY_LIMIT = "40GB"              # Pass 1.5: bigger to let PARTITION_BY accumulate ~600 partitions before flush;
                                            # overflow spills to Windows page file on C: SSD (acceptable)
_DUCKDB_TEMP_DIR: Path | None = None


def _new_con(internal_threads: int = 1, memory_limit: str | None = None):
    """New DuckDB connection with spatial extension loaded.

    DuckDB defaults to N_CPUS internal threads PER connection. With our pool
    of N worker threads each holding their own connection, the default would
    spawn N × N_CPUS threads — wild over-subscription that thrashes the cache
    and starves the CPU. Pin to 1 internal thread per worker connection so
    total = N workers; the main-thread connection (one-off ops only) can use
    a higher count by passing `internal_threads=N` explicitly.

    DuckDB also defaults to ~80% of system RAM PER connection; with N
    connections that's N × 80% — far over physical RAM, which sends Windows
    into page-thrash and starves CPU. memory_limit caps each connection so
    the total stays under physical. Spill goes to D:\\ to keep system drive
    free. Without these, Pass 1 with PARTITION_BY (which buffers rows per
    open partition file) commits 80+ GB and crawls.
    """
    import duckdb  # type: ignore
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(f"SET threads = {q_int(internal_threads)}")
    con.execute(f"SET memory_limit = '{q_mem_limit(memory_limit or _PER_CONNECTION_MEMORY_LIMIT)}'")
    # CRITICAL: without this, COPY ... PARTITION_BY serializes the writer to
    # preserve row order, buffers all open partitions in memory until the
    # limit, and produces the "low CPU + cycling RAM" pattern (DuckDB's own
    # OOM error explicitly flags this as the fix).
    con.execute("SET preserve_insertion_order = false")
    if _DUCKDB_TEMP_DIR is not None:
        con.execute(f"SET temp_directory = '{q_path(_DUCKDB_TEMP_DIR)}'")
    return con


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Pass 1 — file-first partitioning
# ---------------------------------------------------------------------------

def file_hash(path: Path) -> str:
    """Stable short hash of the file's posix path. Used as staging subdir."""
    return hashlib.sha1(path.as_posix().encode("utf-8")).hexdigest()[:12]


def staging_dir_for_theme(workdir: Path, theme: str) -> Path:
    return workdir / ".tile-staging" / theme


def staging_dir_for_file(workdir: Path, theme: str, parquet_path: Path) -> Path:
    return staging_dir_for_theme(workdir, theme) / file_hash(parquet_path)


_PASS1_MARKER = "_DONE"
_PASS1_FILE_MANIFEST = "_manifest.json"


_INTERMEDIATE_FILENAME = "intermediate.parquet"
_COARSE_DIR_NAME = "_coarse"            # one subdir under staging/<theme>/, holds Pass 1.5 output
_COARSE_DONE_MARKER = "_DONE"
_FINE_PER_COARSE_SIDE = 10               # 1° coarse = 10 × 0.1° fine cells per side


def _create_intermediate_for_file(
    parquet_path: Path,
    out_dir: Path,
    region_bbox: Bbox | None,
    n_threads: int,
) -> tuple[bool, str | None]:
    """Pass 1 (v10, no-UNNEST): write ONE intermediate parquet per input file.

    Why no UNNEST: cross-joining each row to all cells it touches via
    `UNNEST(range(...))` is single-threaded in DuckDB and dominates wall time
    (10× slower than the rest of the pipeline combined — verified by
    diagnose_duckdb.py).

    Instead we keep each row ONCE plus its cell-bound integers
    (`_lon_lo, _lon_hi, _lat_lo, _lat_hi`). Pass 2 does the cell expansion
    via per-cell range-scan queries — DuckDB row-group statistics make those
    queries skip most of the file when the cell is empty or far from this
    file's data.

    The output is sorted by (_lat_lo, _lon_lo) so row-group stats line up
    with cell coordinates and pruning works.
    """
    if out_dir.exists():
        for child in out_dir.iterdir():
            try:
                if child.is_dir():
                    shutil.rmtree(child)
                else:
                    child.unlink()
            except Exception:
                pass
    out_dir.mkdir(parents=True, exist_ok=True)

    n_lon = round((WORLD_BOUNDS[2] - WORLD_BOUNDS[0]) / START_CELL_DEG)  # 3600
    n_lat = round((WORLD_BOUNDS[3] - WORLD_BOUNDS[1]) / START_CELL_DEG)  # 1800

    region_filter = ""
    if region_bbox is not None:
        region_filter = (
            f"AND bbox.xmax > {q_float(region_bbox.min_lon)} "
            f"AND bbox.xmin < {q_float(region_bbox.max_lon)} "
            f"AND bbox.ymax > {q_float(region_bbox.min_lat)} "
            f"AND bbox.ymin < {q_float(region_bbox.max_lat)}"
        )

    intermediate = out_dir / _INTERMEDIATE_FILENAME
    sql = f"""
        COPY (
            WITH base AS (
                SELECT
                    *,
                    GREATEST(CAST(floor((bbox.xmin + 180) / {q_float(START_CELL_DEG)}) AS INTEGER), 0) AS _lon_lo,
                    LEAST(CAST(floor((bbox.xmax + 180) / {q_float(START_CELL_DEG)}) AS INTEGER), {q_int(n_lon - 1)}) AS _lon_hi,
                    GREATEST(CAST(floor((bbox.ymin +  90) / {q_float(START_CELL_DEG)}) AS INTEGER), 0) AS _lat_lo,
                    LEAST(CAST(floor((bbox.ymax +  90) / {q_float(START_CELL_DEG)}) AS INTEGER), {q_int(n_lat - 1)}) AS _lat_hi
                FROM read_parquet('{q_path(parquet_path)}')
                WHERE bbox IS NOT NULL
                  {region_filter}
            )
            SELECT *
            FROM base
            WHERE (_lon_hi - _lon_lo) <= {q_int(MAX_RECT_SPAN)}
              AND (_lat_hi - _lat_lo) <= {q_int(MAX_RECT_SPAN)}
            ORDER BY _lat_lo, _lon_lo
        ) TO '{q_path(intermediate)}' (FORMAT 'parquet', COMPRESSION 'gzip')
    """

    con = _new_con(internal_threads=n_threads, memory_limit=_PASS1_FILE_MEMORY_LIMIT)
    try:
        con.execute(sql)
    except Exception as e:
        return False, str(e)
    finally:
        try:
            con.close()
        except Exception:
            pass

    if not intermediate.exists():
        return False, "intermediate not produced"

    # Read back bbox extent + row count from the intermediate's column stats.
    stat_con = _new_con(internal_threads=max(1, n_threads // 2), memory_limit=_PASS1_FILE_MEMORY_LIMIT)
    try:
        row = stat_con.execute(f"""
            SELECT
                COUNT(*),
                MIN(_lon_lo), MAX(_lon_hi),
                MIN(_lat_lo), MAX(_lat_hi)
            FROM read_parquet('{q_path(intermediate)}')
        """).fetchone()
    finally:
        try:
            stat_con.close()
        except Exception:
            pass

    if row is None or row[0] == 0:
        manifest = {
            "intermediate": intermediate.as_posix(),
            "rows": 0,
            "lon_lo_min": None, "lon_hi_max": None,
            "lat_lo_min": None, "lat_hi_max": None,
            "bytes": intermediate.stat().st_size if intermediate.exists() else 0,
        }
    else:
        manifest = {
            "intermediate": intermediate.as_posix(),
            "rows": int(row[0]),
            "lon_lo_min": int(row[1]),
            "lon_hi_max": int(row[2]),
            "lat_lo_min": int(row[3]),
            "lat_hi_max": int(row[4]),
            "bytes": intermediate.stat().st_size,
        }
    (out_dir / _PASS1_FILE_MANIFEST).write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    (out_dir / _PASS1_MARKER).write_text(_iso_now(), encoding="utf-8")
    return True, None


def coarse_dir_for_theme(workdir: Path, theme: str) -> Path:
    return staging_dir_for_theme(workdir, theme) / _COARSE_DIR_NAME


def _coarse_partition_done(coarse_dir: Path) -> bool:
    return (coarse_dir / _COARSE_DONE_MARKER).exists()


def run_pass1_5_coarse_partition(
    intermediate_paths: list[str],
    coarse_dir: Path,
    n_threads: int,
    region_bbox: Bbox | None,
) -> tuple[bool, str | None, int]:
    """Pass 1.5: merge all intermediates and PARTITION_BY into 1° coarse cells.

    This is the ONE place we pay the UNNEST cost — single-threaded, but the
    UNNEST factor is small (1° grid means ~1-3 coarse cells per row vs ~5-50
    for fine cells, ~10x less expansion). preserve_insertion_order=false
    unlocks the parallel writers; ~600 partitions globally for infrastructure
    is well within DuckDB's PARTITION_BY sweet spot.

    Output layout:
        <coarse_dir>/coarse_lon=X/coarse_lat=Y/data_*.parquet

    Pass 2 reads ONE such bucket per fine cell — small data per query.
    """
    if coarse_dir.exists():
        for child in coarse_dir.iterdir():
            try:
                if child.is_dir():
                    shutil.rmtree(child)
                else:
                    child.unlink()
            except Exception:
                pass
    coarse_dir.mkdir(parents=True, exist_ok=True)

    files_lit = _files_sql(intermediate_paths)
    region_filter = ""
    if region_bbox is not None:
        rb_lon_lo = int((region_bbox.min_lon + 180) / START_CELL_DEG)
        rb_lon_hi = int((region_bbox.max_lon + 180) / START_CELL_DEG)
        rb_lat_lo = int((region_bbox.min_lat + 90) / START_CELL_DEG)
        rb_lat_hi = int((region_bbox.max_lat + 90) / START_CELL_DEG)
        region_filter = (
            f"AND _lon_hi >= {q_int(rb_lon_lo)} AND _lon_lo <= {q_int(rb_lon_hi)} "
            f"AND _lat_hi >= {q_int(rb_lat_lo)} AND _lat_lo <= {q_int(rb_lat_hi)}"
        )

    # _lon_lo // 10 yields coarse-cell index; UNNEST(range(...)) cross-joins
    # each row with every coarse cell its bbox touches. Original cell-bound
    # columns are kept so Pass 2 can do a tight per-fine-cell range filter.
    sql = f"""
        COPY (
            WITH base AS (
                SELECT *,
                    _lon_lo // {q_int(_FINE_PER_COARSE_SIDE)} AS _coarse_lon_lo,
                    _lon_hi // {q_int(_FINE_PER_COARSE_SIDE)} AS _coarse_lon_hi,
                    _lat_lo // {q_int(_FINE_PER_COARSE_SIDE)} AS _coarse_lat_lo,
                    _lat_hi // {q_int(_FINE_PER_COARSE_SIDE)} AS _coarse_lat_hi
                FROM read_parquet({files_lit}, union_by_name=True)
                WHERE 1=1 {region_filter}
            )
            SELECT * EXCLUDE (_coarse_lon_lo, _coarse_lon_hi, _coarse_lat_lo, _coarse_lat_hi),
                coarse_lon, coarse_lat
            FROM base,
                 UNNEST(range(base._coarse_lon_lo, base._coarse_lon_hi + 1)) AS lon_t(coarse_lon),
                 UNNEST(range(base._coarse_lat_lo, base._coarse_lat_hi + 1)) AS lat_t(coarse_lat)
        ) TO '{q_path(coarse_dir)}' (
            FORMAT 'parquet',
            PARTITION_BY (coarse_lon, coarse_lat),
            COMPRESSION 'gzip',
            OVERWRITE_OR_IGNORE TRUE
        )
    """

    con = _new_con(internal_threads=n_threads, memory_limit=_PASS1_5_MEMORY_LIMIT)
    try:
        con.execute(sql)
    except Exception as e:
        return False, str(e), 0
    finally:
        try:
            con.close()
        except Exception:
            pass

    # Walk to count coarse buckets actually produced.
    coarse_count = 0
    for lon_dir in coarse_dir.iterdir():
        if not lon_dir.is_dir() or not lon_dir.name.startswith("coarse_lon="):
            continue
        for lat_dir in lon_dir.iterdir():
            if lat_dir.is_dir() and lat_dir.name.startswith("coarse_lat="):
                coarse_count += 1

    (coarse_dir / _COARSE_DONE_MARKER).write_text(_iso_now(), encoding="utf-8")
    return True, None, coarse_count


def list_nonempty_coarse_cells(coarse_dir: Path) -> list[tuple[int, int]]:
    """Walk <coarse_dir>/coarse_lon=X/coarse_lat=Y/ and yield non-empty buckets."""
    cells: list[tuple[int, int]] = []
    if not coarse_dir.exists():
        return cells
    for lon_dir in coarse_dir.iterdir():
        if not lon_dir.is_dir() or not lon_dir.name.startswith("coarse_lon="):
            continue
        try:
            cl = int(lon_dir.name.split("=", 1)[1])
        except ValueError:
            continue
        for lat_dir in lon_dir.iterdir():
            if not lat_dir.is_dir() or not lat_dir.name.startswith("coarse_lat="):
                continue
            try:
                ca = int(lat_dir.name.split("=", 1)[1])
            except ValueError:
                continue
            if any(p.suffix == ".parquet" for p in lat_dir.iterdir()):
                cells.append((cl, ca))
    return sorted(cells)


def coarse_bucket_paths(coarse_dir: Path, coarse_lon: int, coarse_lat: int) -> list[str]:
    bucket = coarse_dir / f"coarse_lon={coarse_lon}" / f"coarse_lat={coarse_lat}"
    if not bucket.exists():
        return []
    return [p.as_posix() for p in bucket.iterdir() if p.suffix == ".parquet"]


def _file_already_partitioned(out_dir: Path) -> bool:
    """True if Pass 1 finished successfully for this input file.

    A "_DONE" marker file is written after the COPY completes. Without it we
    treat the partition as incomplete (left over from a crash) and re-run.
    """
    return (out_dir / _PASS1_MARKER).exists()


def run_pass1(
    parquet_files: list[Path],
    theme: str,
    workdir: Path,
    region_bbox: Bbox | None,
    n_workers: int,
) -> int:
    """Pass 1: partition every input parquet into per-cell partials.

    Outer loop is SERIAL across files — each file is already parallelized
    internally (one DuckDB scan into a shared Arrow table, then n_workers
    band-slice workers running UNNEST + COPY ... PARTITION_BY in parallel).
    Going parallel on the outer loop on top of that would multiply RAM
    pressure (each file = its own Arrow buffer + N writer fan-outs) for no
    extra throughput.

    Returns count of files successfully partitioned (or skipped as already
    done). Failures are printed but don't abort the whole pass.
    """
    print(
        f"Pass 1 (v10): writing intermediate parquet for {len(parquet_files)} input files "
        f"(serial across files; {n_workers} DuckDB internal threads per file; no UNNEST)...",
        flush=True,
    )
    t0 = time.time()

    ok_count = 0
    failures = 0
    skipped = 0

    for idx, parquet_path in enumerate(parquet_files, start=1):
        out_dir = staging_dir_for_file(workdir, theme, parquet_path)
        if _file_already_partitioned(out_dir):
            skipped += 1
            ok_count += 1
        else:
            t_file = time.time()
            ok, err = _create_intermediate_for_file(parquet_path, out_dir, region_bbox, n_workers)
            file_elapsed = time.time() - t_file
            if ok:
                ok_count += 1
                print(
                    f"  [{idx}/{len(parquet_files)}] OK   {parquet_path.name} "
                    f"in {file_elapsed:.1f}s",
                    flush=True,
                )
            else:
                failures += 1
                print(
                    f"  [{idx}/{len(parquet_files)}] FAIL {parquet_path.name}: {err}",
                    flush=True,
                )

        if idx % 4 == 0 or idx == len(parquet_files):
            print(
                f"  Pass 1 progress: {idx}/{len(parquet_files)} "
                f"(skipped {skipped} resumed, ok {ok_count}, fail {failures}) "
                f"  {time.time()-t0:.1f}s elapsed",
                flush=True,
            )

    elapsed = time.time() - t0
    print(
        f"Pass 1 done: {ok_count} ok, {failures} failed, "
        f"{skipped} resumed in {elapsed/60:.1f} min",
        flush=True,
    )
    return ok_count


# ---------------------------------------------------------------------------
# Pass 2 — per-cell merge
# ---------------------------------------------------------------------------

@dataclass
class IntermediateSet:
    """Pointers to all per-file intermediates produced by Pass 1."""
    paths: list[str]
    total_rows: int
    total_bytes: int
    # Global cell-bound extent — Pass 2 iterates candidate cells inside this.
    lon_lo_min: int
    lon_hi_max: int
    lat_lo_min: int
    lat_hi_max: int


def collect_intermediates_and_extent(
    workdir: Path,
    theme: str,
) -> IntermediateSet | None:
    """Read every per-file _manifest.json. Return aggregate intermediates + extent."""
    staging = staging_dir_for_theme(workdir, theme)
    if not staging.exists():
        return None

    paths: list[str] = []
    total_rows = 0
    total_bytes = 0
    lon_lo_min: int | None = None
    lon_hi_max: int | None = None
    lat_lo_min: int | None = None
    lat_hi_max: int | None = None

    for file_subdir in sorted(staging.iterdir()):
        if not file_subdir.is_dir():
            continue
        manifest_path = file_subdir / _PASS1_FILE_MANIFEST
        if not manifest_path.exists():
            print(
                f"  [warn] {file_subdir.name}: no _manifest.json — skipping (Pass 1 didn't finish?)",
                flush=True,
            )
            continue
        try:
            m = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  [warn] manifest read failed for {file_subdir.name}: {e}", flush=True)
            continue
        if m.get("rows", 0) == 0:
            continue
        intermediate_path = m.get("intermediate")
        if not intermediate_path or not Path(intermediate_path).exists():
            print(f"  [warn] {file_subdir.name}: intermediate missing", flush=True)
            continue
        paths.append(intermediate_path)
        total_rows += int(m.get("rows", 0))
        total_bytes += int(m.get("bytes", 0))
        for key, var in (
            ("lon_lo_min", "lon_lo_min"),
            ("lon_hi_max", "lon_hi_max"),
            ("lat_lo_min", "lat_lo_min"),
            ("lat_hi_max", "lat_hi_max"),
        ):
            v = m.get(key)
            if v is None:
                continue
        lon_lo_min = m["lon_lo_min"] if lon_lo_min is None else min(lon_lo_min, m["lon_lo_min"])
        lon_hi_max = m["lon_hi_max"] if lon_hi_max is None else max(lon_hi_max, m["lon_hi_max"])
        lat_lo_min = m["lat_lo_min"] if lat_lo_min is None else min(lat_lo_min, m["lat_lo_min"])
        lat_hi_max = m["lat_hi_max"] if lat_hi_max is None else max(lat_hi_max, m["lat_hi_max"])

    if not paths:
        return None
    return IntermediateSet(
        paths=paths,
        total_rows=total_rows,
        total_bytes=total_bytes,
        lon_lo_min=lon_lo_min if lon_lo_min is not None else 0,
        lon_hi_max=lon_hi_max if lon_hi_max is not None else 3599,
        lat_lo_min=lat_lo_min if lat_lo_min is not None else 0,
        lat_hi_max=lat_hi_max if lat_hi_max is not None else 1799,
    )


def fine_cells_for_coarse(coarse_lon: int, coarse_lat: int, region_bbox: Bbox | None) -> list[tuple[int, int]]:
    """Generate the 100 fine sub-cells inside one 1° coarse cell (clipped to region)."""
    fine_lon_lo = coarse_lon * _FINE_PER_COARSE_SIDE
    fine_lon_hi = fine_lon_lo + _FINE_PER_COARSE_SIDE - 1
    fine_lat_lo = coarse_lat * _FINE_PER_COARSE_SIDE
    fine_lat_hi = fine_lat_lo + _FINE_PER_COARSE_SIDE - 1
    if region_bbox is not None:
        rb_lon_lo = int((region_bbox.min_lon + 180) / START_CELL_DEG)
        rb_lon_hi = int((region_bbox.max_lon + 180) / START_CELL_DEG)
        rb_lat_lo = int((region_bbox.min_lat + 90) / START_CELL_DEG)
        rb_lat_hi = int((region_bbox.max_lat + 90) / START_CELL_DEG)
        fine_lon_lo = max(fine_lon_lo, rb_lon_lo)
        fine_lon_hi = min(fine_lon_hi, rb_lon_hi)
        fine_lat_lo = max(fine_lat_lo, rb_lat_lo)
        fine_lat_hi = min(fine_lat_hi, rb_lat_hi)
    return [
        (cl, ca)
        for cl in range(fine_lon_lo, fine_lon_hi + 1)
        for ca in range(fine_lat_lo, fine_lat_hi + 1)
    ]


def _files_sql(files: list[str]) -> str:
    """DuckDB list-of-files literal: ['/p/a.parquet','/p/b.parquet'].

    Each path is validated against q_path() before interpolation.
    """
    return q_path_list(files)


def write_tile_from_temp(
    con,
    bbox: Bbox,
    out_path: Path,
) -> tuple[int, int, str | None]:
    """COPY features from TEMP TABLE _cell clipped to bbox. Returns (count, size, sha256).

    Used at every recursion level — no input parquet re-read needed because
    _cell already holds the cell-scoped data.
    """
    tmp = out_path.with_suffix(".tmp")
    con.execute(f"""
        COPY (
            SELECT * EXCLUDE (cell_lon, cell_lat) FROM _cell
            WHERE bbox.xmin < {q_float(bbox.max_lon)}
              AND bbox.xmax > {q_float(bbox.min_lon)}
              AND bbox.ymin < {q_float(bbox.max_lat)}
              AND bbox.ymax > {q_float(bbox.min_lat)}
        ) TO '{q_path(tmp)}' (FORMAT 'parquet', COMPRESSION 'gzip')
    """)
    if not tmp.exists():
        return 0, 0, None
    size_bytes = tmp.stat().st_size
    if size_bytes == 0:
        tmp.unlink()
        return 0, 0, None
    count_row = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{q_path(tmp)}')"
    ).fetchone()
    feature_count = int(count_row[0]) if count_row else 0
    if feature_count == 0:
        tmp.unlink()
        return 0, 0, None
    sha256 = _sha256_file(tmp)
    tmp.rename(out_path)
    return feature_count, size_bytes, sha256


def write_tile_from_partials(
    con,
    partials: list[str],
    out_path: Path,
) -> tuple[int, int, str | None]:
    """Direct merge from partial parquets to a final tile.

    First-pass write that bypasses the TEMP TABLE for the dominant case where
    the cell fits in one tile. If oversize, the caller materializes _cell
    and recurses via write_tile_from_temp.
    """
    if not partials:
        return 0, 0, None
    tmp = out_path.with_suffix(".tmp")
    files_lit = _files_sql(partials)
    con.execute(f"""
        COPY (
            SELECT * EXCLUDE (cell_lon, cell_lat)
            FROM read_parquet({files_lit}, union_by_name=True)
        ) TO '{q_path(tmp)}' (FORMAT 'parquet', COMPRESSION 'gzip')
    """)
    if not tmp.exists():
        return 0, 0, None
    size_bytes = tmp.stat().st_size
    if size_bytes == 0:
        tmp.unlink()
        return 0, 0, None
    count_row = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{q_path(tmp)}')"
    ).fetchone()
    feature_count = int(count_row[0]) if count_row else 0
    if feature_count == 0:
        tmp.unlink()
        return 0, 0, None
    sha256 = _sha256_file(tmp)
    tmp.rename(out_path)
    return feature_count, size_bytes, sha256


def materialize_cell_from_partials(con, partials: list[str]) -> int:
    """Load all partial parquets for a cell into TEMP TABLE _cell.

    Includes the synthetic cell_lon/cell_lat columns; write_tile_from_temp
    drops them before writing.
    """
    files_lit = _files_sql(partials)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _cell AS
        SELECT * FROM read_parquet({files_lit}, union_by_name=True)
    """)
    row = con.execute("SELECT COUNT(*) FROM _cell").fetchone()
    return int(row[0]) if row else 0


# ---------------------------------------------------------------------------
# Pass 2 (v10) — query intermediates with per-cell range filter
# ---------------------------------------------------------------------------

def cell_count_from_intermediates(con, intermediates: list[str], cell_lon: int, cell_lat: int) -> int:
    """Cheap EXISTS-style probe via COUNT(*) on the row-group-pruned scan.

    The intermediates are sorted by (_lat_lo, _lon_lo) so DuckDB's row-group
    statistics let it skip groups that can't contain rows overlapping the
    cell. Empty cells return in microseconds.
    """
    files_lit = _files_sql(intermediates)
    cl, ca = q_int(cell_lon), q_int(cell_lat)
    row = con.execute(f"""
        SELECT COUNT(*)
        FROM read_parquet({files_lit}, union_by_name=True)
        WHERE _lon_lo <= {cl} AND _lon_hi >= {cl}
          AND _lat_lo <= {ca} AND _lat_hi >= {ca}
    """).fetchone()
    return int(row[0]) if row else 0


def write_tile_from_intermediates(
    con,
    intermediates: list[str],
    cell_lon: int,
    cell_lat: int,
    out_path: Path,
) -> tuple[int, int, str | None]:
    """Direct write of one tile by range-filtering the intermediates.

    Strips the cell-bound aux columns from output (they're internal). Returns
    (feature_count, size_bytes, sha256). On empty result, no file is written.
    """
    files_lit = _files_sql(intermediates)
    cl, ca = q_int(cell_lon), q_int(cell_lat)
    tmp = out_path.with_suffix(".tmp")
    con.execute(f"""
        COPY (
            SELECT * EXCLUDE (_lon_lo, _lon_hi, _lat_lo, _lat_hi)
            FROM read_parquet({files_lit}, union_by_name=True)
            WHERE _lon_lo <= {cl} AND _lon_hi >= {cl}
              AND _lat_lo <= {ca} AND _lat_hi >= {ca}
        ) TO '{q_path(tmp)}' (FORMAT 'parquet', COMPRESSION 'gzip')
    """)
    if not tmp.exists():
        return 0, 0, None
    size_bytes = tmp.stat().st_size
    if size_bytes == 0:
        tmp.unlink()
        return 0, 0, None
    count_row = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{q_path(tmp)}')"
    ).fetchone()
    feature_count = int(count_row[0]) if count_row else 0
    if feature_count == 0:
        tmp.unlink()
        return 0, 0, None
    sha256 = _sha256_file(tmp)
    tmp.rename(out_path)
    return feature_count, size_bytes, sha256


def materialize_cell_from_intermediates(
    con,
    intermediates: list[str],
    cell_lon: int,
    cell_lat: int,
) -> int:
    """Load this cell's range-filtered slice into TEMP TABLE _cell for recursion."""
    files_lit = _files_sql(intermediates)
    cl, ca = q_int(cell_lon), q_int(cell_lat)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _cell AS
        SELECT * EXCLUDE (_lon_lo, _lon_hi, _lat_lo, _lat_hi)
        FROM read_parquet({files_lit}, union_by_name=True)
        WHERE _lon_lo <= {cl} AND _lon_hi >= {cl}
          AND _lat_lo <= {ca} AND _lat_hi >= {ca}
    """)
    row = con.execute("SELECT COUNT(*) FROM _cell").fetchone()
    return int(row[0]) if row else 0


# ---------------------------------------------------------------------------
# Tile checkpoint
# ---------------------------------------------------------------------------

def _tile_key(z: int, x: int, y: int) -> str:
    return f"{z}:{x}:{y}"


def load_tile_checkpoint(tiles_theme_dir: Path) -> dict[str, dict]:
    """Return {z:x:y -> entry} mapping from _tiles.json."""
    cp = tiles_theme_dir / TILES_CHECKPOINT_FILENAME
    if not cp.exists():
        return {}
    try:
        data = json.loads(cp.read_text(encoding="utf-8"))
        return {_tile_key(e["z"], e["x"], e["y"]): e for e in data}
    except Exception as e:
        print(f"[warn] tile checkpoint read error: {e} — starting fresh", flush=True)
        return {}


def save_tile_checkpoint(tiles_theme_dir: Path, entries: dict[str, dict]) -> None:
    cp = tiles_theme_dir / TILES_CHECKPOINT_FILENAME
    tmp = cp.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(list(entries.values()), indent=2), encoding="utf-8")
    tmp.replace(cp)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def tile_filename(z: int, x: int, y: int) -> str:
    return f"{z}_{x}_{y}.parquet.gz"


def _empty_entry(addr: TileAddress) -> dict:
    return {
        "z": addr.z, "x": addr.x, "y": addr.y,
        "status": "empty",
        "feature_count": 0, "size_bytes": 0,
        "sha256": None, "at": _iso_now(),
    }


def _failed_entry(addr: TileAddress) -> dict:
    return {
        "z": addr.z, "x": addr.x, "y": addr.y,
        "status": "failed",
        "feature_count": 0, "size_bytes": 0,
        "sha256": None, "at": _iso_now(),
    }


def _in_progress_entry(addr: TileAddress) -> dict:
    return {
        "z": addr.z, "x": addr.x, "y": addr.y,
        "status": "in_progress",
        "feature_count": 0, "size_bytes": 0,
        "sha256": None, "at": _iso_now(),
    }


def _done_entry(addr: TileAddress, feature_count: int, size_bytes: int, sha256: str) -> dict:
    return {
        "z": addr.z, "x": addr.x, "y": addr.y,
        "status": "done",
        "feature_count": feature_count,
        "size_bytes": size_bytes,
        "sha256": sha256, "at": _iso_now(),
    }


# ---------------------------------------------------------------------------
# Worker context (shared across thread pool)
# ---------------------------------------------------------------------------

@dataclass
class WorkerCtx:
    theme: str
    workdir: Path
    tiles_theme_dir: Path
    dry_run: bool
    tile_checkpoint: dict[str, dict]
    manifest: list[TileRecord]
    lock: threading.Lock
    save_counter: list[int]  # mutable container so workers can increment
    coarse_dir: Path  # Pass 1.5 output root; per-cell lookup yields a small bucket parquet


# Per-thread DuckDB connection — created on first use, kept for thread lifetime.
_thread_local = threading.local()


def _get_thread_con():
    con = getattr(_thread_local, "con", None)
    if con is None:
        con = _new_con()
        _thread_local.con = con
    return con


def _bump_save_counter(ctx: WorkerCtx) -> None:
    with ctx.lock:
        ctx.save_counter[0] += 1
        n = ctx.save_counter[0]
        if n % CHECKPOINT_SAVE_INTERVAL == 0:
            save_tile_checkpoint(ctx.tiles_theme_dir, ctx.tile_checkpoint)


# ---------------------------------------------------------------------------
# Pass 2 worker — per-cell merge + recursive subdivide
# ---------------------------------------------------------------------------

def merge_cell_threaded(cell_lon: int, cell_lat: int, ctx: WorkerCtx) -> None:
    """Worker entry point: merge all partials for one cell into a tile (+recurse if oversize)."""
    addr = cell_to_address(cell_lon, cell_lat)
    key = _tile_key(addr.z, addr.x, addr.y)
    tile_name = tile_filename(addr.z, addr.x, addr.y)

    # --- Checkpoint check (skip if already done) ---------------------------
    skip_resolved = False
    with ctx.lock:
        entry = ctx.tile_checkpoint.get(key)
        if entry and entry["status"] in ("done", "empty") and not ctx.dry_run:
            if entry["status"] == "done":
                ctx.manifest.append(TileRecord(
                    theme=ctx.theme,
                    z=addr.z, x=addr.x, y=addr.y,
                    bbox=addr.bbox.as_list(),
                    size_bytes=entry["size_bytes"],
                    feature_count=entry["feature_count"],
                ))
            skip_resolved = True
    if skip_resolved:
        # Bump counter OUTSIDE the lock — _bump_save_counter re-acquires it
        # and threading.Lock is non-reentrant.
        _bump_save_counter(ctx)
        return

    con = _get_thread_con()
    coarse_lon = cell_lon // _FINE_PER_COARSE_SIDE
    coarse_lat = cell_lat // _FINE_PER_COARSE_SIDE
    bucket = coarse_bucket_paths(ctx.coarse_dir, coarse_lon, coarse_lat)

    # --- EXISTS probe on the cell's coarse bucket (small parquet) ----------
    try:
        n_features = cell_count_from_intermediates(con, bucket, cell_lon, cell_lat)
    except Exception as e:
        print(f"  FAIL  {ctx.theme}/{tile_name} (probe): {e}", flush=True)
        with ctx.lock:
            ctx.tile_checkpoint[key] = _failed_entry(addr)
        _bump_save_counter(ctx)
        return

    if n_features == 0:
        if not ctx.dry_run:
            with ctx.lock:
                ctx.tile_checkpoint[key] = _empty_entry(addr)
        _bump_save_counter(ctx)
        return

    # --- Dry-run: COUNT only, no write -------------------------------------
    if ctx.dry_run:
        print(
            f"  DRY  {ctx.theme}/{tile_name} features={n_features}",
            flush=True,
        )
        _bump_save_counter(ctx)
        return

    ctx.tiles_theme_dir.mkdir(parents=True, exist_ok=True)
    tile_path = ctx.tiles_theme_dir / tile_name

    with ctx.lock:
        ctx.tile_checkpoint[key] = _in_progress_entry(addr)

    # --- First write: direct range-scan from the coarse bucket -------------
    try:
        feature_count, size_bytes, sha256 = write_tile_from_intermediates(
            con, bucket, cell_lon, cell_lat, tile_path
        )
    except Exception as e:
        print(f"  FAIL  {ctx.theme}/{tile_name}: {e}", flush=True)
        with ctx.lock:
            ctx.tile_checkpoint[key] = _failed_entry(addr)
        _bump_save_counter(ctx)
        return

    if feature_count == 0:
        with ctx.lock:
            ctx.tile_checkpoint[key] = _empty_entry(addr)
        _bump_save_counter(ctx)
        return

    at_min_cell = addr.bbox.width_deg() <= MIN_CELL_DEG + 1e-9

    # --- Fits — accept and finish (the dominant happy path) ----------------
    if size_bytes <= MAX_TILE_BYTES or at_min_cell:
        if size_bytes > MAX_TILE_BYTES and at_min_cell:
            print(
                f"  WARN  {ctx.theme}/{tile_name} "
                f"{size_bytes/1e6:.1f} MB > 20 MB but at min cell ({MIN_CELL_DEG}°) — accepting",
                flush=True,
            )
        with ctx.lock:
            ctx.tile_checkpoint[key] = _done_entry(addr, feature_count, size_bytes, sha256)
            ctx.manifest.append(TileRecord(
                theme=ctx.theme,
                z=addr.z, x=addr.x, y=addr.y,
                bbox=addr.bbox.as_list(),
                size_bytes=size_bytes,
                feature_count=feature_count,
            ))
        print(
            f"  TILE  {ctx.theme}/{tile_name} "
            f"{size_bytes/1e6:.1f} MB  {feature_count} features",
            flush=True,
        )
        _bump_save_counter(ctx)
        return

    # --- Oversize — discard, materialize partials, recurse in-memory -------
    tile_path.unlink(missing_ok=True)
    with ctx.lock:
        ctx.tile_checkpoint.pop(key, None)
    print(
        f"  SPLIT {ctx.theme}/{tile_name} "
        f"{size_bytes/1e6:.1f} MB > 20 MB, materializing for in-memory recursion",
        flush=True,
    )

    try:
        cell_count = materialize_cell_from_intermediates(con, bucket, cell_lon, cell_lat)
    except Exception as e:
        print(f"  FAIL  {ctx.theme}/{tile_name} (materialize for split): {e}", flush=True)
        with ctx.lock:
            ctx.tile_checkpoint[key] = _failed_entry(addr)
        _bump_save_counter(ctx)
        return

    if cell_count == 0:
        with ctx.lock:
            ctx.tile_checkpoint[key] = _empty_entry(addr)
        try:
            con.execute("DROP TABLE IF EXISTS _cell")
        except Exception:
            pass
        _bump_save_counter(ctx)
        return

    try:
        for i, child_bbox in enumerate(addr.bbox.quadrants()):
            child_addr = child_address(addr, child_bbox, i)
            _emit_from_temp(con, child_addr, ctx)
    finally:
        try:
            con.execute("DROP TABLE IF EXISTS _cell")
        except Exception:
            pass

    _bump_save_counter(ctx)


def _emit_from_temp(con, addr: TileAddress, ctx: WorkerCtx) -> None:
    """Write addr's clipped slice of _cell to disk; subdivide in-memory if oversize.

    All queries hit the TEMP TABLE _cell — no input parquet (or partial) re-reads
    at any recursion level.
    """
    key = _tile_key(addr.z, addr.x, addr.y)
    tile_name = tile_filename(addr.z, addr.x, addr.y)
    tile_path = ctx.tiles_theme_dir / tile_name

    # Sub-cell may already be done from a prior crash-resume of this z=0 cell
    with ctx.lock:
        entry = ctx.tile_checkpoint.get(key)
        if entry and entry["status"] in ("done", "empty"):
            if entry["status"] == "done":
                ctx.manifest.append(TileRecord(
                    theme=ctx.theme,
                    z=addr.z, x=addr.x, y=addr.y,
                    bbox=addr.bbox.as_list(),
                    size_bytes=entry["size_bytes"],
                    feature_count=entry["feature_count"],
                ))
            return
        ctx.tile_checkpoint[key] = _in_progress_entry(addr)

    try:
        feature_count, size_bytes, sha256 = write_tile_from_temp(con, addr.bbox, tile_path)
    except Exception as e:
        print(f"  FAIL  {ctx.theme}/{tile_name}: {e}", flush=True)
        with ctx.lock:
            ctx.tile_checkpoint[key] = _failed_entry(addr)
        return

    if feature_count == 0:
        with ctx.lock:
            ctx.tile_checkpoint[key] = _empty_entry(addr)
        return

    at_min_cell = addr.bbox.width_deg() <= MIN_CELL_DEG + 1e-9

    if size_bytes > MAX_TILE_BYTES and not at_min_cell:
        tile_path.unlink(missing_ok=True)
        with ctx.lock:
            ctx.tile_checkpoint.pop(key, None)
        print(
            f"  SPLIT {ctx.theme}/{tile_name} "
            f"{size_bytes/1e6:.1f} MB > 20 MB, subdividing in-memory",
            flush=True,
        )
        for i, child_bbox in enumerate(addr.bbox.quadrants()):
            child_addr = child_address(addr, child_bbox, i)
            _emit_from_temp(con, child_addr, ctx)
        return

    if size_bytes > MAX_TILE_BYTES and at_min_cell:
        print(
            f"  WARN  {ctx.theme}/{tile_name} "
            f"{size_bytes/1e6:.1f} MB > 20 MB but at min cell ({MIN_CELL_DEG}°) — accepting",
            flush=True,
        )

    with ctx.lock:
        ctx.tile_checkpoint[key] = _done_entry(addr, feature_count, size_bytes, sha256)
        ctx.manifest.append(TileRecord(
            theme=ctx.theme,
            z=addr.z, x=addr.x, y=addr.y,
            bbox=addr.bbox.as_list(),
            size_bytes=size_bytes,
            feature_count=feature_count,
        ))
    print(
        f"  TILE  {ctx.theme}/{tile_name} "
        f"{size_bytes/1e6:.1f} MB  {feature_count} features",
        flush=True,
    )


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------

MANIFEST_FIELD_ORDER = ["theme", "z", "x", "y", "bbox", "size_bytes", "feature_count"]


def serialize_manifest(records: list[TileRecord]) -> str:
    rows = [{k: getattr(r, k) for k in MANIFEST_FIELD_ORDER} for r in records]
    return json.dumps(rows, indent=2)


def load_existing_manifest(workdir: Path) -> list[TileRecord]:
    path = workdir / "manifest.json"
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
        return [
            TileRecord(
                theme=row["theme"],
                z=row["z"], x=row["x"], y=row["y"],
                bbox=row["bbox"],
                size_bytes=row["size_bytes"],
                feature_count=row["feature_count"],
            )
            for row in data
        ]
    except Exception:
        return []


def write_manifest(workdir: Path, records: list[TileRecord]) -> None:
    path = workdir / "manifest.json"
    path.write_text(serialize_manifest(records))
    print(f"\nManifest written: {path}  ({len(records)} tiles)", flush=True)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def resolve_workdir(cli_value: str | None) -> Path:
    if cli_value:
        return Path(cli_value)
    env_val = os.environ.get("OVERTURE_WORKDIR")
    if env_val:
        return Path(env_val)
    if sys.platform == "win32":
        return Path("D:/overture")
    return Path("/Volumes/SSD/overture")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Tile local Overture Maps parquet files into <=20 MB gzipped Parquet tiles. "
            "Phase 2 of the download -> tile pipeline. "
            "Reads from <workdir>/raw/<theme>/ (populated by download.py)."
        )
    )
    parser.add_argument(
        "--theme",
        required=True,
        choices=ALL_THEMES,
        help=f"Theme to tile. Choices: {', '.join(ALL_THEMES)}",
    )
    parser.add_argument(
        "--workdir",
        default=None,
        help=(
            "Root directory. Overrides OVERTURE_WORKDIR env var. "
            "Reads raw/<theme>/, writes tiles/<theme>/. "
            "Default: D:\\overture (Windows) or /Volumes/SSD/overture (Mac)"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Walk + count without writing tile files (Pass 1 still runs to populate staging).",
    )
    parser.add_argument(
        "--release",
        default=None,
        help=f"Overture release tag (informational only; default: {OVERTURE_RELEASE}).",
    )
    parser.add_argument(
        "--bbox",
        default=None,
        help=(
            "Restrict tiling to a region. Format: 'min_lon,min_lat,max_lon,max_lat' "
            "in degrees. E.g. Denmark: '8,54,16,58'. Default: full planet."
        ),
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=None,
        help=(
            "Worker threads (each gets its own DuckDB connection). "
            "Default: os.cpu_count() - 2."
        ),
    )
    parser.add_argument(
        "--keep-staging",
        action="store_true",
        help=(
            "Keep <workdir>/.tile-staging/<theme>/ after Pass 2 finishes. "
            "Default: delete after success (regenerable from raw if needed)."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    release = args.release or OVERTURE_RELEASE
    workdir = resolve_workdir(args.workdir)
    tiles_theme_dir = workdir / "tiles" / args.theme
    n_threads = args.threads if args.threads else max(1, (os.cpu_count() or 4) - 2)

    print(f"Overture tiler v11 (intermediate + 1° coarse-partition + per-cell scan)  release={release}  theme={args.theme}")
    print(f"Workdir:     {workdir}")
    print(f"Raw source:  {workdir / 'raw' / args.theme}")
    print(f"Staging:     {staging_dir_for_theme(workdir, args.theme)}")
    print(f"Tile output: {tiles_theme_dir}")
    print(f"Threads:     {n_threads}")
    if args.dry_run:
        print("DRY RUN — Pass 1 still partitions, Pass 2 only counts (no tile writes)\n")

    # Verify raw files exist
    raw_dir = workdir / "raw" / args.theme
    if not raw_dir.exists():
        print(
            f"\n[error] Raw directory not found: {raw_dir}\n"
            f"Run download.py --theme {args.theme} first.",
            file=sys.stderr,
        )
        sys.exit(1)
    parquet_files = sorted(raw_dir.rglob("*.parquet"))
    if not parquet_files:
        print(
            f"\n[error] No parquet files found in {raw_dir}\n"
            f"Run download.py --theme {args.theme} first.",
            file=sys.stderr,
        )
        sys.exit(1)
    print(f"Raw files:   {len(parquet_files)} parquet files found")

    # Disk-space heads-up. Pass 1 worst case = input theme size.
    expected_gb = THEME_SIZE_GB.get(args.theme, 50)
    try:
        free_bytes = shutil.disk_usage(workdir.anchor or "/").free
        free_gb = free_bytes / (1024 ** 3)
        print(
            f"Disk:        ~{expected_gb} GB staging needed; "
            f"{free_gb:.0f} GB free on {workdir.anchor or '/'}"
        )
        if free_gb < expected_gb + 20:
            print(
                f"  WARN: free space ({free_gb:.0f} GB) is close to the "
                f"expected staging footprint (~{expected_gb} GB). "
                f"Consider --keep-staging=False (default) and freeing space first."
            )
    except Exception as e:
        print(f"  (could not measure free space: {e})")

    workdir.mkdir(parents=True, exist_ok=True)
    tiles_theme_dir.mkdir(parents=True, exist_ok=True)
    staging_dir_for_theme(workdir, args.theme).mkdir(parents=True, exist_ok=True)

    # DuckDB spill goes to C:\ (a SECOND SSD), not D:\ where raw + intermediates
    # + coarse + tiles all live. Splitting reads/writes across drives stops the
    # single-drive contention that throttles parallel I/O — same reason game
    # downloads slow down when the temp cache shares the install drive.
    global _DUCKDB_TEMP_DIR
    _DUCKDB_TEMP_DIR = Path(os.environ.get("LOCALAPPDATA", r"C:\Users\Public\AppData\Local")) / "Temp" / "duckdb-tiler"
    _DUCKDB_TEMP_DIR.mkdir(parents=True, exist_ok=True)
    print(f"DuckDB spill: {_DUCKDB_TEMP_DIR}  (C:\\ — separate drive from D:\\ working set)")

    # Region bbox (optional)
    region_bbox: Bbox | None = None
    if args.bbox:
        parts = [p.strip() for p in args.bbox.split(",")]
        if len(parts) != 4:
            raise SystemExit(f"--bbox needs 4 comma-separated values, got: {args.bbox!r}")
        region_bbox = Bbox(
            min_lon=float(parts[0]),
            min_lat=float(parts[1]),
            max_lon=float(parts[2]),
            max_lat=float(parts[3]),
        )
        print(f"Region:      {region_bbox.as_list()}")

    print()

    # ----- PASS 1 ----------------------------------------------------------
    t_pass1 = time.time()
    run_pass1(parquet_files, args.theme, workdir, region_bbox, n_threads)
    pass1_elapsed = time.time() - t_pass1

    # ----- Collect intermediates + global cell-bound extent ----------------
    print("\nCollecting intermediates...", flush=True)
    iset = collect_intermediates_and_extent(workdir, args.theme)
    if iset is None or not iset.paths:
        print("No intermediates found. Nothing to do.", flush=True)
        return
    print(
        f"  {len(iset.paths)} intermediate parquets, {iset.total_rows:,} rows, "
        f"{iset.total_bytes/(1024**3):.2f} GB",
        flush=True,
    )

    # ----- Pass 1.5: coarse 1° partition (skipped if _DONE marker present) -
    coarse_dir = coarse_dir_for_theme(workdir, args.theme)
    if _coarse_partition_done(coarse_dir):
        print(f"\nPass 1.5: skipped (resumed from prior _DONE at {coarse_dir})", flush=True)
    else:
        print(f"\nPass 1.5: partitioning intermediates into 1° coarse buckets at {coarse_dir}...", flush=True)
        t_p15 = time.time()
        ok15, err15, n_buckets = run_pass1_5_coarse_partition(
            iset.paths, coarse_dir, n_threads, region_bbox
        )
        if not ok15:
            print(f"  Pass 1.5 FAILED: {err15}", flush=True)
            return
        print(
            f"  Pass 1.5 done: {n_buckets} coarse buckets in {(time.time()-t_p15)/60:.1f} min",
            flush=True,
        )

    # ----- Cell enumeration via coarse buckets -----------------------------
    t_enum = time.time()
    coarse_cells = list_nonempty_coarse_cells(coarse_dir)
    cells: list[tuple[int, int]] = []
    for (cl, ca) in coarse_cells:
        cells.extend(fine_cells_for_coarse(cl, ca, region_bbox))
    print(
        f"  {len(coarse_cells):,} non-empty coarse buckets -> "
        f"{len(cells):,} candidate fine cells  enum took {time.time()-t_enum:.1f}s",
        flush=True,
    )
    if not cells:
        print("No candidate cells. Nothing to do.", flush=True)
        return

    # Load any tiles already in the manifest (other themes from prior runs)
    manifest: list[TileRecord] = load_existing_manifest(workdir)
    manifest = [r for r in manifest if r.theme != args.theme]

    # Load per-theme tile checkpoint
    tile_checkpoint: dict[str, dict] = {}
    if not args.dry_run:
        tile_checkpoint = load_tile_checkpoint(tiles_theme_dir)
        done_count = sum(1 for e in tile_checkpoint.values() if e["status"] == "done")
        empty_count = sum(1 for e in tile_checkpoint.values() if e["status"] == "empty")
        failed_count = sum(1 for e in tile_checkpoint.values() if e["status"] == "failed")
        in_progress = sum(1 for e in tile_checkpoint.values() if e["status"] == "in_progress")
        if tile_checkpoint:
            print(
                f"Checkpoint: {done_count} done, {empty_count} empty, "
                f"{failed_count} failed, {in_progress} in_progress (will retry) "
                f"— skip done+empty"
            )
            for e in tile_checkpoint.values():
                if e["status"] == "done":
                    manifest.append(TileRecord(
                        theme=args.theme,
                        z=e["z"], x=e["x"], y=e["y"],
                        bbox=[0, 0, 0, 0],  # backfilled at read; manifest derives from tiles
                        size_bytes=e["size_bytes"],
                        feature_count=e["feature_count"],
                    ))

    # ----- PASS 2 ----------------------------------------------------------
    print(f"\nPass 2: merging {len(cells)} cells ({n_threads} parallel workers)...", flush=True)
    ctx = WorkerCtx(
        theme=args.theme,
        workdir=workdir,
        tiles_theme_dir=tiles_theme_dir,
        dry_run=args.dry_run,
        tile_checkpoint=tile_checkpoint,
        manifest=manifest,
        lock=threading.Lock(),
        save_counter=[0],
        coarse_dir=coarse_dir,
    )

    t_pass2 = time.time()
    last_progress = t_pass2
    progress_interval = 30  # seconds

    with ThreadPoolExecutor(max_workers=n_threads) as pool:
        futures = [
            pool.submit(merge_cell_threaded, cl, ca, ctx)
            for (cl, ca) in cells
        ]
        completed = 0
        total = len(futures)
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                print(f"[error] Pass 2 worker exception: {e}", flush=True)
            completed += 1
            now = time.time()
            if now - last_progress >= progress_interval or completed == total:
                rate = completed / max(0.1, now - t_pass2)
                eta_sec = (total - completed) / max(0.001, rate)
                print(
                    f"--- progress: {completed}/{total} cells "
                    f"({100*completed/total:.1f}%)  "
                    f"rate={rate:.1f} cell/s  "
                    f"eta={eta_sec/60:.1f} min",
                    flush=True,
                )
                last_progress = now

    pass2_elapsed = time.time() - t_pass2

    if not args.dry_run:
        save_tile_checkpoint(tiles_theme_dir, tile_checkpoint)
        write_manifest(workdir, manifest)

    # ----- Cleanup ---------------------------------------------------------
    if not args.keep_staging and not args.dry_run:
        staging = staging_dir_for_theme(workdir, args.theme)
        print(f"\nCleaning staging dir: {staging}", flush=True)
        try:
            shutil.rmtree(staging)
            print("  staging dir removed.", flush=True)
        except Exception as e:
            print(f"  WARN: staging cleanup failed: {e}", flush=True)
    elif args.keep_staging:
        print(f"\nKeeping staging dir (--keep-staging): {staging_dir_for_theme(workdir, args.theme)}", flush=True)

    total_elapsed = pass1_elapsed + pass2_elapsed
    print(
        f"\nDone. Pass 1: {pass1_elapsed/60:.1f} min, "
        f"Pass 2: {pass2_elapsed/60:.1f} min, "
        f"total: {total_elapsed/60:.1f} min.",
        flush=True,
    )


if __name__ == "__main__":
    main()
