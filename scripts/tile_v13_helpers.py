"""tile_v13_helpers.py — pure helpers shared across v13 pass scripts.

This module is the single source of Web Mercator z/x/y math, path layout,
and DuckDB connection construction for the v13 pipeline. Pass scripts
(`tile_v13_pass1.py`, `tile_v13_pass1_5.py`, `tile_v13_pass2.py`,
`tile_v13_pass3_merge.py`) import everything from here — no Mercator-math
duplication elsewhere.

See `infrastructure/scripts/overture-tiler/v13_SPEC.md` for the design
decisions referenced here.

Coordinate system
-----------------

Web Mercator (EPSG:3857), native lat extent ±85.0511287798066°.

`(z, x, y)` follows the standard slippy-map convention:

    Tile (z, x, y) covers
        lng_min = -180 + 360 * x / 2^z
        lng_max = -180 + 360 * (x + 1) / 2^z
        lat_max = mercator_lat( pi - 2 * pi * y / 2^z )
        lat_min = mercator_lat( pi - 2 * pi * (y + 1) / 2^z )

x grows east (0 at antimeridian wrap), y grows south (0 at +85.05°).

This convention matches OSM/Mapbox/MapLibre/everything else; the v13 tile
output is directly joinable with any standard slippy-map renderer.
"""
from __future__ import annotations

import math
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

# SQL-safety helpers — see _sql_safety.py for the why.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from _sql_safety import q_int, q_mem_limit, q_path  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Web Mercator native latitude extent. The exact value is
# arctan(sinh(pi)) in degrees ≈ 85.0511287798066.
LAT_BOUNDS: tuple[float, float] = (-85.0511287798066, 85.0511287798066)

# World longitude extent (full planet).
LNG_BOUNDS: tuple[float, float] = (-180.0, 180.0)

# Default leaf depth and tile-size budget for v13.
Z_MAX_DEFAULT: int = 14
TILE_BUDGET_DEFAULT: int = 20 * 1024 * 1024   # 20 MB combined-theme tile budget

# Default peel width for sharding. 360 / 10 = 36 work units, comfortable
# middle ground between load-balance granularity and per-shard DuckDB setup
# overhead.
PEEL_WIDTH_DEG_DEFAULT: int = 10

# Bucket level. z=6 → 4096 buckets globally, ~50 km on the equator.
Z_BUCKET: int = 6

# All Overture themes the v13 pipeline ingests (matches v12 ALL_THEMES set
# but ordered smallest-first for processing). The tile schema carries this
# constant set in the `theme` column.
THEMES: list[str] = [
    "water",
    "land",
    "segments",
    "buildings",
    "land_use",
    "infrastructure",
]

# Per-worker DuckDB defaults. Used when a pass script does not pass an
# explicit override. 6 GB matches the v13 24/7-loop budget: 4 within-peel
# pass2 workers × 6 GB = 24 GB, leaving room for a concurrent pass1-per-peel
# prefetch connection at 8 GB. Total ~36 GB DuckDB allocation; spills to
# disk on the fast 2 TB SSD when physical RAM is exhausted.
DEFAULT_MEMORY_LIMIT: str = "6GB"
# Internal threads stays at 1 by default: when N outer workers each open a
# connection, N × internal_threads must not exceed the physical core count.
# Single-connection callers (e.g. pass1-per-peel S3 reader) pass
# internal_threads=2 explicitly to use both perf cores during S3 fetches.
DEFAULT_INTERNAL_THREADS: int = 1


# ---------------------------------------------------------------------------
# Bbox (re-exported for v13 pass scripts; same shape as v11's tile.Bbox).
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Bbox:
    """Lon/lat bounding box in degrees, WGS84.

    `min_lng < max_lng` is enforced by callers; this struct does not split
    antimeridian-spanning bboxes (no z=6 bucket spans the antimeridian, so
    this never arises in v13's call path).
    """
    min_lng: float
    min_lat: float
    max_lng: float
    max_lat: float

    @property
    def width_deg(self) -> float:
        return self.max_lng - self.min_lng

    @property
    def height_deg(self) -> float:
        return self.max_lat - self.min_lat

    def as_list(self) -> list[float]:
        return [self.min_lng, self.min_lat, self.max_lng, self.max_lat]


# ---------------------------------------------------------------------------
# Web Mercator math
# ---------------------------------------------------------------------------

def lng_lat_to_mercator_xy(lng: float, lat: float, z: int) -> tuple[int, int]:
    """Project (lng, lat) in WGS84 degrees to a Mercator tile (x, y) at zoom `z`.

    `lat` is clamped to LAT_BOUNDS before projection — Web Mercator is
    undefined outside that range.

    Returns integer tile coordinates `0 <= x < 2^z`, `0 <= y < 2^z`. Pure
    function, no I/O.
    """
    if z < 0:
        raise ValueError(f"z must be >= 0, got {z}")
    n = 1 << z
    # Clamp longitude wraparound (rare; Overture data is in [-180, 180]).
    if lng < LNG_BOUNDS[0]:
        lng = LNG_BOUNDS[0]
    elif lng > LNG_BOUNDS[1]:
        lng = LNG_BOUNDS[1]
    # Clamp lat to Web Mercator native extent.
    if lat < LAT_BOUNDS[0]:
        lat = LAT_BOUNDS[0]
    elif lat > LAT_BOUNDS[1]:
        lat = LAT_BOUNDS[1]

    x = int((lng + 180.0) / 360.0 * n)
    if x >= n:
        x = n - 1
    if x < 0:
        x = 0

    lat_rad = math.radians(lat)
    # Standard slippy-map formula.
    y = int((1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi) / 2.0 * n)
    if y >= n:
        y = n - 1
    if y < 0:
        y = 0
    return x, y


def _mercator_y_to_lat(y_norm: float) -> float:
    """Inverse of the mercator y projection.

    `y_norm` is the fractional y position in [0, 1] (top=0 -> +85.05°,
    bottom=1 -> -85.05°). Returns latitude in degrees.
    """
    n = math.pi - 2.0 * math.pi * y_norm
    return math.degrees(math.atan(math.sinh(n)))


def quadkey_extent(z: int, x: int, y: int) -> Bbox:
    """Bbox covered by tile (z, x, y) in WGS84 degrees.

    Pure function. The tile's bbox in WGS84 is rectangular in longitude and
    has variable height in latitude (Mercator stretches near the poles).
    """
    if z < 0:
        raise ValueError(f"z must be >= 0, got {z}")
    n = 1 << z
    if not (0 <= x < n):
        raise ValueError(f"x must be in [0, {n}), got {x}")
    if not (0 <= y < n):
        raise ValueError(f"y must be in [0, {n}), got {y}")

    lng_min = -180.0 + 360.0 * x / n
    lng_max = -180.0 + 360.0 * (x + 1) / n
    # Note: y=0 is the NORTH edge in slippy-map convention.
    lat_max = _mercator_y_to_lat(y / n)
    lat_min = _mercator_y_to_lat((y + 1) / n)
    return Bbox(min_lng=lng_min, min_lat=lat_min, max_lng=lng_max, max_lat=lat_max)


def lng_range_to_z6_keys(
    lng_lo: float,
    lng_hi: float,
    lat_bounds: tuple[float, float] = LAT_BOUNDS,
) -> list[tuple[int, int]]:
    """Enumerate all z=6 (x, y) keys whose extent intersects the lng range.

    Used by pass2 to find the buckets a peel touches. `lat_bounds` is
    Mercator's full extent by default; restrict it to clip a peel to a
    region.

    Returns sorted list of `(x, y)` tuples; never empty for a non-degenerate
    range that intersects the Mercator extent.
    """
    if lng_hi <= lng_lo:
        raise ValueError(f"lng_hi ({lng_hi}) must be > lng_lo ({lng_lo})")
    lat_lo, lat_hi = lat_bounds
    if lat_hi <= lat_lo:
        raise ValueError(f"lat_hi ({lat_hi}) must be > lat_lo ({lat_lo})")

    z = Z_BUCKET
    n = 1 << z

    # Convert lng range to integer x range. Ranges are half-open at the high
    # end, but we want inclusive coverage of any bucket the range *touches*.
    x_lo = int((lng_lo + 180.0) / 360.0 * n)
    # For the high edge, take floor of the position just inside the range.
    x_hi_raw = (lng_hi + 180.0) / 360.0 * n
    x_hi = int(math.floor(x_hi_raw - 1e-12)) if x_hi_raw > 0 else 0
    if x_lo < 0:
        x_lo = 0
    if x_hi >= n:
        x_hi = n - 1

    # y grows south in slippy-map convention: lat_hi -> small y, lat_lo -> large y.
    # `lng_lat_to_mercator_xy` returns (x, y); we only need the y coordinate
    # for the row range, so feed lng=0 (any in-range value works).
    _, y_lo = lng_lat_to_mercator_xy(0.0, lat_hi, z)
    _, y_hi = lng_lat_to_mercator_xy(0.0, lat_lo, z)
    if y_lo > y_hi:
        y_lo, y_hi = y_hi, y_lo

    keys: list[tuple[int, int]] = []
    for xi in range(x_lo, x_hi + 1):
        for yi in range(y_lo, y_hi + 1):
            keys.append((xi, yi))
    return keys


# ---------------------------------------------------------------------------
# Path layout
# ---------------------------------------------------------------------------

def tile_path(workdir: Path, z: int, x: int, y: int) -> Path:
    """Return the v13 tile path: `<workdir>/tiles/{z}/{x}/{y}.parquet`.

    No file is created here; pass scripts call `parent.mkdir(parents=True,
    exist_ok=True)` before COPY.
    """
    return workdir / "tiles" / str(z) / str(x) / f"{y}.parquet"


def per_theme_partition_dir(workdir: Path, theme: str) -> Path:
    """`<workdir>/.tile-staging/v13/per_theme/<theme>/` — pass1 output root."""
    return workdir / ".tile-staging" / "v13" / "per_theme" / theme


def combined_bucket_dir(workdir: Path) -> Path:
    """`<workdir>/.tile-staging/v13/combined/` — pass1.5 output root."""
    return workdir / ".tile-staging" / "v13" / "combined"


def combined_bucket_path(workdir: Path, x: int, y: int) -> Path:
    """`<workdir>/.tile-staging/v13/combined/z6_{x}_{y}.parquet`."""
    return combined_bucket_dir(workdir) / f"z6_{x}_{y}.parquet"


def raw_theme_dir(workdir: Path, theme: str) -> Path:
    """`<workdir>/raw/<theme>/` — populated by download.py."""
    return workdir / "raw" / theme


# ---------------------------------------------------------------------------
# Per-peel path layout (24/7 driver — see infrastructure plan for the why)
# ---------------------------------------------------------------------------
#
# The 24/7 driver materialises one peel's data at a time on local disk and
# deletes it after upload. To make `rm -rf` of an entire peel a single
# syscall and to let the prefetch stage scribble in a sibling directory
# without colliding with the active tile stage, every peel-scoped artefact
# lives under `<workdir>/<root>/peel_<idx>/`.
#
# The on-R2 layout (post-upload) is still the canonical
# `tiles/{z}/{x}/{y}.parquet` — `upload.sh --peel-idx N` strips the
# `peel_<idx>/` prefix when copying to the bucket. Locally the prefix is
# the cleanup unit.

def peel_dir_name(peel_idx: int) -> str:
    """`peel_<idx>` with zero-padded 2-digit idx (peel_00..peel_35)."""
    return f"peel_{peel_idx:02d}"


def staging_peel_dir(workdir: Path, peel_idx: int) -> Path:
    """`<workdir>/staging/peel_<idx>/` — root of one peel's pass1-per-peel output."""
    return workdir / "staging" / peel_dir_name(peel_idx)


def combined_bucket_dir_peel(workdir: Path, peel_idx: int) -> Path:
    """`<workdir>/staging/peel_<idx>/combined/` — pass1-per-peel combined buckets."""
    return staging_peel_dir(workdir, peel_idx) / "combined"


def combined_bucket_path_peel(workdir: Path, peel_idx: int, x: int, y: int) -> Path:
    """`<workdir>/staging/peel_<idx>/combined/z6_{x}_{y}.parquet`."""
    return combined_bucket_dir_peel(workdir, peel_idx) / f"z6_{x}_{y}.parquet"


def per_theme_partition_dir_peel(workdir: Path, peel_idx: int, theme: str) -> Path:
    """`<workdir>/staging/peel_<idx>/per_theme/<theme>/` — pass1-per-peel intermediate.

    The intermediate is dropped after pass1.5-equivalent collation finishes;
    pass2 reads from `combined/`, not this path.
    """
    return staging_peel_dir(workdir, peel_idx) / "per_theme" / theme


def tiles_peel_root(workdir: Path, peel_idx: int) -> Path:
    """`<workdir>/tiles/peel_<idx>/` — root of one peel's pass2 + pass3-local output."""
    return workdir / "tiles" / peel_dir_name(peel_idx)


def tile_path_peel(workdir: Path, peel_idx: int, z: int, x: int, y: int) -> Path:
    """`<workdir>/tiles/peel_<idx>/{z}/{x}/{y}.parquet` — per-peel leaf path.

    The non-peel-scoped :func:`tile_path` is preserved for legacy v13
    one-shot scripts (`tile_v13_pass2.py --all-peels`, `tile_v13_pass3_merge.py`).
    """
    return tiles_peel_root(workdir, peel_idx) / str(z) / str(x) / f"{y}.parquet"


def duckdb_tmp_dir_peel(workdir: Path, peel_idx: int) -> Path:
    """`<workdir>/duckdb-tmp/peel_<idx>/` — peel-scoped DuckDB temp_directory.

    Passed to :func:`new_con` via the `temp_dir` argument. The cleanup step
    `rm -rf` this directory after the peel uploads, freeing any spill files
    DuckDB didn't auto-clean on connection close.
    """
    return workdir / "duckdb-tmp" / peel_dir_name(peel_idx)


def peel_manifest_path(workdir: Path, peel_idx: int) -> Path:
    """`<workdir>/tiles/peel_<idx>/_manifest.json` — per-peel pass3-local manifest.

    Holds the {z,x,y,size_bytes,feature_count,theme_counts,bbox,vintage} rows
    that the index writer merges into the global `driver-state/tiles_index.json`.
    """
    return tiles_peel_root(workdir, peel_idx) / "_manifest.json"


def driver_state_dir(workdir: Path) -> Path:
    """`<workdir>/driver-state/` — persistent across peels and cycles."""
    return workdir / "driver-state"


def global_index_path(workdir: Path) -> Path:
    """`<workdir>/driver-state/tiles_index.json` — global tile index uploaded to R2."""
    return driver_state_dir(workdir) / "tiles_index.json"


def driver_state_path(workdir: Path) -> Path:
    """`<workdir>/driver-state/driver_state.json` — driver's checkpoint."""
    return driver_state_dir(workdir) / "driver_state.json"


# ---------------------------------------------------------------------------
# Workdir resolution (single-volume per v13 spec)
# ---------------------------------------------------------------------------

def resolve_workdir(cli_value: str | None) -> Path:
    """Resolve the workdir for v13 scripts.

    Order:
        1. `--workdir` CLI flag (caller passes the parsed value)
        2. OVERTURE_WORKDIR env var
        3. /Volumes/SSD/overture (macOS) or D:/overture (Windows)

    The internal-NVMe `/Users/arbirk/overture` default is gone in v13; the
    new fast SSD holds raw + staging + tiles all on one volume.
    """
    if cli_value:
        return Path(cli_value)
    env_val = os.environ.get("OVERTURE_WORKDIR")
    if env_val:
        return Path(env_val)
    if sys.platform == "win32":
        return Path("D:/overture")
    return Path("/Volumes/SSD/overture")


# ---------------------------------------------------------------------------
# DuckDB connection factory
# ---------------------------------------------------------------------------

def new_con(
    internal_threads: int = DEFAULT_INTERNAL_THREADS,
    memory_limit: str = DEFAULT_MEMORY_LIMIT,
    temp_dir: Path | None = None,
):
    """Construct a new DuckDB connection with v13's standard tunables.

    Mirrors v11/v12's `_new_con`:

      - `INSTALL spatial; LOAD spatial;` — geometry types
      - `SET threads = internal_threads` — pin to 1 by default; the
        outer worker pool provides parallelism. Without this, N workers
        × N_CPUS internal threads each = wild over-subscription.
      - `SET memory_limit = '<memory_limit>'` — caps each connection so
        N × memory_limit stays at or below the physical+swap budget.
      - `SET preserve_insertion_order = false` — required for parallel
        COPY ... PARTITION_BY (otherwise DuckDB serializes the writer
        and buffers all open partitions in memory).
      - `SET temp_directory = '<temp_dir>'` if supplied — pin DuckDB
        spill to a known volume (v13 spec: internal NVMe).

    The caller owns the connection and must `con.close()` it (or use a
    `try / finally`).
    """
    import duckdb  # type: ignore

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(f"SET threads = {q_int(internal_threads)}")
    con.execute(f"SET memory_limit = '{q_mem_limit(memory_limit)}'")
    con.execute("SET preserve_insertion_order = false")
    if temp_dir is not None:
        con.execute(f"SET temp_directory = '{q_path(temp_dir)}'")
    return con


# ---------------------------------------------------------------------------
# Iteration helpers
# ---------------------------------------------------------------------------

def all_z6_keys() -> Iterable[tuple[int, int]]:
    """Yield every z=6 (x, y) key in the world (4096 total).

    Used by pass1.5 (collate) and pass2 (peel-shard) for global iteration.
    """
    n = 1 << Z_BUCKET
    for x in range(n):
        for y in range(n):
            yield (x, y)


# ---------------------------------------------------------------------------
# Peel scheduling helpers (24/7 driver)
# ---------------------------------------------------------------------------

def n_peels(peel_width_deg: int = PEEL_WIDTH_DEG_DEFAULT) -> int:
    """Number of peels in a full longitude sweep. 360 / width."""
    return int((LNG_BOUNDS[1] - LNG_BOUNDS[0]) // peel_width_deg)


def peel_lng_range(
    peel_idx: int,
    peel_width_deg: int = PEEL_WIDTH_DEG_DEFAULT,
) -> tuple[float, float]:
    """Return `(lng_lo, lng_hi)` for `peel_idx` under the canonical -180° anchor.

    peel_idx=0  → (-180, -170)
    peel_idx=18 → (   0,   10)
    peel_idx=35 → ( 170,  180)
    """
    lo = LNG_BOUNDS[0] + peel_idx * peel_width_deg
    hi = lo + peel_width_deg
    return lo, hi


def eastward_peel_order_from_zero(
    peel_width_deg: int = PEEL_WIDTH_DEG_DEFAULT,
) -> list[int]:
    """Peel iteration order that starts at lng=0° and proceeds east.

    With the default 10° peel width this returns
    `[18, 19, ..., 35, 0, 1, ..., 17]`:
    peel 18 is lng [0, 10), peel 35 is lng [170, 180), peel 0 wraps to
    lng [-180, -170), and the cycle ends with peel 17 at lng [-10, 0).
    """
    total = n_peels(peel_width_deg)
    # The peel whose lower bound equals 0° is at index (-LNG_BOUNDS[0] / width).
    start = int(-LNG_BOUNDS[0] // peel_width_deg)
    return [(start + i) % total for i in range(total)]
