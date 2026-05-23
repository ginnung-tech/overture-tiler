"""
Overture Maps adaptive-quadtree tiler.

Tiles raw Overture Parquet data (read from S3 via DuckDB httpfs) into
<=20 MB gzipped Parquet files, one per quadtree cell.

Output layout: <workdir>/<theme>/<z>_<x>_<y>.parquet.gz
Manifest:      <workdir>/manifest.json

Usage:
    python tiler.py --theme buildings [--workdir /Volumes/SSD/overture] [--dry-run]

Workdir resolution order:
    1. --workdir CLI flag
    2. OVERTURE_WORKDIR env var
    3. /Volumes/SSD/overture (default, Mac SSD)

Resumability:
    Each tile writes a sidecar <theme>/<z>_<x>_<y>.fcount containing the
    integer feature count. On restart, if both .parquet.gz and .fcount exist
    and the sidecar count matches a fresh metadata-only COUNT(*) query, the
    tile is skipped.

Dependencies: duckdb, Python 3.11+, gzip, json (stdlib)
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import math
import os
import re
import sys
import tempfile
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Generator

# SQL-safety helpers — see _sql_safety.py for the why.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from _sql_safety import q_float, q_path  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Strict pattern for OVERTURE_RELEASE, validated at import time to keep it
# out of f-string SQL paths below.
_RELEASE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}(\.\d+)?$")
_release_env = os.environ.get("OVERTURE_RELEASE", "2026-04-15.0")
if not _RELEASE_RE.match(_release_env):
    raise SystemExit(
        f"invalid OVERTURE_RELEASE {_release_env!r}; expected e.g. 2026-04-15.0"
    )
OVERTURE_RELEASE = _release_env
S3_BASE = f"s3://overturemaps-us-west-2/release/{OVERTURE_RELEASE}"

# Overture theme -> S3 subpath pattern used with hive_partitioning=1
THEME_PATHS: dict[str, str] = {
    "buildings": f"{S3_BASE}/theme=buildings/type=building/**/*.parquet",
    "segments": f"{S3_BASE}/theme=transportation/type=segment/**/*.parquet",
    "land_use": f"{S3_BASE}/theme=base/type=land_use/**/*.parquet",
    "water": f"{S3_BASE}/theme=base/type=water/**/*.parquet",
    "land": f"{S3_BASE}/theme=base/type=land/**/*.parquet",
    "infrastructure": f"{S3_BASE}/theme=base/type=infrastructure/**/*.parquet",
}

ALL_THEMES = list(THEME_PATHS.keys())

# Quadtree parameters (degrees)
START_CELL_DEG: float = 0.1      # ~10 km initial grid
MIN_CELL_DEG: float = 0.001     # ~100 m minimum (accept oversize below this)
MAX_TILE_BYTES: int = 20 * 1024 * 1024  # 20 MB gzipped

# World bounds
WORLD_BOUNDS = (-180.0, -90.0, 180.0, 90.0)

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
    """Quadtree address: z = depth (0 = root), x/y = column/row at that depth."""
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
# Quadtree enumeration
# ---------------------------------------------------------------------------

def _depth_from_width(width_deg: float) -> int:
    """Return the quadtree depth (z) for a given cell width."""
    if width_deg >= START_CELL_DEG - 1e-9:
        return 0
    depth = 0
    w = START_CELL_DEG
    while w > width_deg + 1e-9:
        w /= 2.0
        depth += 1
    return depth


def _cell_indices(bbox: Bbox, start_width: float = START_CELL_DEG) -> tuple[int, int]:
    """Return (x, y) tile index within the global grid at the cell's zoom level."""
    lon_offset = bbox.min_lon - WORLD_BOUNDS[0]
    lat_offset = bbox.min_lat - WORLD_BOUNDS[1]
    width = bbox.width_deg()
    # Use start_width / cell_width to find the scale factor, then index
    scale = start_width / width if width > 0 else 1
    grid_width = int(round((WORLD_BOUNDS[2] - WORLD_BOUNDS[0]) / width))
    x = int(round(lon_offset / width)) % max(grid_width, 1)
    y = int(round(lat_offset / width))
    return x, y


def enumerate_start_cells(
    region_bbox: Bbox | None = None,
) -> Generator[TileAddress, None, None]:
    """Yield the 0.1° starting cells covering the world (or just region_bbox).

    region_bbox is optional and lets callers tile a subset of the planet
    instead of the full ~6.48M-cell global walk. Useful for dev cycles
    (e.g. "tile Denmark only") and for incremental rollouts where the
    initial tile pass covers a target country before going global. The
    z/x/y indices remain in the global grid so a later global pass
    fills in the rest without renumbering existing tiles.
    """
    bounds_min_lon = WORLD_BOUNDS[0] if region_bbox is None else max(WORLD_BOUNDS[0], region_bbox.min_lon)
    bounds_max_lon = WORLD_BOUNDS[2] if region_bbox is None else min(WORLD_BOUNDS[2], region_bbox.max_lon)
    bounds_min_lat = WORLD_BOUNDS[1] if region_bbox is None else max(WORLD_BOUNDS[1], region_bbox.min_lat)
    bounds_max_lat = WORLD_BOUNDS[3] if region_bbox is None else min(WORLD_BOUNDS[3], region_bbox.max_lat)

    # Snap region bounds DOWN to the nearest 0.1° cell so the grid alignment
    # stays consistent with the global walk (the global y/x indices below
    # are computed against WORLD_BOUNDS, not against the region origin).
    snapped_min_lon = WORLD_BOUNDS[0] + math.floor((bounds_min_lon - WORLD_BOUNDS[0]) / START_CELL_DEG) * START_CELL_DEG
    snapped_min_lat = WORLD_BOUNDS[1] + math.floor((bounds_min_lat - WORLD_BOUNDS[1]) / START_CELL_DEG) * START_CELL_DEG

    lon = snapped_min_lon
    while round(lon, 6) < bounds_max_lon:
        lat = snapped_min_lat
        while round(lat, 6) < bounds_max_lat:
            # y indexes longitude (E-W), x indexes latitude (N-S) per the
            # convention enumerate_start_cells uses today — preserve it.
            y = int(round((lon - WORLD_BOUNDS[0]) / START_CELL_DEG))
            x = int(round((lat - WORLD_BOUNDS[1]) / START_CELL_DEG))
            bbox = Bbox(
                min_lon=lon,
                min_lat=lat,
                max_lon=min(lon + START_CELL_DEG, WORLD_BOUNDS[2]),
                max_lat=min(lat + START_CELL_DEG, WORLD_BOUNDS[3]),
            )
            yield TileAddress(z=0, x=x, y=y, bbox=bbox)
            lat = round(lat + START_CELL_DEG, 6)
        lon = round(lon + START_CELL_DEG, 6)


def child_address(parent: TileAddress, child_bbox: Bbox, child_index: int) -> TileAddress:
    """
    Compute the z/x/y for a child quadrant.

    Child index layout (same order as Bbox.quadrants()):
        0 = SW, 1 = SE, 2 = NW, 3 = NE

    x/y encoding:
        z increments by 1.
        At depth z the grid has 2^z times as many cells per axis as depth 0.
        child_x = parent_x * 2 + (1 if SE or NE else 0)
        child_y = parent_y * 2 + (1 if NW or NE else 0)
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

def _get_con():
    """Return a DuckDB connection with spatial + httpfs extensions loaded."""
    import duckdb  # type: ignore
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_region='us-west-2';")
    # Anonymous access to overturemaps public bucket
    con.execute("SET s3_use_ssl=true;")
    return con


def _theme_glob(theme: str) -> str:
    """Look up `theme` in THEME_PATHS and re-validate the resulting glob.

    The glob is built once from a release tag that was strict-pattern-checked
    at module import, plus a hard-coded literal. Re-validating against q_path
    is belt-and-braces against THEME_PATHS being edited to inject anything
    that could escape the surrounding `'...'` SQL literal.
    """
    if theme not in THEME_PATHS:
        raise KeyError(f"unknown theme: {theme!r}")
    return q_path(THEME_PATHS[theme])


def count_features(con, theme: str, bbox: Bbox) -> int:
    """Fast COUNT(*) with bbox pushdown — no geometry decode, just metadata scan."""
    glob = _theme_glob(theme)
    sql = f"""
        SELECT COUNT(*) FROM read_parquet('{glob}', hive_partitioning=1)
        WHERE bbox.xmin < {q_float(bbox.max_lon)}
          AND bbox.xmax > {q_float(bbox.min_lon)}
          AND bbox.ymin < {q_float(bbox.max_lat)}
          AND bbox.ymax > {q_float(bbox.min_lat)}
    """
    row = con.execute(sql).fetchone()
    return int(row[0]) if row else 0


def write_tile_parquet(con, theme: str, bbox: Bbox, out_path: Path) -> int:
    """
    Query features clipped to bbox and write a gzipped Parquet file.

    Uses DuckDB COPY ... (FORMAT 'parquet', COMPRESSION 'gzip') for a
    single-pass write — no intermediate Python buffering.

    Returns the feature count written.
    """
    glob = _theme_glob(theme)
    # Write to a temp file first to avoid partial output on failure
    tmp = out_path.with_suffix(".tmp")
    sql = f"""
        COPY (
            SELECT * FROM read_parquet('{glob}', hive_partitioning=1)
            WHERE bbox.xmin < {q_float(bbox.max_lon)}
              AND bbox.xmax > {q_float(bbox.min_lon)}
              AND bbox.ymin < {q_float(bbox.max_lat)}
              AND bbox.ymax > {q_float(bbox.min_lat)}
        ) TO '{q_path(tmp)}' (FORMAT 'parquet', COMPRESSION 'gzip')
    """
    con.execute(sql)
    # Get count from the written file (avoids a second S3 query)
    count_row = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{q_path(tmp)}')"
    ).fetchone()
    feature_count = int(count_row[0]) if count_row else 0
    tmp.rename(out_path)
    return feature_count


# ---------------------------------------------------------------------------
# Resumability
# ---------------------------------------------------------------------------

def fcount_path(tile_path: Path) -> Path:
    return tile_path.with_suffix("").with_suffix(".fcount")


def is_tile_complete(tile_path: Path, con, theme: str, bbox: Bbox) -> bool:
    """
    Return True if the tile file + sidecar both exist and the sidecar count
    matches a fresh COUNT(*) query. This avoids re-processing completed tiles.
    """
    fc_path = fcount_path(tile_path)
    if not tile_path.exists() or not fc_path.exists():
        return False
    try:
        cached_count = int(fc_path.read_text().strip())
    except (ValueError, OSError):
        return False
    live_count = count_features(con, theme, bbox)
    return cached_count == live_count


# ---------------------------------------------------------------------------
# Tile name
# ---------------------------------------------------------------------------

def tile_filename(z: int, x: int, y: int) -> str:
    return f"{z}_{x}_{y}.parquet.gz"


# ---------------------------------------------------------------------------
# Core tiling logic
# ---------------------------------------------------------------------------

def tile_cell(
    con,
    theme: str,
    addr: TileAddress,
    workdir: Path,
    dry_run: bool,
    manifest: list[TileRecord],
) -> None:
    """
    Recursively tile one quadtree cell.

    Algorithm:
        1. COUNT features in cell.
        2. If 0 — skip.
        3. Write tile; if >20 MB AND cell width > MIN_CELL_DEG — subdivide.
        4. If at MIN_CELL_DEG and oversize — accept; log a warning.
    """
    theme_dir = workdir / theme
    tile_path = theme_dir / tile_filename(addr.z, addr.x, addr.y)

    # --- Resumability check -------------------------------------------------
    if not dry_run and is_tile_complete(tile_path, con, theme, addr.bbox):
        print(
            f"  SKIP  {theme}/{tile_filename(addr.z, addr.x, addr.y)} "
            f"(resumable, sidecar matches)"
        )
        # Still need to add to manifest from sidecar
        size = tile_path.stat().st_size
        count = int(fcount_path(tile_path).read_text().strip())
        manifest.append(
            TileRecord(
                theme=theme,
                z=addr.z, x=addr.x, y=addr.y,
                bbox=addr.bbox.as_list(),
                size_bytes=size,
                feature_count=count,
            )
        )
        return

    # --- Feature count check ------------------------------------------------
    feature_count = count_features(con, theme, addr.bbox)
    if feature_count == 0:
        if dry_run:
            print(f"  DRY   {theme}/{tile_filename(addr.z, addr.x, addr.y)} — 0 features, skip")
        return

    if dry_run:
        print(
            f"  DRY   {theme}/{tile_filename(addr.z, addr.x, addr.y)} "
            f"bbox={addr.bbox.as_list()} features={feature_count}"
        )
        # Recurse to show the full planned tree (no size info without writing)
        if addr.bbox.width_deg() > MIN_CELL_DEG:
            _recurse_dry(con, theme, addr, workdir, manifest)
        return

    # --- Write tile ---------------------------------------------------------
    theme_dir.mkdir(parents=True, exist_ok=True)
    written_count = write_tile_parquet(con, theme, addr.bbox, tile_path)
    size_bytes = tile_path.stat().st_size

    at_min_cell = addr.bbox.width_deg() <= MIN_CELL_DEG + 1e-9

    if size_bytes > MAX_TILE_BYTES and not at_min_cell:
        # Oversize and can subdivide — delete and recurse into quadrants
        tile_path.unlink(missing_ok=True)
        print(
            f"  SPLIT {theme}/{tile_filename(addr.z, addr.x, addr.y)} "
            f"{size_bytes/1e6:.1f} MB > 20 MB, subdividing"
        )
        for i, child_bbox in enumerate(addr.bbox.quadrants()):
            child_addr = child_address(addr, child_bbox, i)
            tile_cell(con, theme, child_addr, workdir, dry_run, manifest)
        return

    if size_bytes > MAX_TILE_BYTES and at_min_cell:
        print(
            f"  WARN  {theme}/{tile_filename(addr.z, addr.x, addr.y)} "
            f"{size_bytes/1e6:.1f} MB > 20 MB but at min cell size ({MIN_CELL_DEG}°) — accepting"
        )

    # Write sidecar
    fcount_path(tile_path).write_text(str(written_count))

    print(
        f"  TILE  {theme}/{tile_filename(addr.z, addr.x, addr.y)} "
        f"{size_bytes/1e6:.1f} MB  {written_count} features"
    )
    manifest.append(
        TileRecord(
            theme=theme,
            z=addr.z, x=addr.x, y=addr.y,
            bbox=addr.bbox.as_list(),
            size_bytes=size_bytes,
            feature_count=written_count,
        )
    )


def _recurse_dry(con, theme: str, addr: TileAddress, workdir: Path, manifest: list) -> None:
    """Dry-run recursion: show planned subdivision without writing."""
    for i, child_bbox in enumerate(addr.bbox.quadrants()):
        child_addr = child_address(addr, child_bbox, i)
        count = count_features(con, theme, child_addr.bbox)
        if count > 0:
            print(
                f"  DRY     {theme}/{tile_filename(child_addr.z, child_addr.x, child_addr.y)} "
                f"features={count} (would subdivide further if oversize)"
            )
            if child_addr.bbox.width_deg() > MIN_CELL_DEG:
                _recurse_dry(con, theme, child_addr, workdir, manifest)


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------

MANIFEST_FIELD_ORDER = ["theme", "z", "x", "y", "bbox", "size_bytes", "feature_count"]


def serialize_manifest(records: list[TileRecord]) -> str:
    """JSON with stable field order per spec."""
    rows = []
    for r in records:
        row = {k: getattr(r, k) for k in MANIFEST_FIELD_ORDER}
        rows.append(row)
    return json.dumps(rows, indent=2)


def load_existing_manifest(workdir: Path) -> list[TileRecord]:
    """Load a prior-run manifest to prepend resumed tiles."""
    path = workdir / "manifest.json"
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
        records = []
        for row in data:
            records.append(
                TileRecord(
                    theme=row["theme"],
                    z=row["z"], x=row["x"], y=row["y"],
                    bbox=row["bbox"],
                    size_bytes=row["size_bytes"],
                    feature_count=row["feature_count"],
                )
            )
        return records
    except Exception:
        return []


def write_manifest(workdir: Path, records: list[TileRecord]) -> None:
    path = workdir / "manifest.json"
    path.write_text(serialize_manifest(records))
    print(f"\nManifest written: {path}  ({len(records)} tiles)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def resolve_workdir(cli_value: str | None) -> Path:
    if cli_value:
        return Path(cli_value)
    env_val = os.environ.get("OVERTURE_WORKDIR")
    if env_val:
        return Path(env_val)
    return Path("/Volumes/SSD/overture")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Tile Overture Maps data into <=20 MB gzipped Parquet files."
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
        help="Output directory. Overrides OVERTURE_WORKDIR env var. "
             "Default: /Volumes/SSD/overture",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Walk the quadtree without writing files. Prints planned tiles.",
    )
    parser.add_argument(
        "--release",
        default=None,
        help=f"Overture release tag (default: {OVERTURE_RELEASE}). "
             "Overrides OVERTURE_RELEASE env var.",
    )
    parser.add_argument(
        "--bbox",
        default=None,
        help="Restrict tiling to a region. Format: 'min_lon,min_lat,max_lon,max_lat' "
             "in degrees. E.g. Denmark: '8,54,16,58'. Default: full planet "
             "(~6.48 M starting cells, multi-day). z/x/y indices stay in the "
             "global grid so a later global pass fills in the rest.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    workdir = resolve_workdir(args.workdir)

    if args.release:
        # Patch the module-level constant so all helpers pick it up
        global OVERTURE_RELEASE
        OVERTURE_RELEASE = args.release
        new_base = f"s3://overturemaps-us-west-2/release/{args.release}"
        for k in THEME_PATHS:
            THEME_PATHS[k] = THEME_PATHS[k].replace(S3_BASE, new_base)

    print(f"Overture tiler  release={OVERTURE_RELEASE}  theme={args.theme}")
    print(f"Workdir: {workdir}")
    if args.dry_run:
        print("DRY RUN — no files will be written\n")

    workdir.mkdir(parents=True, exist_ok=True)

    con = _get_con()

    # Load any tiles already in the manifest (other themes from prior runs)
    manifest: list[TileRecord] = load_existing_manifest(workdir)
    # Remove any existing entries for this theme (will be rebuilt)
    manifest = [r for r in manifest if r.theme != args.theme]

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
        print(f"Region: {region_bbox.min_lon},{region_bbox.min_lat},{region_bbox.max_lon},{region_bbox.max_lat}")

    cells = list(enumerate_start_cells(region_bbox))
    total = len(cells)
    print(f"Start cells: {total}\n")

    for i, addr in enumerate(cells, 1):
        print(f"[{i}/{total}] Cell z={addr.z} x={addr.x} y={addr.y}  bbox={addr.bbox.as_list()}")
        tile_cell(con, args.theme, addr, workdir, args.dry_run, manifest)

    if not args.dry_run:
        write_manifest(workdir, manifest)

    print("\nDone.")


if __name__ == "__main__":
    main()
