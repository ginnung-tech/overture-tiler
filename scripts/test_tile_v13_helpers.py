"""test_tile_v13_helpers.py — pure-Python tests for v13 helpers.

No DuckDB. No I/O. No network. Verifies:

- `lng_lat_to_mercator_xy` round-trips at z=14 and z=6 for representative
  points (Copenhagen, Greenwich, antipodes near each pole).
- `quadkey_extent` widths match Earth circumference / 2^z at the equator.
- `lng_range_to_z6_keys(0, 10, LAT_BOUNDS)` returns the expected count of
  z=6 keys.
- `tile_path` produces `tiles/<z>/<x>/<y>.parquet`.

Run from `scripts/overture-tiler/scripts/`:

    python -m pytest test_tile_v13_helpers.py -v
"""
from __future__ import annotations

import math
from pathlib import Path

import pytest

from tile_v13_helpers import (
    LAT_BOUNDS,
    LNG_BOUNDS,
    Z_BUCKET,
    Z_MAX_DEFAULT,
    lng_lat_to_mercator_xy,
    lng_range_to_z6_keys,
    quadkey_extent,
    tile_path,
)


# ---------------------------------------------------------------------------
# lng_lat_to_mercator_xy round-trip
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("lng,lat,z", [
    (0.0, 0.0, 0),       # null island, z=0
    (0.0, 0.0, 14),      # null island, z=14
    (12.5683, 55.6761, 14),   # Copenhagen, z=14
    (12.5683, 55.6761, 6),    # Copenhagen, z=6 (bucket level)
    (-122.4194, 37.7749, 14), # San Francisco, z=14
    (139.6917, 35.6895, 14),  # Tokyo, z=14
    (-43.1729, -22.9068, 14), # Rio de Janeiro, z=14
    (174.7633, -36.8485, 14), # Auckland, z=14
])
def test_lng_lat_round_trips_within_tile_extent(lng: float, lat: float, z: int) -> None:
    """Projecting a point and reading back its tile extent must contain the point."""
    x, y = lng_lat_to_mercator_xy(lng, lat, z)
    n = 1 << z
    assert 0 <= x < n, f"x={x} out of [0, {n})"
    assert 0 <= y < n, f"y={y} out of [0, {n})"

    ext = quadkey_extent(z, x, y)
    # The tile is half-open at the eastern / southern edges in the standard
    # slippy-map convention; a point exactly on max_lng / min_lat would map
    # to the next tile. Use strict-less / strict-greater on those edges.
    assert ext.min_lng <= lng < ext.max_lng or math.isclose(lng, ext.max_lng), \
        f"lng {lng} not in [{ext.min_lng}, {ext.max_lng})"
    # lat clamping at LAT_BOUNDS may shift; our test points are inside.
    assert ext.min_lat < lat <= ext.max_lat or math.isclose(lat, ext.min_lat), \
        f"lat {lat} not in ({ext.min_lat}, {ext.max_lat}]"


def test_lng_lat_clamps_polar_extremes() -> None:
    """Latitudes outside ±85.05° clamp into the last row."""
    z = 14
    n = 1 << z
    # North of the Mercator extent.
    _, y = lng_lat_to_mercator_xy(0.0, 89.5, z)
    assert y == 0, f"north pole should clamp to y=0, got {y}"
    # South of the extent.
    _, y2 = lng_lat_to_mercator_xy(0.0, -89.5, z)
    assert y2 == n - 1, f"south pole should clamp to y={n-1}, got {y2}"


def test_lng_lat_west_east_corners_z0() -> None:
    """At z=0 the whole world is one tile; any in-range point returns (0, 0)."""
    for lng, lat in [(-179.9, 0.0), (0.0, 0.0), (179.9, 0.0), (1.0, 84.0), (-1.0, -84.0)]:
        assert lng_lat_to_mercator_xy(lng, lat, 0) == (0, 0), (lng, lat)


# ---------------------------------------------------------------------------
# quadkey_extent
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("z", [0, 1, 2, 6, 10, 14, 15])
def test_quadkey_extent_width_matches_360_over_2z(z: int) -> None:
    """Equatorial tile width in degrees lng = 360 / 2^z exactly."""
    n = 1 << z
    expected_width = 360.0 / n
    # Pick the equator-anchored tile in the middle.
    x = n // 2
    y = n // 2
    ext = quadkey_extent(z, x, y)
    width = ext.max_lng - ext.min_lng
    assert math.isclose(width, expected_width, rel_tol=1e-12), \
        f"z={z}: width={width}, expected {expected_width}"


def test_quadkey_extent_z0_is_world() -> None:
    """At z=0 the single tile spans -180..+180 lng and ±85.05 lat."""
    ext = quadkey_extent(0, 0, 0)
    assert math.isclose(ext.min_lng, -180.0)
    assert math.isclose(ext.max_lng, 180.0)
    # lat extent should be Mercator's native bounds (the constant LAT_BOUNDS).
    assert math.isclose(ext.min_lat, LAT_BOUNDS[0], abs_tol=1e-9), ext.min_lat
    assert math.isclose(ext.max_lat, LAT_BOUNDS[1], abs_tol=1e-9), ext.max_lat


def test_quadkey_extent_z1_quadrants() -> None:
    """At z=1 the world is 4 tiles meeting at (0, 0)."""
    nw = quadkey_extent(1, 0, 0)
    ne = quadkey_extent(1, 1, 0)
    sw = quadkey_extent(1, 0, 1)
    se = quadkey_extent(1, 1, 1)
    # Lng halves: -180..0 vs 0..180.
    assert math.isclose(nw.min_lng, -180.0)
    assert math.isclose(nw.max_lng, 0.0)
    assert math.isclose(ne.min_lng, 0.0)
    assert math.isclose(ne.max_lng, 180.0)
    # Lat: north half is y=0, south half is y=1. They meet at lat=0.
    assert math.isclose(nw.min_lat, 0.0, abs_tol=1e-9), nw.min_lat
    assert math.isclose(sw.max_lat, 0.0, abs_tol=1e-9), sw.max_lat
    # SW and SE share min_lat with each other.
    assert math.isclose(sw.min_lat, se.min_lat)


def test_quadkey_extent_rejects_invalid_addresses() -> None:
    with pytest.raises(ValueError):
        quadkey_extent(2, 4, 0)   # x out of range at z=2 (max 3)
    with pytest.raises(ValueError):
        quadkey_extent(2, 0, 4)
    with pytest.raises(ValueError):
        quadkey_extent(-1, 0, 0)


# ---------------------------------------------------------------------------
# lng_range_to_z6_keys
# ---------------------------------------------------------------------------

def test_lng_range_to_z6_keys_zero_to_ten_full_lat() -> None:
    """[0°, 10°) at z=6 spans (10° / (360°/64)) = ~1.78 columns -> 2 cols, all 64 rows."""
    keys = lng_range_to_z6_keys(0.0, 10.0, LAT_BOUNDS)
    n = 1 << Z_BUCKET  # 64
    # 2 columns × 64 rows.
    assert len(keys) == 2 * n, f"expected {2*n}, got {len(keys)}"
    cols = sorted({k[0] for k in keys})
    rows = sorted({k[1] for k in keys})
    assert cols == [32, 33], cols
    assert rows == list(range(n)), rows[:5]


def test_lng_range_to_z6_keys_full_world_lat_band() -> None:
    """A full-lng band returns every (x, y) in the equator-band rows."""
    keys = lng_range_to_z6_keys(-180.0, 180.0, LAT_BOUNDS)
    n = 1 << Z_BUCKET
    assert len(keys) == n * n, f"expected {n*n} for whole world, got {len(keys)}"


def test_lng_range_to_z6_keys_validates_args() -> None:
    with pytest.raises(ValueError):
        lng_range_to_z6_keys(10.0, 0.0, LAT_BOUNDS)  # hi <= lo
    with pytest.raises(ValueError):
        lng_range_to_z6_keys(0.0, 10.0, (10.0, 0.0))  # lat hi <= lat lo


def test_lng_range_to_z6_keys_narrow_lat_band() -> None:
    """A narrow lat slice around the equator returns fewer rows."""
    # About ±5° around the equator -> in z=6 (n=64), each row covers about
    # LAT_BOUNDS / n vertically near the equator (~2.66°/row ish), so 5°
    # straddles ~3-4 rows around y=32.
    keys = lng_range_to_z6_keys(0.0, 10.0, (-5.0, 5.0))
    cols = sorted({k[0] for k in keys})
    assert cols == [32, 33], cols
    rows = sorted({k[1] for k in keys})
    # Expect a small contiguous slice of rows centred on y=32.
    assert all(28 <= r <= 36 for r in rows), rows
    assert min(rows) <= 32 <= max(rows)


# ---------------------------------------------------------------------------
# tile_path
# ---------------------------------------------------------------------------

def test_tile_path_layout() -> None:
    workdir = Path("/Volumes/SSD/overture")
    p = tile_path(workdir, 14, 8763, 5128)
    assert p.as_posix().endswith("tiles/14/8763/5128.parquet"), p.as_posix()
    # Parents resolve correctly.
    assert p.parent.name == "8763"
    assert p.parent.parent.name == "14"
    assert p.parent.parent.parent.name == "tiles"


def test_tile_path_z_max_default_consistent_with_constant() -> None:
    """tile_path accepts Z_MAX_DEFAULT; trivially exercises the int path."""
    p = tile_path(Path("/x"), Z_MAX_DEFAULT, 0, 0)
    assert p.name == "0.parquet"
    assert str(Z_MAX_DEFAULT) in p.as_posix()


# ---------------------------------------------------------------------------
# Sanity: LAT_BOUNDS is the Mercator-native extent
# ---------------------------------------------------------------------------

def test_lat_bounds_is_mercator_native() -> None:
    """arctan(sinh(pi)) ≈ 85.05112877980659 degrees."""
    expected = math.degrees(math.atan(math.sinh(math.pi)))
    assert math.isclose(LAT_BOUNDS[1], expected, abs_tol=1e-9), (LAT_BOUNDS[1], expected)
    assert math.isclose(LAT_BOUNDS[0], -expected, abs_tol=1e-9), LAT_BOUNDS[0]


def test_lng_bounds_is_full_world() -> None:
    assert LNG_BOUNDS == (-180.0, 180.0)
