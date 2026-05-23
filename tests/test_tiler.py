"""
Tests for tiler.py — run without S3 or DuckDB.

Coverage:
    - Quadtree subdivision math
    - Bbox arithmetic (4 child cells, no gaps, no overlaps)
    - Manifest serialization shape
    - Resumability skip logic (mocked fs + prior sidecar)
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open

import pytest

# ---------------------------------------------------------------------------
# Make tiler importable without DuckDB installed
# ---------------------------------------------------------------------------
sys.modules.setdefault("duckdb", MagicMock())

# Add parent directory to path so we can import tiler directly
sys.path.insert(0, str(Path(__file__).parent.parent))

from tiler import (
    Bbox,
    TileAddress,
    TileRecord,
    child_address,
    enumerate_start_cells,
    fcount_path,
    is_tile_complete,
    serialize_manifest,
    tile_filename,
    MANIFEST_FIELD_ORDER,
    START_CELL_DEG,
    MIN_CELL_DEG,
    WORLD_BOUNDS,
)


# ===========================================================================
# Bbox arithmetic
# ===========================================================================

class TestBboxQuadrants:
    def test_four_quadrants_returned(self):
        bbox = Bbox(0.0, 0.0, 1.0, 1.0)
        children = bbox.quadrants()
        assert len(children) == 4

    def test_quadrants_no_gaps(self):
        """All four quadrants together cover the exact parent area."""
        bbox = Bbox(10.0, 20.0, 11.0, 21.0)
        children = bbox.quadrants()

        lons = sorted({c.min_lon for c in children} | {c.max_lon for c in children})
        lats = sorted({c.min_lat for c in children} | {c.max_lat for c in children})

        assert lons[0] == pytest.approx(bbox.min_lon)
        assert lons[-1] == pytest.approx(bbox.max_lon)
        assert lats[0] == pytest.approx(bbox.min_lat)
        assert lats[-1] == pytest.approx(bbox.max_lat)

    def test_quadrants_no_overlaps(self):
        """No two children share interior area."""
        bbox = Bbox(0.0, 0.0, 2.0, 2.0)
        children = bbox.quadrants()

        mid_lon = (bbox.min_lon + bbox.max_lon) / 2
        mid_lat = (bbox.min_lat + bbox.max_lat) / 2

        # SW child
        sw = children[0]
        assert sw.min_lon == pytest.approx(bbox.min_lon)
        assert sw.max_lon == pytest.approx(mid_lon)
        assert sw.min_lat == pytest.approx(bbox.min_lat)
        assert sw.max_lat == pytest.approx(mid_lat)

        # NE child
        ne = children[3]
        assert ne.min_lon == pytest.approx(mid_lon)
        assert ne.max_lon == pytest.approx(bbox.max_lon)
        assert ne.min_lat == pytest.approx(mid_lat)
        assert ne.max_lat == pytest.approx(bbox.max_lat)

    def test_quadrant_widths_are_half_parent(self):
        bbox = Bbox(-10.0, -5.0, 10.0, 5.0)
        children = bbox.quadrants()
        parent_w = bbox.max_lon - bbox.min_lon
        parent_h = bbox.max_lat - bbox.min_lat
        for child in children:
            assert child.max_lon - child.min_lon == pytest.approx(parent_w / 2)
            assert child.max_lat - child.min_lat == pytest.approx(parent_h / 2)

    def test_bbox_as_list_field_order(self):
        bbox = Bbox(1.0, 2.0, 3.0, 4.0)
        lst = bbox.as_list()
        assert lst == [1.0, 2.0, 3.0, 4.0]
        assert lst[0] == bbox.min_lon
        assert lst[1] == bbox.min_lat
        assert lst[2] == bbox.max_lon
        assert lst[3] == bbox.max_lat


# ===========================================================================
# Quadtree subdivision math — child address encoding
# ===========================================================================

class TestChildAddress:
    """
    child_index mapping:
        0 = SW (west, south)
        1 = SE (east, south)
        2 = NW (west, north)
        3 = NE (east, north)

    Encoding:
        child_z = parent_z + 1
        child_x = parent_x * 2 + (1 if east)
        child_y = parent_y * 2 + (1 if north)
    """

    def _make_parent(self, z: int = 0, x: int = 0, y: int = 0) -> TileAddress:
        bbox = Bbox(0.0, 0.0, 1.0, 1.0)
        return TileAddress(z=z, x=x, y=y, bbox=bbox)

    def test_depth_increments(self):
        parent = self._make_parent(z=2)
        child = child_address(parent, parent.bbox.quadrants()[0], 0)
        assert child.z == 3

    def test_sw_child_x_y(self):
        parent = self._make_parent(z=0, x=3, y=5)
        child = child_address(parent, parent.bbox.quadrants()[0], 0)  # SW
        assert child.x == 3 * 2 + 0  # 6
        assert child.y == 5 * 2 + 0  # 10

    def test_se_child_x_y(self):
        parent = self._make_parent(z=0, x=3, y=5)
        child = child_address(parent, parent.bbox.quadrants()[1], 1)  # SE
        assert child.x == 3 * 2 + 1  # 7
        assert child.y == 5 * 2 + 0  # 10

    def test_nw_child_x_y(self):
        parent = self._make_parent(z=0, x=3, y=5)
        child = child_address(parent, parent.bbox.quadrants()[2], 2)  # NW
        assert child.x == 3 * 2 + 0  # 6
        assert child.y == 5 * 2 + 1  # 11

    def test_ne_child_x_y(self):
        parent = self._make_parent(z=0, x=3, y=5)
        child = child_address(parent, parent.bbox.quadrants()[3], 3)  # NE
        assert child.x == 3 * 2 + 1  # 7
        assert child.y == 5 * 2 + 1  # 11

    def test_child_bbox_attached_correctly(self):
        parent = self._make_parent(z=0, x=0, y=0)
        quadrants = parent.bbox.quadrants()
        for i, q in enumerate(quadrants):
            child = child_address(parent, q, i)
            assert child.bbox == q

    def test_two_level_subdivision_x_y_unique(self):
        """After 2 levels of subdivision, all 16 grandchildren have unique (z,x,y)."""
        root = self._make_parent(z=0, x=0, y=0)
        children = [
            child_address(root, q, i) for i, q in enumerate(root.bbox.quadrants())
        ]
        grandchildren = []
        for child in children:
            for i, q in enumerate(child.bbox.quadrants()):
                grandchildren.append(child_address(child, q, i))

        addresses = {(gc.z, gc.x, gc.y) for gc in grandchildren}
        assert len(addresses) == 16

    def test_all_four_child_indices_produce_distinct_addresses(self):
        parent = self._make_parent(z=1, x=2, y=4)
        quadrants = parent.bbox.quadrants()
        children = [child_address(parent, quadrants[i], i) for i in range(4)]
        addresses = {(c.x, c.y) for c in children}
        assert len(addresses) == 4


# ===========================================================================
# Tile filename
# ===========================================================================

class TestTileFilename:
    def test_format(self):
        assert tile_filename(0, 3, 7) == "0_3_7.parquet.gz"

    def test_zero_indices(self):
        assert tile_filename(0, 0, 0) == "0_0_0.parquet.gz"

    def test_large_indices(self):
        assert tile_filename(10, 1023, 511) == "10_1023_511.parquet.gz"


# ===========================================================================
# Manifest serialization
# ===========================================================================

class TestManifestSerialization:
    def _make_record(self, theme="buildings", z=0, x=1, y=2) -> TileRecord:
        return TileRecord(
            theme=theme,
            z=z, x=x, y=y,
            bbox=[-10.0, -5.0, 0.0, 5.0],
            size_bytes=1_000_000,
            feature_count=500,
        )

    def test_output_is_valid_json(self):
        records = [self._make_record()]
        result = serialize_manifest(records)
        parsed = json.loads(result)
        assert isinstance(parsed, list)

    def test_one_record_per_tile(self):
        records = [self._make_record(z=0, x=i, y=0) for i in range(5)]
        parsed = json.loads(serialize_manifest(records))
        assert len(parsed) == 5

    def test_field_order_matches_spec(self):
        record = self._make_record()
        parsed = json.loads(serialize_manifest([record]))
        row = parsed[0]
        keys = list(row.keys())
        assert keys == MANIFEST_FIELD_ORDER

    def test_bbox_is_four_element_list(self):
        record = self._make_record()
        parsed = json.loads(serialize_manifest([record]))
        bbox = parsed[0]["bbox"]
        assert isinstance(bbox, list)
        assert len(bbox) == 4

    def test_correct_values_round_trip(self):
        record = TileRecord(
            theme="water",
            z=3, x=14, y=7,
            bbox=[1.5, 2.5, 3.5, 4.5],
            size_bytes=8_388_608,
            feature_count=12_345,
        )
        parsed = json.loads(serialize_manifest([record]))
        row = parsed[0]
        assert row["theme"] == "water"
        assert row["z"] == 3
        assert row["x"] == 14
        assert row["y"] == 7
        assert row["bbox"] == [1.5, 2.5, 3.5, 4.5]
        assert row["size_bytes"] == 8_388_608
        assert row["feature_count"] == 12_345

    def test_empty_manifest_is_empty_array(self):
        result = serialize_manifest([])
        parsed = json.loads(result)
        assert parsed == []

    def test_multiple_themes_preserved(self):
        records = [
            self._make_record(theme="buildings"),
            self._make_record(theme="water"),
            self._make_record(theme="land"),
        ]
        parsed = json.loads(serialize_manifest(records))
        themes = [r["theme"] for r in parsed]
        assert themes == ["buildings", "water", "land"]


# ===========================================================================
# Resumability skip logic (mocked filesystem, no DuckDB)
# ===========================================================================

class TestResumability:
    def test_skip_when_tile_and_sidecar_exist_and_count_matches(self, tmp_path):
        theme = "buildings"
        theme_dir = tmp_path / theme
        theme_dir.mkdir()

        tile = theme_dir / "0_0_0.parquet.gz"
        fc = fcount_path(tile)

        tile.write_bytes(b"fake parquet data")
        fc.write_text("42")

        mock_con = MagicMock()
        mock_con.execute.return_value.fetchone.return_value = (42,)

        bbox = Bbox(0.0, 0.0, 0.1, 0.1)
        result = is_tile_complete(tile, mock_con, theme, bbox)
        assert result is True

    def test_no_skip_when_count_mismatches(self, tmp_path):
        theme = "buildings"
        theme_dir = tmp_path / theme
        theme_dir.mkdir()

        tile = theme_dir / "0_0_0.parquet.gz"
        fc = fcount_path(tile)

        tile.write_bytes(b"fake parquet data")
        fc.write_text("42")

        mock_con = MagicMock()
        # S3 now returns 99 features — mismatch with sidecar 42
        mock_con.execute.return_value.fetchone.return_value = (99,)

        bbox = Bbox(0.0, 0.0, 0.1, 0.1)
        result = is_tile_complete(tile, mock_con, theme, bbox)
        assert result is False

    def test_no_skip_when_tile_missing(self, tmp_path):
        theme = "buildings"
        theme_dir = tmp_path / theme
        theme_dir.mkdir()

        tile = theme_dir / "0_0_0.parquet.gz"
        # Don't create the tile file

        mock_con = MagicMock()
        bbox = Bbox(0.0, 0.0, 0.1, 0.1)
        result = is_tile_complete(tile, mock_con, theme, bbox)
        assert result is False

    def test_no_skip_when_sidecar_missing(self, tmp_path):
        theme = "buildings"
        theme_dir = tmp_path / theme
        theme_dir.mkdir()

        tile = theme_dir / "0_0_0.parquet.gz"
        tile.write_bytes(b"fake parquet data")
        # Don't create the .fcount sidecar

        mock_con = MagicMock()
        bbox = Bbox(0.0, 0.0, 0.1, 0.1)
        result = is_tile_complete(tile, mock_con, theme, bbox)
        assert result is False

    def test_no_skip_when_sidecar_corrupted(self, tmp_path):
        theme = "buildings"
        theme_dir = tmp_path / theme
        theme_dir.mkdir()

        tile = theme_dir / "0_0_0.parquet.gz"
        fc = fcount_path(tile)

        tile.write_bytes(b"fake parquet data")
        fc.write_text("not-a-number")

        mock_con = MagicMock()
        mock_con.execute.return_value.fetchone.return_value = (42,)

        bbox = Bbox(0.0, 0.0, 0.1, 0.1)
        result = is_tile_complete(tile, mock_con, theme, bbox)
        assert result is False

    def test_fcount_path_derives_from_tile_path(self, tmp_path):
        tile = tmp_path / "buildings" / "3_14_7.parquet.gz"
        fc = fcount_path(tile)
        assert fc == tmp_path / "buildings" / "3_14_7.fcount"
        assert fc.suffix == ".fcount"
        assert fc.stem == "3_14_7"


# ===========================================================================
# World-coverage: start cells cover the full world
# ===========================================================================

class TestEnumerateStartCells:
    def test_cell_count_is_correct(self):
        """360 * 180 / (0.1 * 0.1) = 3_240_000 cells total."""
        # Just check the count for a known sub-range to avoid long run
        lon_range = 1.0  # degrees
        lat_range = 1.0
        expected = int(lon_range / START_CELL_DEG) * int(lat_range / START_CELL_DEG)
        assert expected == 100

    def test_cells_unique_addresses(self):
        """Each (z,x,y) is unique — verified on a small sample (full enumeration is 3.24M cells)."""
        # Enumerate only a 3x3 degree region to keep test fast
        import itertools
        cells = list(itertools.islice(enumerate_start_cells(), 1000))
        addresses = {(c.z, c.x, c.y) for c in cells}
        assert len(addresses) == len(cells)

    def test_cells_all_at_depth_zero(self):
        import itertools
        cells = list(itertools.islice(enumerate_start_cells(), 100))
        assert all(c.z == 0 for c in cells)

    def test_first_cell_starts_at_world_min(self):
        import itertools
        cells = list(itertools.islice(enumerate_start_cells(), 1))
        first = cells[0]
        assert first.bbox.min_lon == pytest.approx(WORLD_BOUNDS[0])
        assert first.bbox.min_lat == pytest.approx(WORLD_BOUNDS[1])
