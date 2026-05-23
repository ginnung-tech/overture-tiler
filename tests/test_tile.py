"""
Tests for tile.py — run without DuckDB or real parquet files.

Coverage:
    - Quadtree subdivision math (mirrors test_tiler.py for the renamed module)
    - Bbox arithmetic
    - Tile checkpoint load/save round-trip
    - Checkpoint status classification (done/empty/failed)
    - Manifest serialization
    - local_parquet_glob path construction (forward slashes on Windows)
    - resolve_workdir priority
    - tile_filename format
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Make duckdb importable without a real install
# ---------------------------------------------------------------------------
sys.modules.setdefault("duckdb", MagicMock())

sys.path.insert(0, str(Path(__file__).parent.parent))

from tile import (
    Bbox,
    TileAddress,
    TileRecord,
    child_address,
    enumerate_start_cells,
    serialize_manifest,
    tile_filename,
    load_tile_checkpoint,
    save_tile_checkpoint,
    local_parquet_glob,
    resolve_workdir,
    MANIFEST_FIELD_ORDER,
    START_CELL_DEG,
    MIN_CELL_DEG,
    WORLD_BOUNDS,
    TILES_CHECKPOINT_FILENAME,
    ALL_THEMES,
)


# ===========================================================================
# Bbox arithmetic (same as test_tiler.py, now against tile.py)
# ===========================================================================

class TestBboxQuadrants:
    def test_four_quadrants_returned(self):
        bbox = Bbox(0.0, 0.0, 1.0, 1.0)
        children = bbox.quadrants()
        assert len(children) == 4

    def test_quadrants_no_gaps(self):
        bbox = Bbox(10.0, 20.0, 11.0, 21.0)
        children = bbox.quadrants()
        lons = sorted({c.min_lon for c in children} | {c.max_lon for c in children})
        lats = sorted({c.min_lat for c in children} | {c.max_lat for c in children})
        assert lons[0] == pytest.approx(bbox.min_lon)
        assert lons[-1] == pytest.approx(bbox.max_lon)
        assert lats[0] == pytest.approx(bbox.min_lat)
        assert lats[-1] == pytest.approx(bbox.max_lat)

    def test_quadrants_no_overlaps(self):
        bbox = Bbox(0.0, 0.0, 2.0, 2.0)
        children = bbox.quadrants()
        mid_lon = (bbox.min_lon + bbox.max_lon) / 2
        mid_lat = (bbox.min_lat + bbox.max_lat) / 2
        sw = children[0]
        assert sw.min_lon == pytest.approx(bbox.min_lon)
        assert sw.max_lon == pytest.approx(mid_lon)
        assert sw.min_lat == pytest.approx(bbox.min_lat)
        assert sw.max_lat == pytest.approx(mid_lat)
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


# ===========================================================================
# Quadtree subdivision math
# ===========================================================================

class TestChildAddress:
    def _make_parent(self, z=0, x=0, y=0) -> TileAddress:
        return TileAddress(z=z, x=x, y=y, bbox=Bbox(0.0, 0.0, 1.0, 1.0))

    def test_depth_increments(self):
        parent = self._make_parent(z=2)
        child = child_address(parent, parent.bbox.quadrants()[0], 0)
        assert child.z == 3

    def test_sw_child_x_y(self):
        parent = self._make_parent(z=0, x=3, y=5)
        child = child_address(parent, parent.bbox.quadrants()[0], 0)
        assert child.x == 6
        assert child.y == 10

    def test_se_child_x_y(self):
        parent = self._make_parent(z=0, x=3, y=5)
        child = child_address(parent, parent.bbox.quadrants()[1], 1)
        assert child.x == 7
        assert child.y == 10

    def test_nw_child_x_y(self):
        parent = self._make_parent(z=0, x=3, y=5)
        child = child_address(parent, parent.bbox.quadrants()[2], 2)
        assert child.x == 6
        assert child.y == 11

    def test_ne_child_x_y(self):
        parent = self._make_parent(z=0, x=3, y=5)
        child = child_address(parent, parent.bbox.quadrants()[3], 3)
        assert child.x == 7
        assert child.y == 11

    def test_two_level_subdivision_unique_addresses(self):
        root = self._make_parent(z=0, x=0, y=0)
        children = [child_address(root, q, i) for i, q in enumerate(root.bbox.quadrants())]
        grandchildren = []
        for child in children:
            for i, q in enumerate(child.bbox.quadrants()):
                grandchildren.append(child_address(child, q, i))
        addresses = {(gc.z, gc.x, gc.y) for gc in grandchildren}
        assert len(addresses) == 16

    def test_all_four_children_have_distinct_x_y(self):
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
# Tile checkpoint
# ===========================================================================

class TestTileCheckpoint:
    def _make_entry(self, z=0, x=1, y=2, status="done") -> dict:
        return {
            "z": z, "x": x, "y": y,
            "status": status,
            "feature_count": 42,
            "size_bytes": 1024,
            "sha256": "deadbeef",
            "at": "2026-05-03T14:00:00+00:00",
        }

    def test_empty_on_missing_file(self, tmp_path):
        tiles_dir = tmp_path / "tiles" / "buildings"
        tiles_dir.mkdir(parents=True)
        result = load_tile_checkpoint(tiles_dir)
        assert result == {}

    def test_save_then_load_round_trips(self, tmp_path):
        tiles_dir = tmp_path / "tiles" / "buildings"
        tiles_dir.mkdir(parents=True)
        entries = {
            "0:1:2": self._make_entry(0, 1, 2, "done"),
            "0:3:4": self._make_entry(0, 3, 4, "empty"),
            "1:5:6": self._make_entry(1, 5, 6, "failed"),
        }
        save_tile_checkpoint(tiles_dir, entries)
        loaded = load_tile_checkpoint(tiles_dir)
        assert set(loaded.keys()) == {"0:1:2", "0:3:4", "1:5:6"}
        assert loaded["0:1:2"]["status"] == "done"
        assert loaded["0:3:4"]["status"] == "empty"
        assert loaded["1:5:6"]["status"] == "failed"

    def test_checkpoint_file_is_named_correctly(self, tmp_path):
        tiles_dir = tmp_path / "tiles" / "water"
        tiles_dir.mkdir(parents=True)
        entries = {"0:0:0": self._make_entry()}
        save_tile_checkpoint(tiles_dir, entries)
        assert (tiles_dir / TILES_CHECKPOINT_FILENAME).exists()
        assert TILES_CHECKPOINT_FILENAME == "_tiles.json"

    def test_checkpoint_file_is_valid_json_array(self, tmp_path):
        tiles_dir = tmp_path / "tiles" / "land"
        tiles_dir.mkdir(parents=True)
        entries = {"0:0:0": self._make_entry()}
        save_tile_checkpoint(tiles_dir, entries)
        raw = json.loads((tiles_dir / TILES_CHECKPOINT_FILENAME).read_text())
        assert isinstance(raw, list)

    def test_corrupt_checkpoint_returns_empty(self, tmp_path):
        tiles_dir = tmp_path / "tiles" / "land"
        tiles_dir.mkdir(parents=True)
        (tiles_dir / TILES_CHECKPOINT_FILENAME).write_text("not json", encoding="utf-8")
        result = load_tile_checkpoint(tiles_dir)
        assert result == {}

    def test_checkpoint_keyed_by_z_x_y_string(self, tmp_path):
        tiles_dir = tmp_path / "tiles" / "segments"
        tiles_dir.mkdir(parents=True)
        entries = {"3:14:7": self._make_entry(3, 14, 7)}
        save_tile_checkpoint(tiles_dir, entries)
        loaded = load_tile_checkpoint(tiles_dir)
        assert "3:14:7" in loaded

    def test_done_tiles_have_feature_count_and_size(self, tmp_path):
        tiles_dir = tmp_path / "tiles" / "buildings"
        tiles_dir.mkdir(parents=True)
        # Use z=0, x=0, y=0 so the key matches _tile_key(0, 0, 0) = "0:0:0"
        entry = self._make_entry(z=0, x=0, y=0, status="done")
        entry["feature_count"] = 9999
        entry["size_bytes"] = 5_000_000
        entries = {"0:0:0": entry}
        save_tile_checkpoint(tiles_dir, entries)
        loaded = load_tile_checkpoint(tiles_dir)
        assert loaded["0:0:0"]["feature_count"] == 9999
        assert loaded["0:0:0"]["size_bytes"] == 5_000_000


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
            theme="water", z=3, x=14, y=7,
            bbox=[1.5, 2.5, 3.5, 4.5],
            size_bytes=8_388_608,
            feature_count=12_345,
        )
        parsed = json.loads(serialize_manifest([record]))
        row = parsed[0]
        assert row["theme"] == "water"
        assert row["z"] == 3
        assert row["feature_count"] == 12_345

    def test_empty_manifest_is_empty_array(self):
        result = serialize_manifest([])
        assert json.loads(result) == []


# ===========================================================================
# local_parquet_glob — forward slashes cross-platform
# ===========================================================================

class TestLocalParquetGlob:
    def test_glob_contains_theme(self, tmp_path):
        glob = local_parquet_glob("buildings", tmp_path)
        assert "buildings" in glob

    def test_glob_ends_with_parquet_wildcard(self, tmp_path):
        glob = local_parquet_glob("segments", tmp_path)
        assert glob.endswith("/**/*.parquet")

    def test_glob_uses_forward_slashes(self, tmp_path):
        """DuckDB requires forward slashes even on Windows."""
        glob = local_parquet_glob("water", tmp_path)
        assert "\\" not in glob

    def test_glob_points_to_raw_subdir(self, tmp_path):
        glob = local_parquet_glob("land", tmp_path)
        assert "/raw/land/" in glob

    def test_glob_is_posix_path(self, tmp_path):
        glob = local_parquet_glob("infrastructure", tmp_path)
        # Must be a string starting with drive letter or /
        assert isinstance(glob, str)
        # No backslashes allowed
        assert "\\" not in glob


# ===========================================================================
# resolve_workdir priority
# ===========================================================================

class TestResolveWorkdir:
    def test_cli_flag_takes_priority(self, tmp_path):
        with patch.dict(os.environ, {"OVERTURE_WORKDIR": "/env/path"}):
            result = resolve_workdir(str(tmp_path))
        assert result == tmp_path

    def test_env_var_used_when_no_cli(self):
        with patch.dict(os.environ, {"OVERTURE_WORKDIR": "/from/env"}, clear=False):
            result = resolve_workdir(None)
        assert result == Path("/from/env")

    def test_returns_path_object(self):
        result = resolve_workdir("/some/path")
        assert isinstance(result, Path)


# ===========================================================================
# All themes present
# ===========================================================================

class TestAllThemes:
    def test_expected_themes_present(self):
        expected = {"buildings", "segments", "land_use", "water", "land", "infrastructure"}
        assert set(ALL_THEMES) == expected

    def test_theme_list_not_empty(self):
        assert len(ALL_THEMES) > 0
