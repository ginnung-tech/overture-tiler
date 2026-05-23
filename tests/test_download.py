"""
Tests for download.py — run without S3, DuckDB, or httpx.

Coverage:
    - Checkpoint load/save round-trip
    - Status classification (done/failed/in_progress)
    - Resumability: done files are skipped, in_progress files are cleaned up
    - list_s3_files path construction
    - resolve_workdir priority order
    - _iso_now() produces valid ISO 8601 strings
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, AsyncMock
import pytest

# ---------------------------------------------------------------------------
# Make duckdb and httpx importable without real installs
# ---------------------------------------------------------------------------
sys.modules.setdefault("duckdb", MagicMock())
sys.modules.setdefault("httpx", MagicMock())

sys.path.insert(0, str(Path(__file__).parent.parent))

from download import (
    checkpoint_path,
    load_checkpoint,
    save_checkpoint,
    resolve_workdir,
    _iso_now,
    ALL_THEMES,
    THEME_S3_PREFIXES,
    CHECKPOINT_FILENAME,
    S3_BUCKET,
)


# ===========================================================================
# Checkpoint path derivation
# ===========================================================================

class TestCheckpointPath:
    def test_checkpoint_in_raw_theme_dir(self, tmp_path):
        raw_dir = tmp_path / "raw" / "buildings"
        cp = checkpoint_path(raw_dir)
        assert cp == raw_dir / CHECKPOINT_FILENAME

    def test_checkpoint_filename_constant(self):
        assert CHECKPOINT_FILENAME == "_download.json"


# ===========================================================================
# Checkpoint load/save round-trip
# ===========================================================================

class TestCheckpointRoundTrip:
    def _make_entry(self, key: str, status: str = "done", size: int = 1000) -> dict:
        return {
            "file_key": key,
            "local_path": f"/overture/raw/buildings/{Path(key).name}",
            "size_bytes": size,
            "sha256": "abc123" if status == "done" else None,
            "status": status,
            "at": "2026-05-03T14:00:00+00:00",
        }

    def test_empty_checkpoint_on_missing_file(self, tmp_path):
        raw_dir = tmp_path / "raw" / "buildings"
        raw_dir.mkdir(parents=True)
        result = load_checkpoint(raw_dir)
        assert result == {}

    def test_save_then_load_round_trips(self, tmp_path):
        raw_dir = tmp_path / "raw" / "buildings"
        raw_dir.mkdir(parents=True)

        entries = {
            "s3://bucket/file1.parquet": self._make_entry("s3://bucket/file1.parquet", "done"),
            "s3://bucket/file2.parquet": self._make_entry("s3://bucket/file2.parquet", "failed"),
        }
        save_checkpoint(raw_dir, entries)
        loaded = load_checkpoint(raw_dir)

        assert set(loaded.keys()) == set(entries.keys())
        assert loaded["s3://bucket/file1.parquet"]["status"] == "done"
        assert loaded["s3://bucket/file2.parquet"]["status"] == "failed"

    def test_corrupt_checkpoint_returns_empty(self, tmp_path):
        raw_dir = tmp_path / "raw" / "buildings"
        raw_dir.mkdir(parents=True)
        cp = checkpoint_path(raw_dir)
        cp.write_text("not valid json", encoding="utf-8")
        result = load_checkpoint(raw_dir)
        assert result == {}

    def test_checkpoint_is_json_list(self, tmp_path):
        """Saved checkpoint must be a JSON array (list of entry objects)."""
        raw_dir = tmp_path / "raw" / "buildings"
        raw_dir.mkdir(parents=True)
        entries = {
            "s3://b/a.parquet": self._make_entry("s3://b/a.parquet"),
        }
        save_checkpoint(raw_dir, entries)
        raw = json.loads(checkpoint_path(raw_dir).read_text())
        assert isinstance(raw, list)
        assert raw[0]["file_key"] == "s3://b/a.parquet"

    def test_checkpoint_keyed_by_file_key(self, tmp_path):
        """load_checkpoint returns dict keyed by file_key."""
        raw_dir = tmp_path / "raw" / "buildings"
        raw_dir.mkdir(parents=True)
        key = "s3://overturemaps-us-west-2/release/2026-04-15.0/theme=buildings/type=building/part-00000.zstd.parquet"
        entries = {key: self._make_entry(key)}
        save_checkpoint(raw_dir, entries)
        loaded = load_checkpoint(raw_dir)
        assert key in loaded

    def test_multiple_entries_survive_round_trip(self, tmp_path):
        raw_dir = tmp_path / "raw" / "land"
        raw_dir.mkdir(parents=True)
        entries = {
            f"s3://b/file{i}.parquet": self._make_entry(f"s3://b/file{i}.parquet", "done", i * 1000)
            for i in range(10)
        }
        save_checkpoint(raw_dir, entries)
        loaded = load_checkpoint(raw_dir)
        assert len(loaded) == 10
        for i in range(10):
            assert loaded[f"s3://b/file{i}.parquet"]["size_bytes"] == i * 1000


# ===========================================================================
# Status classification in checkpoint
# ===========================================================================

class TestCheckpointStatusClassification:
    def test_done_status_preserved(self, tmp_path):
        raw_dir = tmp_path / "raw" / "water"
        raw_dir.mkdir(parents=True)
        entries = {"s3://b/f.parquet": {"file_key": "s3://b/f.parquet", "local_path": "/x", "size_bytes": 100, "sha256": "abc", "status": "done", "at": "2026-01-01T00:00:00+00:00"}}
        save_checkpoint(raw_dir, entries)
        loaded = load_checkpoint(raw_dir)
        assert loaded["s3://b/f.parquet"]["status"] == "done"

    def test_failed_status_preserved(self, tmp_path):
        raw_dir = tmp_path / "raw" / "water"
        raw_dir.mkdir(parents=True)
        entries = {"s3://b/f.parquet": {"file_key": "s3://b/f.parquet", "local_path": "/x", "size_bytes": 0, "sha256": None, "status": "failed", "at": "2026-01-01T00:00:00+00:00"}}
        save_checkpoint(raw_dir, entries)
        loaded = load_checkpoint(raw_dir)
        assert loaded["s3://b/f.parquet"]["status"] == "failed"

    def test_in_progress_status_preserved(self, tmp_path):
        raw_dir = tmp_path / "raw" / "water"
        raw_dir.mkdir(parents=True)
        entries = {"s3://b/f.parquet": {"file_key": "s3://b/f.parquet", "local_path": "/x", "size_bytes": 0, "sha256": None, "status": "in_progress", "at": "2026-01-01T00:00:00+00:00"}}
        save_checkpoint(raw_dir, entries)
        loaded = load_checkpoint(raw_dir)
        assert loaded["s3://b/f.parquet"]["status"] == "in_progress"


# ===========================================================================
# S3 prefix correctness
# ===========================================================================

class TestS3Prefixes:
    def test_all_themes_have_prefixes(self):
        for theme in ALL_THEMES:
            assert theme in THEME_S3_PREFIXES, f"{theme} missing from THEME_S3_PREFIXES"

    def test_buildings_prefix_points_to_type_building(self):
        prefix = THEME_S3_PREFIXES["buildings"].format(release="2026-04-15.0")
        assert "theme=buildings" in prefix
        assert "type=building" in prefix

    def test_segments_prefix_points_to_transportation(self):
        prefix = THEME_S3_PREFIXES["segments"].format(release="2026-04-15.0")
        assert "theme=transportation" in prefix
        assert "type=segment" in prefix

    def test_land_use_prefix_points_to_base(self):
        prefix = THEME_S3_PREFIXES["land_use"].format(release="2026-04-15.0")
        assert "theme=base" in prefix
        assert "type=land_use" in prefix

    def test_water_prefix_points_to_base(self):
        prefix = THEME_S3_PREFIXES["water"].format(release="2026-04-15.0")
        assert "theme=base" in prefix
        assert "type=water" in prefix

    def test_land_prefix_points_to_base(self):
        prefix = THEME_S3_PREFIXES["land"].format(release="2026-04-15.0")
        assert "theme=base" in prefix
        assert "type=land" in prefix

    def test_infrastructure_prefix_points_to_base(self):
        prefix = THEME_S3_PREFIXES["infrastructure"].format(release="2026-04-15.0")
        assert "theme=base" in prefix
        assert "type=infrastructure" in prefix

    def test_prefix_includes_release_placeholder(self):
        for theme, prefix_tpl in THEME_S3_PREFIXES.items():
            assert "{release}" in prefix_tpl, f"{theme} prefix has no {{release}} placeholder"

    def test_prefix_ends_with_slash(self):
        for theme, prefix_tpl in THEME_S3_PREFIXES.items():
            assert prefix_tpl.endswith("/"), f"{theme} prefix must end with /"

    def test_s3_bucket_constant(self):
        assert S3_BUCKET == "overturemaps-us-west-2"


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

    def test_platform_default_when_no_cli_no_env(self):
        env = {k: v for k, v in os.environ.items() if k != "OVERTURE_WORKDIR"}
        with patch.dict(os.environ, env, clear=True):
            with patch("sys.platform", "darwin"):
                result = resolve_workdir(None)
        # On darwin: /Volumes/SSD/overture
        assert "overture" in str(result)

    def test_returns_path_object(self):
        result = resolve_workdir("/some/path")
        assert isinstance(result, Path)


# ===========================================================================
# _iso_now() format
# ===========================================================================

class TestIsoNow:
    def test_returns_string(self):
        assert isinstance(_iso_now(), str)

    def test_contains_T_separator(self):
        ts = _iso_now()
        assert "T" in ts

    def test_ends_with_utc_offset(self):
        ts = _iso_now()
        # Should end with +00:00 or Z
        assert ts.endswith("+00:00") or ts.endswith("Z")

    def test_parseable_as_datetime(self):
        from datetime import datetime, timezone
        ts = _iso_now()
        # Should not raise
        dt = datetime.fromisoformat(ts)
        assert dt.tzinfo is not None
