"""
Overture tiling pipeline status reporter.

Reads _download.json and _tiles.json checkpoints for each theme and prints
a compact summary of progress across both phases.

Usage:
    python status.py [--workdir D:/overture] [--release 2026-04-15.0]

Output example:
    buildings:      download 512/512 files (100.0%, 269.1 GB), tile 2341/12891 cells (18.2%, 4.1 GB)
    segments:       download   0/128 files (  0.0%), tile not started
    land_use:       download  32/ 32 files (100.0%, 19.7 GB), tile done (412 tiles, 891 MB)
    water:          not started
    land:           not started
    infrastructure: not started
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


ALL_THEMES = ["buildings", "segments", "land_use", "water", "land", "infrastructure"]

# Verified sizes per theme for release 2026-04-15.0 (probed 2026-05-03)
# Used as denominator in download progress when checkpoint doesn't have all files yet.
KNOWN_FILE_COUNTS: dict[str, int] = {
    "buildings":      512,
    "segments":       128,
    "land_use":        32,
    "water":           32,
    "land":            32,
    "infrastructure":  16,
}


def load_json(path: Path) -> list | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def format_bytes(n: int) -> str:
    if n >= 1e12:
        return f"{n/1e12:.1f} TB"
    if n >= 1e9:
        return f"{n/1e9:.1f} GB"
    if n >= 1e6:
        return f"{n/1e6:.0f} MB"
    return f"{n/1e3:.0f} KB"


def theme_status(theme: str, workdir: Path) -> str:
    raw_dir = workdir / "raw" / theme
    tiles_dir = workdir / "tiles" / theme

    dl_entries = load_json(raw_dir / "_download.json")
    tile_entries = load_json(tiles_dir / "_tiles.json")

    parts: list[str] = []

    # -- Download phase --
    if dl_entries is None:
        parts.append("download: not started")
    else:
        done_files = [e for e in dl_entries if e.get("status") == "done"]
        fail_files = [e for e in dl_entries if e.get("status") == "failed"]
        inprog_files = [e for e in dl_entries if e.get("status") == "in_progress"]
        total_files = max(len(dl_entries), KNOWN_FILE_COUNTS.get(theme, len(dl_entries)))
        done_bytes = sum(e.get("size_bytes", 0) for e in done_files)
        total_bytes = sum(e.get("size_bytes", 0) for e in dl_entries)
        pct = len(done_files) / total_files * 100 if total_files > 0 else 0
        dl_str = (
            f"download {len(done_files):>{len(str(total_files))}}/{total_files} files "
            f"({pct:4.1f}%"
        )
        if done_bytes > 0:
            dl_str += f", {format_bytes(done_bytes)}"
        if fail_files:
            dl_str += f", {len(fail_files)} failed"
        if inprog_files:
            dl_str += f", {len(inprog_files)} in-progress"
        dl_str += ")"
        parts.append(dl_str)

    # -- Tile phase --
    if tile_entries is None:
        parts.append("tile: not started")
    else:
        done_tiles = [e for e in tile_entries if e.get("status") == "done"]
        empty_tiles = [e for e in tile_entries if e.get("status") == "empty"]
        fail_tiles = [e for e in tile_entries if e.get("status") == "failed"]
        visited = len(tile_entries)
        tile_bytes = sum(e.get("size_bytes", 0) for e in done_tiles)
        tile_str = (
            f"tile {len(done_tiles)} tiles "
            f"({len(empty_tiles)} empty, {len(fail_tiles)} failed"
        )
        if tile_bytes > 0:
            tile_str += f", {format_bytes(tile_bytes)}"
        tile_str += f", {visited} cells visited)"
        parts.append(tile_str)

    return "  ".join(parts)


def resolve_workdir(cli_value: str | None) -> Path:
    if cli_value:
        return Path(cli_value)
    env_val = os.environ.get("OVERTURE_WORKDIR")
    if env_val:
        return Path(env_val)
    if sys.platform == "win32":
        return Path("D:/overture")
    return Path("/Volumes/SSD/overture")


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(
        description="Show download + tiling progress across all Overture themes."
    )
    parser.add_argument("--workdir", default=None, help="Root workdir. Overrides OVERTURE_WORKDIR.")
    parser.add_argument("--release", default=None, help="Overture release (informational).")
    args = parser.parse_args()

    workdir = resolve_workdir(args.workdir)
    release = args.release or os.environ.get("OVERTURE_RELEASE", "2026-04-15.0")

    print(f"Overture pipeline status  release={release}  workdir={workdir}")
    print()

    max_len = max(len(t) for t in ALL_THEMES)
    for theme in ALL_THEMES:
        status = theme_status(theme, workdir)
        print(f"  {theme:<{max_len}}  {status}")

    print()


if __name__ == "__main__":
    main()
