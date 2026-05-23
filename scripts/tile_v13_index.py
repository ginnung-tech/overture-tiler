"""tile_v13_index.py — global tile index writer for the 24/7 driver.

After each peel uploads, this module rewrites
`<workdir>/driver-state/tiles_index.json` so it reflects:

  - the new tile set produced for that peel (from the per-peel manifest)
  - the prior cycle's tiles in OTHER peels (untouched)
  - the prior cycle's tiles in THIS peel (replaced)

The driver then uploads the rewritten index to R2 with `Cache-Control:
no-store, no-cache, must-revalidate, max-age=0` so SPA clients always see
the latest peel's tiles + the freshness timestamps for older peels.

Schema (the SPA consumer contract — see infrastructure plan §"Index file shape"):

    {
      "schema_version": 1,
      "generated_at": "2026-05-23T14:32:00Z",
      "cycle": 7,
      "tile_count": 142318,
      "peels": [
        {"peel_idx": 18, "lng_lo": 0.0,   "lng_hi": 10.0,  "vintage": "..."},
        ...
      ],
      "tiles": [
        {"z": 14, "x": 8567, "y": 5145, "size_bytes": 1843200,
         "feature_count": 12345, "theme_counts": {"buildings": 9876, ...},
         "bbox": [12.65, 55.66, 12.67, 55.68], "vintage": "..."},
        ...
      ]
    }
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from tile_v13_helpers import (
    PEEL_WIDTH_DEG_DEFAULT,
    driver_state_dir,
    eastward_peel_order_from_zero,
    global_index_path,
    n_peels,
    peel_lng_range,
)

SCHEMA_VERSION = 1


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _empty_index(cycle: int, peel_width_deg: int) -> dict[str, Any]:
    """Initial index shape — all peels listed with vintage=null, no tiles yet."""
    total = n_peels(peel_width_deg)
    peels = []
    for idx in range(total):
        lng_lo, lng_hi = peel_lng_range(idx, peel_width_deg)
        peels.append(
            {
                "peel_idx": idx,
                "lng_lo": lng_lo,
                "lng_hi": lng_hi,
                "vintage": None,
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _iso_now(),
        "cycle": cycle,
        "tile_count": 0,
        "peels": peels,
        "tiles": [],
    }


def load_index(workdir: Path) -> dict[str, Any] | None:
    """Return the on-disk global index, or None if it doesn't exist yet."""
    p = global_index_path(workdir)
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def _tile_lng_center(bbox: list[float]) -> float:
    """Centre lng of a [west, south, east, north] WGS84 bbox.

    Handles antimeridian-spanning tiles by checking sign flip. The tiler
    never emits antimeridian-spanners (Mercator tile bboxes don't wrap),
    but the guard makes downstream filters robust.
    """
    west, _south, east, _north = bbox
    if east < west:  # spans antimeridian
        return ((west + east + 360.0) / 2.0 + 180.0) % 360.0 - 180.0
    return (west + east) / 2.0


def _in_peel_lng_range(lng: float, lng_lo: float, lng_hi: float) -> bool:
    """True if `lng` falls in the half-open lng range [lng_lo, lng_hi).

    Both lng_lo and lng_hi are in [-180, 180]. A peel never wraps the
    antimeridian (peel widths are submultiples of 360°), so no special case.
    """
    return lng_lo <= lng < lng_hi


def update_global_index(
    workdir: Path,
    peel_idx: int,
    cycle: int,
    new_peel_entries: list[dict[str, Any]],
    peel_width_deg: int = PEEL_WIDTH_DEG_DEFAULT,
) -> Path:
    """Merge a peel's new tile entries into the global index, atomically.

    Steps:
      1. Load existing index (or initialise empty for this cycle).
      2. Drop tiles from the global `tiles` list whose centre lng falls in
         this peel's lng range — those are the prior-cycle tiles for this
         peel and they're being replaced.
      3. Stamp each entry in `new_peel_entries` with `vintage=now()` and
         add them to the global `tiles` list.
      4. Update the `peels[peel_idx].vintage` to now().
      5. Recompute `tile_count`, refresh `generated_at` + `cycle`.
      6. Sort `tiles` by (z, x, y) for deterministic diffs.
      7. Atomic write via tmp → rename.

    Returns the path of the written index file.
    """
    state_dir = driver_state_dir(workdir)
    state_dir.mkdir(parents=True, exist_ok=True)
    out_path = global_index_path(workdir)

    index = load_index(workdir) or _empty_index(cycle, peel_width_deg)

    # Drop prior-cycle tiles in this peel's lng range.
    lng_lo, lng_hi = peel_lng_range(peel_idx, peel_width_deg)
    kept_tiles = [
        t for t in index["tiles"]
        if not _in_peel_lng_range(_tile_lng_center(t["bbox"]), lng_lo, lng_hi)
    ]

    # Stamp + add new entries.
    vintage = _iso_now()
    for e in new_peel_entries:
        # Carry only the fields the SPA consumer needs. Pass3-local writes
        # the per-peel manifest with these fields plus a few it controls;
        # we copy the public ones explicitly so manifest schema drift never
        # leaks into the index.
        kept_tiles.append(
            {
                "z": e["z"],
                "x": e["x"],
                "y": e["y"],
                "size_bytes": e["size_bytes"],
                "feature_count": e.get("feature_count", 0),
                "theme_counts": e.get("theme_counts", {}),
                "bbox": e["bbox"],
                "vintage": vintage,
            }
        )

    # Update peel vintage.
    for p in index["peels"]:
        if p["peel_idx"] == peel_idx:
            p["vintage"] = vintage
            break
    else:
        # Peel entry missing (schema migration) — append a fresh one.
        index["peels"].append(
            {
                "peel_idx": peel_idx,
                "lng_lo": lng_lo,
                "lng_hi": lng_hi,
                "vintage": vintage,
            }
        )
        index["peels"].sort(key=lambda p: p["peel_idx"])

    # Sort tiles deterministically for stable diffs across writes.
    kept_tiles.sort(key=lambda t: (t["z"], t["x"], t["y"]))

    index["tiles"] = kept_tiles
    index["tile_count"] = len(kept_tiles)
    index["generated_at"] = vintage
    index["cycle"] = cycle
    index["schema_version"] = SCHEMA_VERSION

    # Atomic write: rename is atomic on POSIX; on Windows os.replace covers it.
    tmp = out_path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(index, separators=(",", ":")), encoding="utf-8")
    os.replace(tmp, out_path)
    return out_path


def viewport_tiles(
    index: dict[str, Any],
    west: float,
    south: float,
    east: float,
    north: float,
    z_min: int | None = None,
    z_max: int | None = None,
) -> list[dict[str, Any]]:
    """Reference implementation of the SPA's tile-selection algorithm.

    Used by tests and for a CLI debug command. Not invoked by the driver.
    Filters the index's `tiles` list to those whose bbox intersects the
    viewport (and, optionally, falls in the [z_min, z_max] zoom range).
    """
    result = []
    for t in index["tiles"]:
        w, s, e, n = t["bbox"]
        if e <= west or w >= east or n <= south or s >= north:
            continue
        if z_min is not None and t["z"] < z_min:
            continue
        if z_max is not None and t["z"] > z_max:
            continue
        result.append(t)
    return result


def remove_peel(
    workdir: Path,
    peel_idx: int,
    cycle: int,
    peel_width_deg: int = PEEL_WIDTH_DEG_DEFAULT,
) -> Path:
    """Drop a peel's tiles from the global index without adding replacements.

    Used by tests and the cycle-end finalize step when a peel produces an
    empty manifest (e.g. an open-ocean peel that fully collapsed to z<5
    boundary tiles handled by the finalizer).
    """
    return update_global_index(workdir, peel_idx, cycle, [], peel_width_deg)


__all__ = [
    "SCHEMA_VERSION",
    "load_index",
    "update_global_index",
    "viewport_tiles",
    "remove_peel",
]
