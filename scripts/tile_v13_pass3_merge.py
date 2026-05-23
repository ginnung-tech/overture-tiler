#!/usr/bin/env python3
"""tile_v13_pass3_merge.py — adaptive z=z_max+1 subdivide + bottom-up merge.

Stage 2b / pass 3 of the v13 pipeline (see `v13_SPEC.md`).

Two phases:

  Phase A (adaptive subdivide). For every z=z_max leaf flagged
  `oversize` by pass2, re-COPY its source rows from the z=6 combined
  bucket into 4 z=z_max+1 children. If any child is still over the
  budget, log a WARN and accept (no further subdivision in v13 v1).

  Phase B (bottom-up merge). For z in (z_max..1) descending: group
  leaves by parent (z-1, x>>1, y>>1). If all 4 children exist AND
  sum(sizes) <= TILE_BUDGET, concat their parquets into a parent and
  delete children. Recurse upward. Asymmetric sibling sets stay at the
  lower z (the spec's example: ocean/coast meeting points keep their
  fine-grain coast tiles).

Both phases update the manifest in place.

Run after pass2 has emitted `_v13_pass2_manifest.json`.

Run
---

    python tile_v13_pass3_merge.py [--workdir /Volumes/SSD/overture] [--workers N]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _sql_safety import q_float, q_path, q_path_list  # noqa: E402
from tile_v13_helpers import (  # noqa: E402
    DEFAULT_INTERNAL_THREADS,
    DEFAULT_MEMORY_LIMIT,
    TILE_BUDGET_DEFAULT,
    Z_BUCKET,
    Z_MAX_DEFAULT,
    combined_bucket_path,
    new_con,
    quadkey_extent,
    resolve_workdir,
    tile_path,
)

PASS2_CHECKPOINT_FILENAME = "_v13_pass2_checkpoint.json"
PASS2_MANIFEST_FILENAME = "_v13_pass2_manifest.json"
PASS3_MANIFEST_FILENAME = "_v13_pass3_manifest.json"

PROGRESS_INTERVAL_SEC = 30


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# Manifest helpers
# ---------------------------------------------------------------------------

def _load_pass2_checkpoint(workdir: Path) -> dict[str, dict]:
    p = workdir / "tiles" / PASS2_CHECKPOINT_FILENAME
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return {f"{e['z']}:{e['x']}:{e['y']}": e for e in data}
    except Exception as e:
        print(f"[warn] checkpoint read error: {e}", flush=True)
        return {}


def _save_manifest(workdir: Path, manifest: dict[str, dict]) -> None:
    p = workdir / "tiles" / PASS3_MANIFEST_FILENAME
    p.parent.mkdir(parents=True, exist_ok=True)
    rows = sorted(
        (e for e in manifest.values() if e.get("status") == "done"),
        key=lambda e: (e["z"], e["x"], e["y"]),
    )
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    tmp.replace(p)


# ---------------------------------------------------------------------------
# Phase A — adaptive z=z_max+1 subdivide
# ---------------------------------------------------------------------------

def _z_to_z6_parent(z: int, x: int, y: int) -> tuple[int, int]:
    """Walk up from (z, x, y) to its z=6 ancestor."""
    if z < Z_BUCKET:
        raise ValueError(f"z ({z}) must be >= Z_BUCKET ({Z_BUCKET})")
    delta = z - Z_BUCKET
    return (x >> delta, y >> delta)


def _subdivide_one_oversize(
    workdir: Path,
    z: int,
    x: int,
    y: int,
    bbox_list: list[float],
    tile_budget: int,
    memory_limit: str,
    internal_threads: int,
) -> tuple[int, int, list[dict], str | None]:
    """Subdivide a single oversize leaf into 4 z+1 children.

    Re-COPYs source rows from the z=6 combined bucket (cheaper to re-read
    than to round-trip the parent leaf parquet through DuckDB twice).

    Returns (n_children_written, n_children_still_oversize, child_records, error).
    """
    z6_x, z6_y = _z_to_z6_parent(z, x, y)
    bucket_path = combined_bucket_path(workdir, z6_x, z6_y)
    if not bucket_path.exists():
        return 0, 0, [], f"missing combined bucket z6_{z6_x}_{z6_y}.parquet for parent {z}/{x}/{y}"

    children = [
        (z + 1, x * 2,     y * 2),
        (z + 1, x * 2 + 1, y * 2),
        (z + 1, x * 2,     y * 2 + 1),
        (z + 1, x * 2 + 1, y * 2 + 1),
    ]

    con = new_con(internal_threads=internal_threads, memory_limit=memory_limit)
    n_written = 0
    n_still_oversize = 0
    records: list[dict] = []
    try:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _bucket AS
            SELECT * FROM read_parquet('{q_path(bucket_path)}')
        """)
        for (cz, cx, cy) in children:
            ext = quadkey_extent(cz, cx, cy)
            out_path = tile_path(workdir, cz, cx, cy)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = out_path.with_suffix(".parquet.tmp")
            try:
                con.execute(f"""
                    COPY (
                        SELECT * FROM _bucket
                        WHERE bbox.xmin < {q_float(ext.max_lng)}
                          AND bbox.xmax > {q_float(ext.min_lng)}
                          AND bbox.ymin < {q_float(ext.max_lat)}
                          AND bbox.ymax > {q_float(ext.min_lat)}
                    ) TO '{q_path(tmp)}' (
                        FORMAT 'parquet',
                        COMPRESSION 'zstd',
                        COMPRESSION_LEVEL 3
                    )
                """)
            except Exception as e:
                if tmp.exists():
                    try:
                        tmp.unlink()
                    except Exception:
                        pass
                return n_written, n_still_oversize, records, str(e)
            if not tmp.exists():
                # empty child
                continue
            cnt_row = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{q_path(tmp)}')"
            ).fetchone()
            feature_count = int(cnt_row[0]) if cnt_row else 0
            if feature_count == 0:
                tmp.unlink()
                continue

            theme_counts: dict[str, int] = {}
            try:
                rows = con.execute(
                    f"""
                        SELECT theme, COUNT(*)
                        FROM read_parquet('{q_path(tmp)}')
                        GROUP BY theme
                    """
                ).fetchall()
                for theme_name, n in rows:
                    theme_counts[str(theme_name)] = int(n)
            except Exception:
                theme_counts = {"unknown": feature_count}

            size_bytes = tmp.stat().st_size
            tmp.replace(out_path)
            still_oversize = size_bytes > tile_budget
            if still_oversize:
                n_still_oversize += 1
                print(
                    f"  WARN child still oversize: z={cz} x={cx} y={cy} "
                    f"size={size_bytes/1e6:.1f} MB (>{tile_budget/1e6:.0f} MB) — accepting",
                    flush=True,
                )
            n_written += 1
            records.append({
                "z": cz, "x": cx, "y": cy,
                "status": "done",
                "size_bytes": size_bytes,
                "feature_count": feature_count,
                "theme_counts": theme_counts,
                "oversize": still_oversize,
                "bbox": ext.as_list(),
                "at": _iso_now(),
            })
    finally:
        try:
            con.execute("DROP TABLE IF EXISTS _bucket")
            con.close()
        except Exception:
            pass

    # Drop the parent leaf — it's been replaced by its (smaller) children.
    parent_path = tile_path(workdir, z, x, y)
    if parent_path.exists():
        try:
            parent_path.unlink()
        except Exception:
            pass

    return n_written, n_still_oversize, records, None


def _phase_a_subdivide(
    workdir: Path,
    manifest: dict[str, dict],
    z_max: int,
    tile_budget: int,
    memory_limit: str,
    internal_threads: int,
    workers: int,
) -> None:
    """Run Phase A: subdivide every oversize leaf at z_max."""
    candidates = [
        e for e in manifest.values()
        if e.get("status") == "done"
        and e.get("oversize")
        and e["z"] == z_max
    ]
    print(
        f"\nPhase A: {len(candidates)} oversize leaves at z={z_max} "
        f"(budget {tile_budget/1e6:.0f} MB)",
        flush=True,
    )
    if not candidates:
        return

    manifest_lock = threading.Lock()

    def task(entry: dict) -> tuple[int, int, list[dict], str | None, dict]:
        n_w, n_os, recs, err = _subdivide_one_oversize(
            workdir,
            int(entry["z"]),
            int(entry["x"]),
            int(entry["y"]),
            entry.get("bbox", [0.0, 0.0, 0.0, 0.0]),
            tile_budget,
            memory_limit,
            internal_threads,
        )
        return n_w, n_os, recs, err, entry

    t0 = time.time()
    last_progress = t0
    n_done = 0
    n_children_written = 0
    n_children_oversize = 0
    n_failed = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(task, e) for e in candidates]
        for fut in as_completed(futures):
            try:
                n_w, n_os, recs, err, entry = fut.result()
            except Exception as exc:
                print(f"  subdivide task exception: {exc}", flush=True)
                n_failed += 1
                continue

            if err is not None:
                n_failed += 1
                print(
                    f"  FAIL subdivide z={entry['z']} x={entry['x']} y={entry['y']}: {err}",
                    flush=True,
                )
                continue

            with manifest_lock:
                # Drop the parent's manifest entry; it was replaced by children.
                manifest.pop(f"{entry['z']}:{entry['x']}:{entry['y']}", None)
                for r in recs:
                    manifest[f"{r['z']}:{r['x']}:{r['y']}"] = r
            n_children_written += n_w
            n_children_oversize += n_os
            n_done += 1

            now = time.time()
            if now - last_progress >= PROGRESS_INTERVAL_SEC or n_done == len(candidates):
                print(
                    f"--- Phase A progress: {n_done}/{len(candidates)}  "
                    f"children_written={n_children_written}  "
                    f"still_oversize={n_children_oversize}  failed={n_failed}",
                    flush=True,
                )
                last_progress = now

    print(
        f"Phase A done in {(time.time()-t0)/60:.1f} min: "
        f"{n_children_written} children written ({n_children_oversize} still oversize), "
        f"{n_failed} failed",
        flush=True,
    )


# ---------------------------------------------------------------------------
# Phase B — bottom-up merge
# ---------------------------------------------------------------------------

def _merge_one_parent(
    workdir: Path,
    pz: int,
    px: int,
    py: int,
    children_records: list[dict],
    tile_budget: int,
    memory_limit: str,
    internal_threads: int,
) -> tuple[bool, dict | None, str | None]:
    """Concat 4 child parquets into a parent tile.

    Returns (merged, parent_record, error). `merged=False` means the
    children stay (e.g. union exceeded budget); not an error.
    """
    if len(children_records) != 4:
        return False, None, None
    sizes = [int(c.get("size_bytes", 0)) for c in children_records]
    if sum(sizes) > tile_budget:
        return False, None, None

    child_paths = [
        tile_path(workdir, int(c["z"]), int(c["x"]), int(c["y"]))
        for c in children_records
    ]
    if not all(p.exists() for p in child_paths):
        return False, None, None

    parent_path = tile_path(workdir, pz, px, py)
    parent_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = parent_path.with_suffix(".parquet.tmp")
    files_lit = q_path_list(child_paths)

    con = new_con(internal_threads=internal_threads, memory_limit=memory_limit)
    try:
        try:
            con.execute(f"""
                COPY (
                    SELECT * FROM read_parquet({files_lit}, union_by_name=True)
                ) TO '{q_path(tmp)}' (
                    FORMAT 'parquet',
                    COMPRESSION 'zstd',
                    COMPRESSION_LEVEL 3
                )
            """)
        except Exception as e:
            if tmp.exists():
                try:
                    tmp.unlink()
                except Exception:
                    pass
            return False, None, str(e)

        if not tmp.exists():
            return False, None, "merged parquet not produced"
        merged_size = tmp.stat().st_size
        if merged_size > tile_budget:
            tmp.unlink()
            return False, None, None  # accept-as-children, not an error

        cnt_row = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{q_path(tmp)}')"
        ).fetchone()
        feature_count = int(cnt_row[0]) if cnt_row else 0

        theme_counts: dict[str, int] = {}
        try:
            rows = con.execute(
                f"""
                    SELECT theme, COUNT(*)
                    FROM read_parquet('{q_path(tmp)}')
                    GROUP BY theme
                """
            ).fetchall()
            for theme_name, n in rows:
                theme_counts[str(theme_name)] = int(n)
        except Exception:
            theme_counts = {"unknown": feature_count}
    finally:
        try:
            con.close()
        except Exception:
            pass

    tmp.replace(parent_path)
    # Delete children only after the parent rename succeeded.
    for cp in child_paths:
        try:
            cp.unlink()
        except Exception:
            pass

    ext = quadkey_extent(pz, px, py)
    parent_record = {
        "z": pz, "x": px, "y": py,
        "status": "done",
        "size_bytes": merged_size,
        "feature_count": feature_count,
        "theme_counts": theme_counts,
        "oversize": False,
        "bbox": ext.as_list(),
        "at": _iso_now(),
    }
    return True, parent_record, None


def _phase_b_merge(
    workdir: Path,
    manifest: dict[str, dict],
    z_max: int,
    tile_budget: int,
    memory_limit: str,
    internal_threads: int,
    workers: int,
) -> None:
    """Run Phase B: bottom-up merge for z in (z_max..1)."""
    print(
        f"\nPhase B: bottom-up merge from z={z_max} down to z=1 "
        f"(budget {tile_budget/1e6:.0f} MB)",
        flush=True,
    )

    # Phase A may have introduced z_max+1 children — start the merge from
    # the deepest z observed, not just z_max.
    deepest_z = max(
        (int(e["z"]) for e in manifest.values() if e.get("status") == "done"),
        default=z_max,
    )
    print(f"  deepest z observed = {deepest_z}", flush=True)

    manifest_lock = threading.Lock()
    total_merged = 0

    for z in range(deepest_z, 0, -1):
        # Group all `done` entries at this z by parent.
        with manifest_lock:
            current = [
                e for e in manifest.values()
                if e.get("status") == "done" and int(e["z"]) == z
            ]
        if not current:
            continue
        groups: dict[tuple[int, int], list[dict]] = {}
        for e in current:
            parent_key = (int(e["x"]) >> 1, int(e["y"]) >> 1)
            groups.setdefault(parent_key, []).append(e)

        # Only consider parents with all 4 children present.
        candidate_parents = [
            (px, py, kids)
            for (px, py), kids in groups.items()
            if len(kids) == 4
        ]
        if not candidate_parents:
            print(f"  z={z}: no complete sibling sets ({len(groups)} parents) — done", flush=True)
            continue

        print(
            f"  z={z}: {len(candidate_parents)} candidate parents (of {len(groups)})",
            flush=True,
        )

        def task(args_):
            px, py, kids = args_
            ok, prec, err = _merge_one_parent(
                workdir, z - 1, px, py, kids,
                tile_budget, memory_limit, internal_threads,
            )
            return px, py, kids, ok, prec, err

        t0 = time.time()
        last_progress = t0
        n_done = 0
        n_merged = 0
        n_failed = 0

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(task, c) for c in candidate_parents]
            for fut in as_completed(futures):
                try:
                    px, py, kids, ok, prec, err = fut.result()
                except Exception as exc:
                    print(f"    merge task exception: {exc}", flush=True)
                    n_failed += 1
                    continue
                n_done += 1
                if err is not None:
                    n_failed += 1
                    print(f"    FAIL merge z={z-1} x={px} y={py}: {err}", flush=True)
                    continue
                if ok and prec is not None:
                    with manifest_lock:
                        for k in kids:
                            manifest.pop(f"{k['z']}:{k['x']}:{k['y']}", None)
                        manifest[f"{prec['z']}:{prec['x']}:{prec['y']}"] = prec
                    n_merged += 1

                now = time.time()
                if now - last_progress >= PROGRESS_INTERVAL_SEC or n_done == len(candidate_parents):
                    print(
                        f"    z={z} progress: {n_done}/{len(candidate_parents)}  "
                        f"merged={n_merged} failed={n_failed}",
                        flush=True,
                    )
                    last_progress = now

        print(
            f"  z={z} -> z={z-1}: {n_merged} merged ({n_failed} failed) "
            f"in {(time.time()-t0)/60:.1f} min",
            flush=True,
        )
        total_merged += n_merged

    print(f"Phase B done: {total_merged} parent tiles emitted total", flush=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--workdir", default=None)
    p.add_argument("--workers", type=int, default=4,
                   help="Parallel subdivide / merge workers (default 4).")
    p.add_argument("--memory-limit", default=DEFAULT_MEMORY_LIMIT,
                   help=f"DuckDB memory_limit per worker (default {DEFAULT_MEMORY_LIMIT}).")
    p.add_argument("--internal-threads", type=int, default=DEFAULT_INTERNAL_THREADS)
    p.add_argument("--z-max", type=int, default=Z_MAX_DEFAULT,
                   help=f"Leaf zoom level emitted by pass2 (default {Z_MAX_DEFAULT}).")
    p.add_argument("--tile-budget-bytes", type=int, default=TILE_BUDGET_DEFAULT,
                   help=f"Merge / subdivide budget (default {TILE_BUDGET_DEFAULT} = 20 MiB).")
    p.add_argument("--skip-phase-a", action="store_true",
                   help="Skip the adaptive z=z_max+1 subdivide phase.")
    p.add_argument("--skip-phase-b", action="store_true",
                   help="Skip the bottom-up merge phase.")
    args = p.parse_args()

    workdir = resolve_workdir(args.workdir)
    print(f"v13 Pass 3 (subdivide + merge)  workdir={workdir}", flush=True)
    print(f"  workers       = {args.workers}", flush=True)
    print(f"  memory_limit  = {args.memory_limit} per worker", flush=True)
    print(f"  z_max         = {args.z_max}", flush=True)
    print(f"  tile_budget   = {args.tile_budget_bytes/1e6:.1f} MB", flush=True)

    manifest = _load_pass2_checkpoint(workdir)
    if not manifest:
        sys.exit(
            f"[error] no pass2 checkpoint at "
            f"{workdir / 'tiles' / PASS2_CHECKPOINT_FILENAME}"
        )

    if not args.skip_phase_a:
        _phase_a_subdivide(
            workdir, manifest, args.z_max, args.tile_budget_bytes,
            args.memory_limit, args.internal_threads, args.workers,
        )
    else:
        print("Phase A skipped (--skip-phase-a)", flush=True)

    if not args.skip_phase_b:
        _phase_b_merge(
            workdir, manifest, args.z_max, args.tile_budget_bytes,
            args.memory_limit, args.internal_threads, args.workers,
        )
    else:
        print("Phase B skipped (--skip-phase-b)", flush=True)

    _save_manifest(workdir, manifest)
    n_done = sum(1 for e in manifest.values() if e.get("status") == "done")
    print(f"\nv13 Pass 3 done: {n_done} tiles in final manifest", flush=True)


if __name__ == "__main__":
    main()
