#!/usr/bin/env python3
"""tile_v13_pass3_local.py — per-peel Phase A + Phase B for the 24/7 driver.

Per-peel variant of `tile_v13_pass3_merge.py`. Operates exclusively on
`tiles/peel_<idx>/...` and `staging/peel_<idx>/combined/z6_*.parquet`,
so it can run in parallel with another peel's pass2 (Stage A prefetch)
without filesystem collision.

Phase A — adaptive z=z_max+1 subdivide. For every leaf flagged `oversize`
in the peel manifest, re-COPYs source rows from the peel's z=6 combined
bucket into 4 z=z_max+1 children. Mirrors the global pass3's Phase A
exactly, just scoped to per-peel paths.

Phase B — bottom-up merge from the deepest z observed in the peel down
to z=Z_BUCKET (z=6). **Stops at z=6**: at lower z, a parent has children
in multiple z=6 buckets which may belong to different peels — those
cross-peel merges are deferred to the cycle-end global finalizer.

Run after the 24/7 driver's pass2 step has written
`tiles/peel_<idx>/_manifest.json`.

Run
---

    python tile_v13_pass3_local.py --peel-idx 18 [--workdir ...]
"""
from __future__ import annotations

import argparse
import json
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
    combined_bucket_path_peel,
    duckdb_tmp_dir_peel,
    new_con,
    peel_dir_name,
    peel_manifest_path,
    quadkey_extent,
    resolve_workdir,
    tile_path_peel,
)
from tile_v13_sentry import init_sentry, log_event, phase_span

PROGRESS_INTERVAL_SEC = 30


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _z_to_z6_parent(z: int, x: int, y: int) -> tuple[int, int]:
    """Walk up from (z, x, y) to its z=6 ancestor (peel-agnostic math)."""
    if z < Z_BUCKET:
        raise ValueError(f"z ({z}) must be >= Z_BUCKET ({Z_BUCKET})")
    delta = z - Z_BUCKET
    return (x >> delta, y >> delta)


def _load_manifest(workdir: Path, peel_idx: int) -> dict[str, dict]:
    p = peel_manifest_path(workdir, peel_idx)
    if not p.exists():
        raise FileNotFoundError(f"per-peel manifest missing: {p}")
    data = json.loads(p.read_text(encoding="utf-8"))
    return {f"{e['z']}:{e['x']}:{e['y']}": e for e in data}


def _save_manifest(workdir: Path, peel_idx: int, manifest: dict[str, dict]) -> Path:
    p = peel_manifest_path(workdir, peel_idx)
    p.parent.mkdir(parents=True, exist_ok=True)
    rows = sorted(
        (e for e in manifest.values() if e.get("status") == "done"),
        key=lambda e: (e["z"], e["x"], e["y"]),
    )
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    tmp.replace(p)
    return p


# ---------------------------------------------------------------------------
# Phase A — adaptive z=z_max+1 subdivide (per-peel)
# ---------------------------------------------------------------------------

def _subdivide_one_oversize(
    workdir: Path,
    peel_idx: int,
    z: int,
    x: int,
    y: int,
    tile_budget: int,
    memory_limit: str,
    internal_threads: int,
    temp_dir: Path,
) -> tuple[int, int, list[dict], str | None]:
    """Subdivide a single oversize leaf into 4 z+1 children.

    Returns (n_children_written, n_children_still_oversize, child_records, error).
    """
    z6_x, z6_y = _z_to_z6_parent(z, x, y)
    bucket_path = combined_bucket_path_peel(workdir, peel_idx, z6_x, z6_y)
    if not bucket_path.exists():
        return 0, 0, [], (
            f"missing peel-scoped combined bucket z6_{z6_x}_{z6_y}.parquet "
            f"for parent {z}/{x}/{y} (peel={peel_idx})"
        )

    children = [
        (z + 1, x * 2,     y * 2),
        (z + 1, x * 2 + 1, y * 2),
        (z + 1, x * 2,     y * 2 + 1),
        (z + 1, x * 2 + 1, y * 2 + 1),
    ]

    con = new_con(
        internal_threads=internal_threads,
        memory_limit=memory_limit,
        temp_dir=temp_dir,
    )
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
            out_path = tile_path_peel(workdir, peel_idx, cz, cx, cy)
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
                    try: tmp.unlink()
                    except Exception: pass
                return n_written, n_still_oversize, records, str(e)
            if not tmp.exists():
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
                for theme_name, n in con.execute(
                    f"SELECT theme, COUNT(*) FROM read_parquet('{q_path(tmp)}') GROUP BY theme"
                ).fetchall():
                    theme_counts[str(theme_name)] = int(n)
            except Exception:
                theme_counts = {"unknown": feature_count}

            size_bytes = tmp.stat().st_size
            tmp.replace(out_path)
            still_oversize = size_bytes > tile_budget
            if still_oversize:
                n_still_oversize += 1
                log_event(
                    "tiler.oversize_tile",
                    level="warning",
                    component="pass3_local",
                    **{"peel.idx": peel_idx},
                    z=cz, x=cx, y=cy, size_bytes=size_bytes,
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

    # Drop the parent oversize tile — replaced by the children.
    parent_path = tile_path_peel(workdir, peel_idx, z, x, y)
    if parent_path.exists():
        try: parent_path.unlink()
        except Exception: pass

    return n_written, n_still_oversize, records, None


def _phase_a(
    workdir: Path,
    peel_idx: int,
    manifest: dict[str, dict],
    z_max: int,
    tile_budget: int,
    memory_limit: str,
    internal_threads: int,
    workers: int,
    temp_dir: Path,
) -> dict:
    """Subdivide every oversize leaf at z_max. Mutates manifest in place."""
    candidates = [
        e for e in manifest.values()
        if e.get("status") == "done" and e.get("oversize") and e["z"] == z_max
    ]
    counters = {"oversize_candidates": len(candidates), "children_written": 0,
                "children_still_oversize": 0, "failed": 0}
    if not candidates:
        return counters

    manifest_lock = threading.Lock()

    def task(entry: dict):
        return _subdivide_one_oversize(
            workdir, peel_idx,
            int(entry["z"]), int(entry["x"]), int(entry["y"]),
            tile_budget, memory_limit, internal_threads, temp_dir,
        ) + (entry,)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for fut in as_completed([pool.submit(task, e) for e in candidates]):
            try:
                n_w, n_os, recs, err, entry = fut.result()
            except Exception as exc:
                counters["failed"] += 1
                log_event("tiler.pass3_local_phase_a_exception", level="error",
                          component="pass3_local",
                          **{"peel.idx": peel_idx}, error=repr(exc))
                continue
            if err:
                counters["failed"] += 1
                continue
            counters["children_written"] += n_w
            counters["children_still_oversize"] += n_os
            with manifest_lock:
                # Parent replaced by children: drop parent entry, add new ones.
                manifest.pop(f"{entry['z']}:{entry['x']}:{entry['y']}", None)
                for rec in recs:
                    manifest[f"{rec['z']}:{rec['x']}:{rec['y']}"] = rec

    return counters


# ---------------------------------------------------------------------------
# Phase B — bottom-up merge (per-peel; stops at z=Z_BUCKET=6)
# ---------------------------------------------------------------------------

def _merge_one_parent(
    workdir: Path,
    peel_idx: int,
    pz: int,
    px: int,
    py: int,
    children: list[dict],
    tile_budget: int,
    memory_limit: str,
    internal_threads: int,
    temp_dir: Path,
) -> tuple[bool, dict | None, str | None]:
    """Concat 4 child parquets into a parent. Mirrors pass3_merge._merge_one_parent."""
    if len(children) != 4:
        return False, None, None
    if sum(int(c.get("size_bytes", 0)) for c in children) > tile_budget:
        return False, None, None

    child_paths = [
        tile_path_peel(workdir, peel_idx, int(c["z"]), int(c["x"]), int(c["y"]))
        for c in children
    ]
    if not all(p.exists() for p in child_paths):
        return False, None, None

    parent_path = tile_path_peel(workdir, peel_idx, pz, px, py)
    parent_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = parent_path.with_suffix(".parquet.tmp")
    files_lit = q_path_list(child_paths)

    con = new_con(
        internal_threads=internal_threads,
        memory_limit=memory_limit,
        temp_dir=temp_dir,
    )
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
                try: tmp.unlink()
                except Exception: pass
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
            for theme_name, n in con.execute(
                f"SELECT theme, COUNT(*) FROM read_parquet('{q_path(tmp)}') GROUP BY theme"
            ).fetchall():
                theme_counts[str(theme_name)] = int(n)
        except Exception:
            theme_counts = {"unknown": feature_count}
    finally:
        try: con.close()
        except Exception: pass

    tmp.replace(parent_path)
    for cp in child_paths:
        try: cp.unlink()
        except Exception: pass

    ext = quadkey_extent(pz, px, py)
    return True, {
        "z": pz, "x": px, "y": py,
        "status": "done",
        "size_bytes": merged_size,
        "feature_count": feature_count,
        "theme_counts": theme_counts,
        "oversize": False,
        "bbox": ext.as_list(),
        "at": _iso_now(),
    }, None


def _phase_b(
    workdir: Path,
    peel_idx: int,
    manifest: dict[str, dict],
    z_max: int,
    tile_budget: int,
    memory_limit: str,
    internal_threads: int,
    workers: int,
    temp_dir: Path,
) -> dict:
    """Bottom-up merge from deepest z down to z=Z_BUCKET (z=6).

    Stops at z=6 — merges at z=6→z=5 cross peel boundaries and are
    deferred to `tile_v13_pass3_global_finalize.py`.
    """
    counters = {"total_merged": 0, "failed": 0}
    deepest_z = max(
        (int(e["z"]) for e in manifest.values() if e.get("status") == "done"),
        default=z_max,
    )

    manifest_lock = threading.Lock()

    for z in range(deepest_z, Z_BUCKET, -1):  # z > Z_BUCKET, so z=7 is the lowest source
        with manifest_lock:
            current = [
                e for e in manifest.values()
                if e.get("status") == "done" and int(e["z"]) == z
            ]
        if not current:
            continue
        groups: dict[tuple[int, int], list[dict]] = {}
        for e in current:
            groups.setdefault((int(e["x"]) >> 1, int(e["y"]) >> 1), []).append(e)
        candidates = [(px, py, kids) for (px, py), kids in groups.items() if len(kids) == 4]
        if not candidates:
            continue

        def task(args_):
            px, py, kids = args_
            ok, prec, err = _merge_one_parent(
                workdir, peel_idx, z - 1, px, py, kids,
                tile_budget, memory_limit, internal_threads, temp_dir,
            )
            return px, py, kids, ok, prec, err

        with ThreadPoolExecutor(max_workers=workers) as pool:
            for fut in as_completed([pool.submit(task, c) for c in candidates]):
                try:
                    px, py, kids, ok, prec, err = fut.result()
                except Exception as exc:
                    counters["failed"] += 1
                    log_event("tiler.pass3_local_phase_b_exception", level="error",
                              component="pass3_local",
                              **{"peel.idx": peel_idx}, error=repr(exc))
                    continue
                if err:
                    counters["failed"] += 1
                    continue
                if ok and prec is not None:
                    with manifest_lock:
                        for k in kids:
                            manifest.pop(f"{k['z']}:{k['x']}:{k['y']}", None)
                        manifest[f"{prec['z']}:{prec['x']}:{prec['y']}"] = prec
                    counters["total_merged"] += 1

    return counters


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------

def run_one_peel(
    workdir: Path,
    peel_idx: int,
    z_max: int = Z_MAX_DEFAULT,
    tile_budget: int = TILE_BUDGET_DEFAULT,
    workers: int = 4,
    memory_limit: str = DEFAULT_MEMORY_LIMIT,
    internal_threads: int = DEFAULT_INTERNAL_THREADS,
    cycle: int = 0,
) -> dict:
    """Top-level callable for the 24/7 driver's Stage B (pass3 portion).

    Mutates `tiles/peel_<idx>/_manifest.json` in place. Returns counters
    suitable for stuffing into the phase_span context.
    """
    t0 = time.time()
    manifest = _load_manifest(workdir, peel_idx)

    temp_dir = duckdb_tmp_dir_peel(workdir, peel_idx)
    temp_dir.mkdir(parents=True, exist_ok=True)

    with phase_span("pass3_local_phase_a", peel_idx=peel_idx, cycle=cycle) as a:
        a.update(_phase_a(
            workdir, peel_idx, manifest,
            z_max, tile_budget, memory_limit, internal_threads, workers, temp_dir,
        ))

    with phase_span("pass3_local_phase_b", peel_idx=peel_idx, cycle=cycle) as b:
        b.update(_phase_b(
            workdir, peel_idx, manifest,
            z_max, tile_budget, memory_limit, internal_threads, workers, temp_dir,
        ))
        b["floor_z"] = Z_BUCKET  # documents the per-peel merge stop

    _save_manifest(workdir, peel_idx, manifest)

    final_done = sum(1 for e in manifest.values() if e.get("status") == "done")
    return {
        "peel.idx": peel_idx,
        "tiles_subdivided": a.get("children_written", 0),
        "tiles_merged_up": b.get("total_merged", 0),
        "final_tile_count": final_done,
        "duration_sec": round(time.time() - t0, 2),
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--workdir", default=None)
    p.add_argument("--peel-idx", type=int, required=True)
    p.add_argument("--cycle", type=int, default=0)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--memory-limit", default=DEFAULT_MEMORY_LIMIT)
    p.add_argument("--internal-threads", type=int, default=DEFAULT_INTERNAL_THREADS)
    p.add_argument("--z-max", type=int, default=Z_MAX_DEFAULT)
    p.add_argument("--tile-budget-bytes", type=int, default=TILE_BUDGET_DEFAULT)
    args = p.parse_args()

    workdir = resolve_workdir(args.workdir)
    init_sentry("pass3_local")
    result = run_one_peel(
        workdir=workdir,
        peel_idx=args.peel_idx,
        z_max=args.z_max,
        tile_budget=args.tile_budget_bytes,
        workers=args.workers,
        memory_limit=args.memory_limit,
        internal_threads=args.internal_threads,
        cycle=args.cycle,
    )
    log_event("tiler.pass3_local_done", component="pass3_local", cycle=args.cycle, **result)
    print(
        f"v13 Pass 3 local (peel {args.peel_idx}, {peel_dir_name(args.peel_idx)}) done in "
        f"{result['duration_sec']}s: "
        f"{result['tiles_subdivided']} subdivided, "
        f"{result['tiles_merged_up']} merged-up, "
        f"final {result['final_tile_count']} tiles",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
