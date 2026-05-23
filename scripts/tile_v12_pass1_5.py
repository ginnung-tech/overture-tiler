"""tile_v12_pass1_5.py — Python-orchestrated coarse bucketing.

Replaces v11's `run_pass1_5_coarse_partition` (DuckDB COPY ... UNNEST ...
PARTITION_BY), which is bottlenecked by DuckDB's single-threaded UNNEST
operator. v11 took ~4 hours to bucket 12 GB of infrastructure intermediates
into 600 1° coarse buckets; the 21× larger buildings theme would take days.

v12 architecture
----------------

Pass 1 stays the same (per-file intermediate parquet with cell-bound integer
columns, sorted by _lat_lo, _lon_lo). Already done for any theme that ran v10
or v11; v12 picks up from there.

This script handles ONLY Pass 1.5. After it finishes, run the v11
`tile.py --theme <theme>` again — Pass 1 will skip (resumed via _DONE), Pass
1.5 will skip (we drop the same _DONE marker the v11 partition path drops),
and Pass 2 picks up the coarse buckets we wrote.

Pipeline:

  1. Discover all per-file intermediates from staging/<theme>/<filehash>/intermediate.parquet
  2. Spawn N worker PROCESSES (multiprocessing, NOT threading — Python GIL
     would kill us). Each worker handles a disjoint slice of intermediate files.
  3. Per worker:
       - Open assigned intermediate files via pyarrow as RecordBatchReader
       - For each batch (default ~100K rows):
           * numpy vectorize: compute coarse_lon_lo/hi, coarse_lat_lo/hi
             from _lon_lo/hi // 10, _lat_lo/hi // 10
           * For each row, expand to (coarse_lon, coarse_lat) tuples
             touching its bbox. Most rows have 1-3 coarse cells (small bbox).
             Long-feature rows have up to MAX_RECT_SPAN/10 squared = 400 max.
           * Group batch by (coarse_lon, coarse_lat); for each group,
             slice the original row indices and write that slice to the
             worker's per-coarse-bucket parquet writer.
       - Keep per-bucket pyarrow.parquet.ParquetWriter open for the worker's
         lifetime; close all at end.
       - Output layout:
           staging/<theme>/_coarse/coarse_lon=X/coarse_lat=Y/data_<workerid>.parquet
         Each worker writes its OWN data_<id>.parquet so writes never collide.
  4. After all workers exit, drop _DONE marker.

Pass 2 (existing v11 code) reads coarse_bucket_paths(coarse_dir, cl, ca) which
already does `iterdir -> all *.parquet`, so it picks up data_0.parquet ...
data_N.parquet automatically without changes.

Why this beats v11 Pass 1.5
---------------------------

- DuckDB UNNEST is single-thread; Python+numpy vectorized expansion is single-
  thread per worker BUT we run N workers concurrently. 8 workers ≈ 8× speedup
  over v11 even ignoring the UNNEST inefficiency itself.
- No 600-partition write-buffer accumulation in DuckDB -> no spill-to-disk
  ceremony. Each worker holds 600 small parquet writers; total memory is
  bounded by row buffer per bucket × N workers.
- numpy bbox math is ~50× faster than DuckDB UNNEST(range(...)) for small
  expansion factors (which is the common case).
- All cores work; D: drive does sequential reads + bucketed writes; C: page
  file gets nothing.

Run
---

    python tile_v12_pass1_5.py --theme infrastructure
    # then:
    python tile.py --theme infrastructure   # resumes; uses our _coarse dir

Constants and helpers are imported from the existing tile.py so the layout
stays in sync.
"""
from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# Reuse layout helpers from the v11 tile.py (same dir).
sys.path.insert(0, str(Path(__file__).resolve().parent))
from tile import (  # noqa: E402
    OVERTURE_RELEASE,
    START_CELL_DEG,
    _COARSE_DIR_NAME,
    _COARSE_DONE_MARKER,
    _FINE_PER_COARSE_SIDE,
    _INTERMEDIATE_FILENAME,
    _PASS1_FILE_MANIFEST,
    coarse_dir_for_theme,
    resolve_workdir,
    staging_dir_for_theme,
)

BATCH_SIZE = 100_000
COMPRESSION = "gzip"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _list_intermediates(workdir: Path, theme: str) -> list[Path]:
    """Same discovery as collect_intermediates_and_extent, slimmer."""
    staging = staging_dir_for_theme(workdir, theme)
    out: list[Path] = []
    if not staging.exists():
        return out
    for sub in sorted(staging.iterdir()):
        if not sub.is_dir() or sub.name == _COARSE_DIR_NAME:
            continue
        m = sub / _PASS1_FILE_MANIFEST
        if not m.exists():
            continue
        try:
            data = json.loads(m.read_text(encoding="utf-8"))
        except Exception:
            continue
        if data.get("rows", 0) == 0:
            continue
        p = data.get("intermediate") or (sub / _INTERMEDIATE_FILENAME).as_posix()
        path = Path(p)
        if path.exists():
            out.append(path)
    return out


def _bucket_dir(coarse_dir: Path, coarse_lon: int, coarse_lat: int) -> Path:
    return coarse_dir / f"coarse_lon={coarse_lon}" / f"coarse_lat={coarse_lat}"


class _PerWorkerBucketSink:
    """Holds open per-(coarse_lon, coarse_lat) ParquetWriter objects for one worker.

    Each worker writes to data_<worker_id>.parquet inside the bucket dir, so
    sibling workers never contend on the same file.
    """

    def __init__(self, coarse_dir: Path, worker_id: int, schema: pa.Schema):
        self._coarse_dir = coarse_dir
        self._worker_id = worker_id
        self._schema = schema
        self._writers: dict[tuple[int, int], pq.ParquetWriter] = {}
        self._row_counts: dict[tuple[int, int], int] = {}

    def write(self, coarse_lon: int, coarse_lat: int, table: pa.Table) -> None:
        key = (coarse_lon, coarse_lat)
        w = self._writers.get(key)
        if w is None:
            d = _bucket_dir(self._coarse_dir, coarse_lon, coarse_lat)
            d.mkdir(parents=True, exist_ok=True)
            target = d / f"data_{self._worker_id}.parquet"
            w = pq.ParquetWriter(target.as_posix(), self._schema, compression=COMPRESSION)
            self._writers[key] = w
            self._row_counts[key] = 0
        w.write_table(table)
        self._row_counts[key] += table.num_rows

    def close(self) -> tuple[int, int]:
        n_buckets = len(self._writers)
        n_rows = sum(self._row_counts.values())
        for w in self._writers.values():
            try:
                w.close()
            except Exception:
                pass
        return n_buckets, n_rows


def _process_intermediate(
    intermediate_path: Path,
    coarse_dir: Path,
    worker_id: int,
    region_bbox_cells: tuple[int, int, int, int] | None,
) -> tuple[int, int, int]:
    """Stream one intermediate through batches, bucket each row by coarse cells.

    Returns (n_input_rows, n_output_rows, n_buckets_touched).
    """
    pf = pq.ParquetFile(intermediate_path.as_posix())
    schema_no_aux = _schema_without_aux(pf.schema_arrow)
    sink = _PerWorkerBucketSink(coarse_dir, worker_id, schema_no_aux)

    total_input_rows = 0
    total_output_rows = 0

    for batch in pf.iter_batches(batch_size=BATCH_SIZE):
        n = batch.num_rows
        total_input_rows += n
        if n == 0:
            continue

        lon_lo = batch.column("_lon_lo").to_numpy()
        lon_hi = batch.column("_lon_hi").to_numpy()
        lat_lo = batch.column("_lat_lo").to_numpy()
        lat_hi = batch.column("_lat_hi").to_numpy()

        c_lon_lo = lon_lo // _FINE_PER_COARSE_SIDE
        c_lon_hi = lon_hi // _FINE_PER_COARSE_SIDE
        c_lat_lo = lat_lo // _FINE_PER_COARSE_SIDE
        c_lat_hi = lat_hi // _FINE_PER_COARSE_SIDE

        if region_bbox_cells is not None:
            r_clo, r_chi, r_alo, r_ahi = region_bbox_cells
            c_lon_lo = np.maximum(c_lon_lo, r_clo)
            c_lon_hi = np.minimum(c_lon_hi, r_chi)
            c_lat_lo = np.maximum(c_lat_lo, r_alo)
            c_lat_hi = np.minimum(c_lat_hi, r_ahi)

        n_lon = (c_lon_hi - c_lon_lo + 1).astype(np.int64)
        n_lat = (c_lat_hi - c_lat_lo + 1).astype(np.int64)
        n_lon = np.where(n_lon > 0, n_lon, 0)
        n_lat = np.where(n_lat > 0, n_lat, 0)
        n_cells = n_lon * n_lat
        total_cells_in_batch = int(n_cells.sum())
        if total_cells_in_batch == 0:
            continue

        # ---- Fast path split: single-cell rows go vectorized ----
        # Most features have small bbox -> exactly 1 coarse cell. Keep that
        # path 100% numpy. Multi-cell rows (long roads etc.) go through a
        # narrow Python loop that's bounded by their (much smaller) count.
        single_mask = (n_cells == 1)
        multi_mask = (n_cells > 1)

        single_rows = np.where(single_mask)[0]
        if single_rows.size:
            single_cl = c_lon_lo[single_rows].astype(np.int64)
            single_ca = c_lat_lo[single_rows].astype(np.int64)
        else:
            single_cl = np.empty(0, dtype=np.int64)
            single_ca = np.empty(0, dtype=np.int64)

        multi_rows_list: list[int] = []
        multi_cl_list: list[int] = []
        multi_ca_list: list[int] = []
        if multi_mask.any():
            multi_rows_arr = np.where(multi_mask)[0]
            for r in multi_rows_arr:
                lo_c = int(c_lon_lo[r])
                hi_c = int(c_lon_hi[r])
                lo_a = int(c_lat_lo[r])
                hi_a = int(c_lat_hi[r])
                ri = int(r)
                for cl in range(lo_c, hi_c + 1):
                    for ca in range(lo_a, hi_a + 1):
                        multi_rows_list.append(ri)
                        multi_cl_list.append(cl)
                        multi_ca_list.append(ca)

        # Concat single + multi expansions
        row_idx_flat = np.concatenate([
            single_rows.astype(np.int64),
            np.asarray(multi_rows_list, dtype=np.int64),
        ])
        cl_flat = np.concatenate([
            single_cl,
            np.asarray(multi_cl_list, dtype=np.int64),
        ])
        ca_flat = np.concatenate([
            single_ca,
            np.asarray(multi_ca_list, dtype=np.int64),
        ])

        # Composite key for groupby — ca range is 0..1799, leave headroom: × 4096
        composite = cl_flat * 4096 + ca_flat
        sort_order = np.argsort(composite, kind="stable")
        composite_sorted = composite[sort_order]
        row_idx_sorted = row_idx_flat[sort_order]
        cl_sorted = cl_flat[sort_order]
        ca_sorted = ca_flat[sort_order]

        # Group boundaries
        change_points = np.where(np.diff(composite_sorted) != 0)[0] + 1
        starts = np.concatenate([[0], change_points])
        ends = np.concatenate([change_points, [len(composite_sorted)]])

        # ONE write per (cl, ca) group instead of one per row. This is the
        # 50-100x win over the per-row variant.
        for s, e in zip(starts, ends):
            cl = int(cl_sorted[s])
            ca = int(ca_sorted[s])
            group_indices = row_idx_sorted[s:e]
            sub_batch = batch.take(pa.array(group_indices))
            sink.write(cl, ca, pa.Table.from_batches([sub_batch], schema=batch.schema))
            total_output_rows += int(e - s)

    n_buckets, _ = sink.close()
    return total_input_rows, total_output_rows, n_buckets


def _schema_without_aux(schema: pa.Schema) -> pa.Schema:
    """Pass 2 still needs _lon_lo/hi + _lat_lo/hi for range filtering — keep them.

    This helper is kept for symmetry; today it's a passthrough.
    """
    return schema


def _process_intermediate_entry(args):
    """Module-level entry point for multiprocessing — picklable."""
    (path_str, coarse_dir_str, worker_id, region_cells) = args
    return _process_intermediate(Path(path_str), Path(coarse_dir_str), worker_id, region_cells)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--theme", required=True)
    p.add_argument("--workdir", default=None)
    p.add_argument("--workers", type=int, default=None,
                   help="parallel processes (default: cpu_count - 2)")
    p.add_argument("--bbox", default=None,
                   help="restrict bucketing to lon_lo,lat_lo,lon_hi,lat_hi degrees")
    p.add_argument("--release", default=None, help="informational")
    args = p.parse_args()

    workdir = resolve_workdir(args.workdir)
    n_workers = args.workers or max(1, (os.cpu_count() or 4) - 2)
    print(f"v12 Pass 1.5 (Python-orchestrated coarse bucketing)  theme={args.theme}")
    print(f"  workdir   = {workdir}")
    print(f"  workers   = {n_workers}")

    intermediates = _list_intermediates(workdir, args.theme)
    if not intermediates:
        print(f"[error] no intermediates found under {staging_dir_for_theme(workdir, args.theme)}",
              file=sys.stderr)
        sys.exit(1)
    print(f"  inputs    = {len(intermediates)} intermediate parquets")

    coarse_dir = coarse_dir_for_theme(workdir, args.theme)
    if (coarse_dir / _COARSE_DONE_MARKER).exists():
        print(f"  coarse already _DONE at {coarse_dir} — nothing to do.")
        return
    if coarse_dir.exists():
        for child in coarse_dir.iterdir():
            try:
                if child.is_dir():
                    shutil.rmtree(child)
                else:
                    child.unlink()
            except Exception:
                pass
    coarse_dir.mkdir(parents=True, exist_ok=True)

    region_cells: tuple[int, int, int, int] | None = None
    if args.bbox:
        parts = [float(x) for x in args.bbox.split(",")]
        if len(parts) != 4:
            raise SystemExit("--bbox needs 4 comma-separated values")
        rb_clo = int((parts[0] + 180) / START_CELL_DEG) // _FINE_PER_COARSE_SIDE
        rb_chi = int((parts[2] + 180) / START_CELL_DEG) // _FINE_PER_COARSE_SIDE
        rb_alo = int((parts[1] + 90) / START_CELL_DEG) // _FINE_PER_COARSE_SIDE
        rb_ahi = int((parts[3] + 90) / START_CELL_DEG) // _FINE_PER_COARSE_SIDE
        region_cells = (rb_clo, rb_chi, rb_alo, rb_ahi)
        print(f"  region    = coarse cells lon[{rb_clo}..{rb_chi}] lat[{rb_alo}..{rb_ahi}]")

    print(f"  output    = {coarse_dir}")
    print()

    t0 = time.time()

    tasks = [
        (str(p_), str(coarse_dir), i, region_cells)
        for i, p_ in enumerate(intermediates)
    ]

    total_in = 0
    total_out = 0
    bucket_set: set[tuple[int, int]] = set()

    with mp.Pool(processes=n_workers) as pool:
        for i, (in_n, out_n, n_buckets) in enumerate(pool.imap_unordered(_process_intermediate_entry, tasks), start=1):
            total_in += in_n
            total_out += out_n
            elapsed = time.time() - t0
            print(f"  [{i}/{len(tasks)}] in={in_n:,} out={out_n:,} buckets={n_buckets}  elapsed={elapsed/60:.1f} min", flush=True)

    elapsed = time.time() - t0
    bucket_total = sum(
        1 for d in coarse_dir.iterdir()
        if d.is_dir() and d.name.startswith("coarse_lon=")
        for _ in d.iterdir() if _.is_dir() and _.name.startswith("coarse_lat=")
    )
    print()
    print(f"v12 Pass 1.5 done: {total_in:,} input rows -> {total_out:,} bucketed rows")
    print(f"  unique coarse buckets: {bucket_total}")
    print(f"  elapsed: {elapsed/60:.1f} min")

    (coarse_dir / _COARSE_DONE_MARKER).write_text(_iso_now(), encoding="utf-8")
    print(f"  marker written: {coarse_dir / _COARSE_DONE_MARKER}")


if __name__ == "__main__":
    main()
