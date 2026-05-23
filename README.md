# Overture tiling pipeline

Tiles Overture Maps Foundation themes (water, land, segments, buildings, land_use, infrastructure) into a vector-tile pyramid suitable for serving from a CDN. This document covers the **v13** architecture (current direction) and the **v11/v12** lessons that motivated the redesign.

Last updated: 2026-05-23 — 24/7 driver wired (`tile_v13_driver.py` is now the production entrypoint; legacy v11/v12 + v13 one-shot paths preserved for migration / offline backfill).

---

## TL;DR — v13 (current direction)

- **Combine all 6 themes per tile** — one parquet per `(z, x, y)` containing all themes with a `theme` column. Vector-tile-spec aligned; cuts download cost ~6× over the v11/v12 per-theme tiles.
- **Web Mercator `z/x/y` addressing** — replaces v12's custom 0.1° lon/lat grid. Standard, joinable, every VT renderer understands it.
- **Leaf-only output, adaptive depth** — `z_max = 14` (typical global VT) with adaptive subdivide to `z = 15` only for tiles that exceed the 20 MB budget. No full pyramid.
- **Bottom-up merge for sparse regions** — emit at `z_max`, fold 4 siblings → parent when combined < 20 MB, recurse upward. Open ocean collapses to z=2/3.
- **Peel-sharded execution** — 10° lng peels (36 work units, ±85° lat extent). Independent per-peel processing for parallelism.
- **Single fast SSD (USB 4, ~3 GB/s sustained, APFS)** holds raw + staging + tiles. Internal NVMe holds OS + macOS swap + DuckDB `/tmp` spill. **No drainer.**
- **Memory: 16 GB DuckDB allocation** (4 workers × 4 GB) on 8 GB physical, leveraging macOS compression + swap on internal NVMe.

---

## Why v13 — what v11/v12 taught us

Each of the seven decisions above is a direct response to a v11/v12 production-run finding. None of them is theoretical.

| v13 decision | v11/v12 finding that drove it |
| --- | --- |
| Combine themes per tile | 6 themes × 20 MB = 120 MB per region — clients downloading multiple files for one viewport. MVT/MapBox spec already supports multi-layer tiles natively. |
| Web Mercator z/x/y | v12's custom equirect grid was self-imposed: incompatible with stock VT renderers, can't join with OSM tiles, and required custom client code. Cost > benefit. |
| Leaf-only, adaptive depth | A full pyramid doubles storage. Most viewers only need leaves; the few that want zoom-out can be added later. |
| z_max=14 with z=15 fallback | v12 emitted at min cell size (0.001°) globally. Manhattan tiles fit comfortably; open-ocean tiles produced thousands of <1 MB files for one logical polygon. |
| Bottom-up merge | v11 had upward merge; v12 dropped it. Result: §2.6 below — open ocean over-splits, ~30× more tiles than necessary. |
| Single-volume APFS SSD | The v11/v12 internal-NVMe-hot + external-exFAT-cold split spawned the entire drainer system (§3) — a permanent operational dependency for what should be one tar at the end. APFS on a fast USB 4 enclosure handles small files at near-internal speed (§2.2 root cause was exFAT, not USB bandwidth). |
| 10° peels | Embarrassingly parallel by longitude shard; per-peel memory predictable; balances shard overhead against load granularity. |

---

## v13 architecture

### Pipeline (4 passes)

```
raw parquet (S3)
   │
   ▼  tile_v13_pass1.py
.tile-staging/v13/per_theme/<theme>/mercator_x=X/mercator_y=Y/*.parquet
   │  (per-theme z=6 quadkey partition; theme column added)
   ▼  tile_v13_pass1_5.py
.tile-staging/v13/combined/z6_<x>_<y>.parquet
   │  (all themes union'd per z=6 bucket; zstd-3)
   ▼  tile_v13_pass2.py
tiles/14/<x>/<y>.parquet
   │  (peel-sharded leaf emit; z=14 default; oversize → marked)
   ▼  tile_v13_pass3_merge.py
tiles/<z>/<x>/<y>.parquet
      (Phase A: subdivide z=14 oversize → z=15 children;
       Phase B: bottom-up merge — fold 4 siblings → parent if combined < 20 MB)
```

### Schema (the contract)

Every leaf tile is a single zstd-3 parquet:

```
theme           STRING        # 'water' | 'land' | 'segments' | 'buildings' | 'land_use' | 'infrastructure'
id              STRING        # Overture GERS ID
geometry        WKB           # WGS84 lon/lat
bbox            STRUCT        # {xmin, ymin, xmax, ymax} for fast filtering
... theme-specific columns, prefixed `<theme>_<col>` for any name collisions
```

Theme-specific columns are union'd via DuckDB `union_by_name=True`. Only `theme`, `id`, `geometry`, and `bbox` are guaranteed across all rows.

### Decisions locked 2026-05-07

| # | Decision | Why |
| --- | --- | --- |
| 1 | Mercator `z/x/y`, EPSG:3857, ±85.0511° extent | Industry standard; every renderer; joinable with OSM tiles |
| 2 | Combine all 6 themes per tile (`theme` column for client filter) | One download per region; matches MVT multi-layer convention |
| 3 | Leaf-only output, adaptive depth | Smaller storage; pyramid can be added later if UX demands |
| 4 | `z_max = 14`, adaptive subdivide to `z = 15` only when tile > 20 MB | Preserves detail in dense regions, avoids inflation in sparse |
| 5 | 10° lng peels, 36 work units | Middle ground between load-balance (5°) and shard overhead (15°) |
| 6 | Bottom-up merge over top-down size estimation | Estimation requires reading the data anyway; merge is deterministic |
| 7 | Single fast external APFS SSD; internal reserved for swap + spill | Independent I/O paths; obeys `feedback_use_all_resources.md` |
| 8 | Multi-bucket features assigned by centroid only (v1) | Avoids UNNEST (v11's bottleneck); >99% of features fit in one z=6 bucket; revisit if seam artifacts surface |

### Tunables (post fast-SSD)

| Var | Default | Rationale |
| --- | --- | --- |
| `WORKERS` | 4 | One per peel-in-flight; matches Mac mini's 4 perf cores |
| `MEMORY` | `4GB` | Per-worker DuckDB allocation. 4×4 = 16 GB total, leverages macOS compression + swap on internal |
| `Z_MAX` | 14 | Web Mercator standard for global VT; adaptive subdivide to 15 in dense |
| `TILE_BUDGET` | `20 MB` | Combined-theme target |
| `PEEL_WIDTH_DEG` | 10 | 36 peels |
| `LAT_BOUNDS` | `(-85.0511, 85.0511)` | Web Mercator native extent |
| `OVERTURE_WORKDIR` | `/Volumes/SSD/overture` | Single APFS volume on USB 4; internal stays for OS + swap |

If swap pressure causes paging stalls (observe via `vm_stat 5`): drop `MEMORY` to 3 GB or `WORKERS` to 3 → 9–12 GB total. Easy dial.

### Pass2 algorithm (peel-sharded, combined)

```python
PEEL_WIDTH_DEG = 10
PEELS = 36                          # 360 / 10
LAT_BOUNDS = (-85.0511, 85.0511)    # Web Mercator extent
Z_MAX = 14
TILE_BUDGET = 20 * 1024 * 1024      # 20 MB

for peel_idx in range(PEELS):
    peel_lng_lo = -180 + peel_idx * PEEL_WIDTH_DEG
    peel_lng_hi = peel_lng_lo + PEEL_WIDTH_DEG
    z6_keys = mercator_z6_keys_in_lng_range(peel_lng_lo, peel_lng_hi, LAT_BOUNDS)

    for z6_key in z6_keys:
        bucket_path = staging / f"z6_{z6_key.x}_{z6_key.y}.parquet"
        emit_leaves_in_bucket(bucket_path, z_max=Z_MAX, budget=TILE_BUDGET)
```

Workers parallel-process *different peels* (not different buckets within a peel) — keeps per-worker memory predictable.

### Pass3 merge algorithm

```python
def merge_pyramid(tile_dir: Path, budget: int):
    z_max_observed = max_z_in_dir(tile_dir)
    for z in range(z_max_observed, 0, -1):
        for parent, children in group_by_parent(z).items():
            if len(children) != 4:
                continue
            if sum(c.size_bytes for c in children) > budget:
                continue
            parent_tile = concat_parquets([c.path for c in children])
            if parent_tile.size <= budget:
                write(parent.path, parent_tile)
                for c in children: c.path.unlink()
```

Only emits a parent if all 4 siblings exist AND merged size fits. Asymmetric sparse regions (ocean meets coast) keep their fine-grain coast tiles.

---

## Implementation status

### v13 scripts (current)

- `tile_v13_helpers.py` — Mercator z/x/y math, peel helpers, DuckDB connection factory, path layout
- `tile_v13_pass1.py` — per-theme z=6 Mercator partition (theme column added)
- `tile_v13_pass1_5.py` — combined-theme bucket collation (zstd-3)
- `tile_v13_pass2.py` — peel-sharded leaf emit at z=14
- `tile_v13_pass3_merge.py` — Phase A z=15 subdivision + Phase B bottom-up merge
- `headroom_monitor.sh` — sidecar `df -h /` logger every 60s

### v11/v12 scripts (legacy, kept for migration)

- `tile.py`, `tiler.py` — the v11 quadtree implementation; `tile.py` adds `--pass1-only` for the v12 download-then-tile split
- `tile_v12_pass1_5.py`, `tile_v12_pass2.py` — the v12 batch-by-bucket pass2 that emits per-fine-cell tiles in custom 0.1° grid addressing
- `download.py` — async S3 download (used by both v12 and v13)
- `run_all.sh` — orchestrator; runs v13 by default, v12 path behind `RUN_LEGACY=1`
- `drain_tiles.sh` — **deleted in v13 stage 1** (no drainer needed on single-volume APFS)
- `upload.sh` — bucket sync (updated in v13 stage 3 for the new `tiles/{z}/{x}/{y}.parquet` layout)

### Known legacy bugs (superseded by v13)

These were flagged during PR #55 review and apply to v11/v12 paths only. **v13 redesign supersedes all of them by architecture, not by patch:**

1. **`tile.py` checkpoint missing `bbox` field** (Sentry HIGH) — resume reads from checkpoint with no bbox, defaults to `[0,0,0,0]`, corrupts manifest. v13 doesn't use this checkpoint shape.
2. **`tile.py` manifest double-entry on resume** (Sentry HIGH) — done tiles added once from checkpoint (corrupted bbox) and once from `tile_cell()`. v13's manifest is built from the merge pass output, no double-write path.
3. **`download.py` per-file checkpoint not flushed mid-batch** (CodeRabbit Major) — completed files stay marked `in_progress` until `gather()` returns; mid-run crash redownloads. v13 inherits `download.py` and benefits from a future fix here, but the bug is bounded to one re-download cycle.
4. **`download.py` `progress["files_done"]` never updated mid-run** (CodeRabbit Major) — `progress_reporter()` reads stale 0 throughout the run.

If you're operating v11/v12 (RUN_LEGACY=1), be aware. If you're on v13 (default), bugs 1–2 don't apply; bugs 3–4 affect only the download phase and don't corrupt output.

---

## How to operate

### 24/7 driver (steady-state, default)

```bash
cd /Volumes/SSD-2TB/overture/scripts
export OVERTURE_WORKDIR=/Volumes/SSD-2TB/overture
export OVERTURE_RELEASE=2026-04-15.0           # pin Overture release for this cycle
export SENTRY_DSN_OVERTURE=<dsn>               # optional; stderr-only without it
export R2_ACCESS_KEY_ID=...
export R2_SECRET_ACCESS_KEY=...
export R2_ACCOUNT_ID=...

# Run forever (default mode of run_all.sh — execs tile_v13_driver.py).
nohup ./run_all.sh > logs/driver.log 2>&1 &
```

The driver processes one peel at a time in eastward-from-0° order
(peel_idx 18, 19, …, 35, 0, 1, …, 17), uploads each peel's tiles to
the `overture-tiles` Cloudflare R2 bucket as it completes, deletes
the local data, and starts the next cycle. State persists in
`driver-state/driver_state.json`; `SIGTERM` triggers a clean exit
and a re-run resumes.

Two stages run concurrently, one peel apart:

| Stage A (I/O) | Stage B (CPU) |
| --- | --- |
| `pass1_per_peel` reads S3 with bbox filter → `staging/peel_<N+1>/combined/` | `pass2.run_one_peel(N)` emits `tiles/peel_<N>/{z}/{x}/{y}.parquet` |
| | `pass3_local.run_one_peel(N)` Phase A + B within the peel (stops at z=6) |
| | `update_global_index(N)` rewrites `driver-state/tiles_index.json` |
| | `bash upload.sh --peel-idx N` (strips peel prefix on R2; no-cache index) |
| | `rm -rf staging/peel_<N>, tiles/peel_<N>, duckdb-tmp/peel_<N>` |

Smoke test:

```bash
./run_all.sh --max-peels 1 --start-peel-idx 18 --no-prefetch
```

### One-shot v13 global (offline backfill)

```bash
# 1. Download raw locally (one-time per Overture release)
./download.py --themes water,land,segments,buildings,land_use,infrastructure

# 2. Run the global pipeline
RUN_V13_GLOBAL=1 ./run_all.sh
```

Runs `tile_v13_pass1.py` → `tile_v13_pass1_5.py` → `tile_v13_pass2.py` → `tile_v13_pass3_merge.py` end-to-end against local raw. Used for offline backfill / single-release rebuilds; NOT the 24/7 steady-state path.

### Operate v11/v12 (legacy)

```bash
RUN_LEGACY=1 ./run_all.sh
```

### Verify a tile

```bash
# Local during a peel (file deleted after upload):
duckdb -c "SELECT theme, COUNT(*) FROM read_parquet('tiles/peel_18/14/8567/5145.parquet') GROUP BY 1"

# Live on R2:
duckdb -c "SELECT theme, COUNT(*) FROM read_parquet('https://pub-<acct>.r2.dev/overture-tiles/tiles/14/8567/5145.parquet') GROUP BY 1"
```

### Index file (SPA consumer contract)

After every peel upload, the global tile index is rewritten on R2 at:

```text
https://pub-${R2_ACCOUNT_ID}.r2.dev/overture-tiles/tiles/tiles_index.json
```

Always served with `Cache-Control: no-store, no-cache, must-revalidate, max-age=0` so clients always see the latest peel data. Schema (see [`scripts/tile_v13_index.py`](scripts/tile_v13_index.py)):

```json
{
  "schema_version": 1,
  "cycle": 7,
  "tile_count": 142318,
  "peels": [
    {"peel_idx": 18, "lng_lo": 0.0, "lng_hi": 10.0, "vintage": "2026-05-22T09:11:00Z"}
  ],
  "tiles": [
    {"z": 14, "x": 8567, "y": 5145, "size_bytes": 1843200, "bbox": [12.65, 55.66, 12.67, 55.68], "vintage": "..."}
  ]
}
```

Reference viewport filter: `tile_v13_index.viewport_tiles(index, west, south, east, north, z_min, z_max)`.

### Upload to CDN (manual one-shot)

```bash
./upload.sh                       # whole tiles/ tree (one-shot use)
./upload.sh --peel-idx 18         # one peel (used by the driver)
```

### Sentry observability

New Sentry project `overture-tiler` (env tag `mac-mini-prod` by default).
Every event carries `component`, `cycle`, `peel.idx`, `peel.lng_lo` for
dashboard widgets. Key events:

| Message | Level | When |
| --- | --- | --- |
| `tiler.startup` | info | Driver boots |
| `tiler.cycle_start` / `tiler.cycle_done` | info | Cycle boundary |
| `tiler.peel_start` / `tiler.peel_done` | info | Peel boundary (with `total_duration_sec`) |
| `tiler.pass1_per_peel_done` / `pass2_done` / `pass3_local_done` / `upload_done` / `cleanup_done` | info | Per phase |
| `tiler.host_pressure` | info | Every 60 s (swap, disk, duckdb_tmp) |
| `tiler.oversize_tile` | warning | Phase A still couldn't fit a z=15 child |
| `tiler.prefetch_paused` | warning | Self-throttle on disk/swap pressure |
| `tiler.peel_failed` / `cleanup_failed` / `upload_failed` | error | Caught exceptions |

Without `SENTRY_DSN_OVERTURE` set, all of these go to stderr only.

Walks `tiles/{z}/{x}/{y}.parquet` and syncs to the configured bucket.

---

## Migration plan (v11/v12 → v13)

Each stage is independently reversible.

| Stage | What | Status |
| --- | --- | --- |
| 1 | Single-volume runtime: drop `drain_tiles.sh`, simplify `run_all.sh`, single `OVERTURE_WORKDIR` | ✅ Landed in v13 PR |
| 2a | Mercator partitioning + combined buckets (`tile_v13_pass1.py` + `tile_v13_pass1_5.py`) | ✅ Landed |
| 2b | Peel-sharded leaf emit + bottom-up merge (`tile_v13_pass2.py` + `tile_v13_pass3_merge.py`) | ✅ Landed |
| 2c | Switch `upload.sh` to v13 layout | ✅ Landed |
| 3 | `tile_path.exists()` skip + headroom monitor | ✅ Landed |
| 4 | Remove v11/v12 paths after first full v13 run validates | ⏳ Pending hardware (new SSD enclosure) |

---

## Lessons learned (v11/v12 operational notes)

These are the production-run findings that motivated v13. Each section is the cost of one re-run. Kept here as the source-of-truth for *why* the redesign happened, in case the v13 simplifications ever look excessive in hindsight.

### 1. SSD layout (the v11/v12 split that mattered then)

```
INTERNAL NVMe (APFS, 228 GiB)            EXTERNAL exFAT (466 GiB, Windows-compat)
─────────────────────────────            ──────────────────────────────────────
/Users/arbirk/overture/                  /Volumes/EXTERNAL/overture/
├── raw/<theme>/      (transient)        ├── raw/<theme>/             (persistent download cache)
├── .tile-staging/    (HOT, ~60–80 GB)   ├── tiles/<theme>_archives/  (tar bundles, final output)
└── tiles/<theme>/    (HOT, transient,   ├── scripts/                 (source of truth, all .py + .sh)
                       drained every     └── scripts/logs/            (run logs)
                       10 min, paused
                       during drain)
```

Per-workload routing:

| Workload | Where | Why |
| --- | --- | --- |
| Reading raw `.parquet` (download.py) | external → internal staging | Sequential reads from exFAT are fine; small writes are not. |
| Pass 1 staging writes (per-cell) | internal | Up to ~10 M small files. APFS handles this; exFAT chokes. |
| Pass 1.5 coarse bucket files | internal | Same reason — many medium files. |
| Pass 2 final tile writes | **internal**, then drained | Direct writes through a symlink to external stalled at zero progress for 30+ min. |
| Final archive (tar) | external | One large file per drain cycle. exFAT is fine with these. |

**v13 collapses this entire table to one row: everything on USB 4 APFS.** The split was forced by exFAT's small-file weakness, not by the bandwidth difference between volumes. APFS on USB 4 is functionally equivalent to internal NVMe for this workload.

### 2. Findings (chronological — each one cost a re-run)

#### 2.1 Internal disk filled mid-pass2 — no draining at all (first attempt)

Pass2 emits ~150 KB `.parquet.gz` tiles, ~1 M+ per theme. Writing them all to internal until cleanup at the end means 150–400 GB peak — more than the 122 GiB free we had. Pass2 died with `No space left on device` after producing ~150 k tiles.

#### 2.2 exFAT cannot keep up with small-file streams via symlink

Symlinked `tiles/water` to external. Pass2 hung: 33 minutes, 4 worker threads at 75 % CPU, **zero progress lines, zero tiles written**. Bottleneck: per-file metadata ops on exFAT (stat, allocate, fsync, dirent insert).

**Fix in v11/v12**: write to internal APFS, drain to external in batched tars. **Fix in v13**: use APFS everywhere; symlink-stall is gone.

#### 2.3 BSD tar's xattr handling wrecked the first real drain

First drainer cycle: 1,354,872 tiles → 52 GB tar in **1566 s**. Log filled with > 1 million lines of `Could not pack extended attributes: No space left on device` from libcopyfile bundling macOS resource forks for every file.

**Fix**: `COPYFILE_DISABLE=1 tar --no-mac-metadata`. Throughput jumped to ~145 MB/s. **v13 removes the tar phase entirely** (small files land directly on APFS final destination).

#### 2.4 Drain interval too long for dense regions

After the xattr fix, dense water regions produced ~6 GB/min. Internal accumulated ~70 GB before the 30-min drainer fired. Reduced INTERVAL to 10 min, then to 5 min in extreme cases.

#### 2.5 Workers≥2 still oversaturated internal — root cause was *contention*, not burn rate

The killer datum:

| Drain | pass2 state | Throughput |
| --- | --- | --- |
| pre-fix (xattr-laden), pass2 active | active | 31 MB/s |
| post-`COPYFILE_DISABLE`, pass2 active | active | 28 MB/s |
| post-`COPYFILE_DISABLE`, pass2 dead | dead | 145–207 MB/s |

Pass2 reads from `.tile-staging` and writes to `tiles/` on the same internal NVMe; tar reads from `tiles/`. All three streams compete for the same device.

**Fix in v11/v12**: pause pass2 (`SIGSTOP`) during drain. **Fix in v13**: split I/O explicitly — internal NVMe gets only swap + `/tmp` spill; external SSD gets all the data flow. No contention, no SIGSTOP dance.

#### 2.6 v12 pass2 over-splits sparse regions (the bottom-up merge motivation)

v11's `tile.py` used an adaptive quadtree: cells <20 MB were merged with siblings up the tree until the merged tile approached the budget. This collapsed open-ocean swaths into a few large tiles.

v12 pass2 only goes *down* — emits one tile per fine cell, subdivides if oversize. It never merges up. Result: thousands of tiny tiles in the middle of the ocean.

**Fix in v13**: bottom-up merge as Pass3 Phase B. Restored what v11 had + added the adaptive z=15 subdivision Phase A.

#### 2.7 v12 pass2 has no `tile_path.exists()` skip

Pass2 runs from a `_tiles.json` checkpoint, not from filesystem state. If the checkpoint is missing (e.g. wiped after a disk-full), every coarse bucket is re-rendered and previously-written tiles are silently overwritten. Not incorrect, but ~150 GB of wasted I/O when restarting.

**Fix in v13**: `tile_path.exists()` skip guard at the top of pass2's per-leaf loop, gated on `--no-skip-existing` for force re-runs.

#### 2.8 exFAT deletes are dog slow AND poison concurrent writes

`rm -rf` on a 151k-file directory on exFAT was killed after **2 hours** of running and had reclaimed essentially zero space. Allocation-unit metadata churn dominates. Worse: parallel `rm` collapsed drain throughput from 207 MB/s down to **1.1–2.3 MB/s**.

**v13 removes exFAT from the system entirely.** APFS deletes complete in seconds.

#### 2.9 Operational risk: filling internal breaks Claude itself

When internal hit ENOSPC, every Bash tool invocation failed because the harness writes its tool output to `/private/tmp/claude-501/...` on internal. The Read tool still worked, so logs could be inspected, but no commands could be run.

**Mitigation in v13**: data flow stays on external; internal headroom is enforced by the design. The headroom monitor (`headroom_monitor.sh`) logs `df -h /` every 60s as ops insurance.

---

## 3. Drainer (v11/v12) — historical reference

**v13 deletes `drain_tiles.sh`.** This section is preserved so an operator running v11/v12 (RUN_LEGACY=1) understands what the drainer did and why. It is not part of the v13 path.

Source: `scripts/drain_tiles.sh` (deleted in v13 PR #63).

**Trigger**: every `INTERVAL` seconds (default 1200 = 20 min; override 600 = 10 min for water).

**Cycle**: collect tiles older than 60s → pause pass2 (`SIGSTOP`) → tar to external → verify count → atomic rename → delete originals → resume pass2 (`SIGCONT`).

**Tunables (env vars)**: `INTERVAL`, `MIN_AGE_SEC`, `OVERTURE_WORKDIR`, `OVERTURE_EXTERNAL`.

**Worst-case internal pressure**: `INTERVAL × pass2_burn_rate`. At 600s and 6 GB/min ≈ 60 GB headroom needed.

---

## Open items

- [ ] Confirm new USB 4 SSD enclosure spec (sustained ≥3 GB/s) before first full v13 run.
- [ ] Decide whether `geometry` column should ship as WKB (current Overture) or quantized integer-mm (Lite-STEP convention) — separate transform pass between v13 pass2 and pass3 if the latter.
- [ ] Confirm consumer-side rendering handles z=15 children where they appear (some renderers expect uniform z).
- [ ] Decide tar-or-not at upload: `aws s3 sync` directly (~1.7 M files) vs. tar-by-z-prefix (~256 archives). Defer until consumer-side story is set.
- [ ] After first full v13 run validates output, schedule removal of v11/v12 legacy paths and the four legacy-bug callouts above.
- [ ] Multi-bucket feature centroid-only (decision #8) — sample a handful of z=6 seams after first run, look for visible truncation; if common, add neighbour-join as v13.1.
