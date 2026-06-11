Cycle 1 pipeline upgrade
Context
Cycle 0 (release 2026-05-20.0) is fully built and live on R2 (latest.json points to it). Before re-running, we want cycle 1 to:

produce smaller tiles (40 MB max, down from 50) via better compression + per-zoom coordinate quantization;
additionally publish giant natural features intact in a sidecar layer (while still clipping them into normal tiles exactly as today);
be followable from the terminal without external monitors;
never disturb the live release — latest.json flips to cycle 1 only when cycle 1 fully completes;
adopt the recorded structural fix: 11.25° peels (no seam collisions/overlaps).
Cycle 1 re-tiles the same Overture source 2026-05-20.0 into a new output prefix 2026-05-20.0-2, so the live tiles are untouched until the new build is done and verified.

Decisions (locked with user)
Coordinate quantization: snap-to-grid, keep WGS84 (non-breaking). Global grid, step(z) = 360 / 2^(z+12) (= 4096 steps per tile width).
Sidecar keep-whole set: theme ∈ {land, water, land_use} AND ST_NPoints > 50000. Giants still clipped into normal tiles as now — sidecar is purely additive (intact copy, stored once).
Output prefix: 2026-05-20.0-2. Source data: 2026-05-20.0.
Peel width: 11.25° (32 peels). Index sharding: deferred (keep monolithic tiles_index.json).
New client docs: client_guide_cycle_1.md (do not edit the cycle-0 CLIENT_GUIDE.md).
Changes
A. Smaller tiles
Budget 50→40 MB — tile_v13_helpers.py:56 TILE_BUDGET_DEFAULT = 40 * 1024 * 1024.
zstd level 3→12 — tile_v13_pass2.py:391. Add named const (e.g. ZSTD_LEVEL = 12) in helpers; emit is no longer the bottleneck (verify is), so the CPU cost is fine.
Per-zoom coordinate quantization — in the emit COPY (tile_v13_pass2.py:349-393), wrap the geometry in ST_ReducePrecision(geometry, <step>) where step = 360.0 / (2 ** (z + 12)) computed per _emit(z,...). Apply after the existing huge-geometry clip, then keep the existing NOT ST_IsEmpty drop and add an ST_IsValid/ST_MakeValid guard (ReducePrecision can collapse slivers). This shrinks raw bytes and dramatically improves zstd (snapped/aligned coords). Global grid → vertices align across tile & zoom boundaries.
B. Giant-feature sidecar (additive — normal tiles unchanged)
New per-peel extraction (new helper, called from the driver after stage_b_compute): select keep-whole giants (theme IN ('land','water','land_use') AND ST_NPoints(geometry) > 50000), unclipped, full precision, deduped by COALESCE(id,'osm:'||osm_id,'geo:'||md5(ST_AsWKB(geometry))), written once to tiles/{output}/_large/peel_{n}.parquet.
Cross-peel dedup: a giant spanning peels appears in two peels' extracts → dedup by id when merging into the published set (reuse the pattern in merge_boundary_tiles.py).
Index: add a top-level large_features array to tiles_index.json — {id, theme, bbox, npoints, path} — so clients fetch only sidecar files intersecting their viewport. Oversize is allowed and expected here.
The normal pyramid emit is unchanged (still clips >50k-vertex geoms into tiles), so no replication runaway and no client regression.
C. Loop progress logging (follow without monitors)
run_tiler.sh already tees to tiler.log + stdout. Add a concise human-readable digest line from the driver loop (tile_v13_driver.py) after each peel and at stage transitions, e.g.:
[progress] cycle=1 peel 12/32 (lng -45.0..-33.75) done — 358 tiles, 0 oversize | computed 12/32 | disk 1.0TiB free
Print with flush=True (tees to log). Default on; no monitors needed.
D. Gate latest.json to cycle completion
upload.sh: guard the latest.json write block (:171-192) behind PUBLISH_LATEST (default 1). Per-peel uploads from the driver pass PUBLISH_LATEST=0 → they upload tiles + per-release index but do not touch latest.json.
At cycle finalize (tile_v13_driver.py:877 finalize.run, after all uploads verified), write+upload latest.json once, pointing at the output prefix. Implement as a small upload.sh mode (--publish-latest-only) or a direct rclone copy in finalize.
Side benefit: because latest never points at 2026-05-20.0-2 until done, in-progress-index transient 404s are invisible to clients — the documented cycle-1 "index leads uploads" hazard is neutralized for latest-based clients without reordering index/upload.
E. Peel width 11.25°
tile_v13_helpers.py:62 PEEL_WIDTH_DEG_DEFAULT → float 11.25. Update int-typed --peel-width-deg argparse to float (driver + any pass scripts). 360/11.25 = 32 peels; boundaries land on z6 column edges (5.625°×2) → straddling buckets impossible → seam collisions AND cross-zoom overlaps structurally gone.
Consequence: _run_merge_boundary and _prune_index become no-ops (nothing to merge/prune). Skip them when peel_width % 5.625 == 0 (cheap guard) — saves the O(n²) per-peel verify churn too. Keep the code for the 5° path.
Watch: 11.25° peels are denser per peel on the 8 GB Mac. Keep the proven 3 workers / 4 GB. The huge-geometry clip + 40 MB budget + quantization keep per-tile size bounded; per-peel disk footprint is larger but cleanup is per-peel.
F. Decouple source-release from output-prefix
Add OUTPUT_RELEASE env / --output-release arg. Default = OVERTURE_RELEASE (back-compat for cycle 0 behavior).
Source (OVERTURE_RELEASE, unchanged): stage_a/pass1 Overture reads.
Output (OUTPUT_RELEASE): stage_b tile dir, upload.sh RELEASE, per-release index path, _verify_upload (driver.py:470 — change it to read the output prefix, not OVERTURE_RELEASE), latest.json target+body, sidecar _large/ path.
Persist both in driver-state/driver_state.json. Driver resolves output_release = env OUTPUT_RELEASE or state or release.
G. Docs — client_guide_cycle_1.md (new file)
40 MB tile budget.
Coordinate quantization: explain the global snap-to-grid, the step(z) = 360 / 2^(z+12) formula, and a precision table (z14 ≈ 0.6 m, z10 ≈ 9.5 m, etc.). Note coords remain WGS84 lng/lat, just rounded to render resolution; vertices are consistent across tiles/zooms.
Sidecar large_features: how to read the index array and fetch intact giant geometries for a viewport; note these files can exceed the tile budget by design and that the same features also appear clipped in normal tiles (dedupe by id if combining).
Peel-width note: no more seam overlaps in this release (so the cycle-0 "rely on the smallest covering tile" caveat no longer applies).
Files to modify
scripts/tile_v13_helpers.py — budget, zstd const, peel-width float.
scripts/tile_v13_pass2.py — quantization in emit COPY; zstd level.
scripts/tile_v13_driver.py — output-release plumbing, sidecar extraction call, progress logging, finalize latest-publish, skip merge/prune for aligned peels, _verify_upload output prefix, argparse floats.
scripts/upload.sh — PUBLISH_LATEST guard + --publish-latest-only mode; use output prefix.
scripts/tile_v13_index.py — add large_features array.
New: sidecar extraction helper (e.g. scripts/extract_large_features.py) + cross-peel dedup (mirror merge_boundary_tiles.py).
New: client_guide_cycle_1.md.
tile_v13_pass3_global_finalize.py — publish latest.json at cycle end (or via upload.sh mode).
Verification
Dry single-peel run into a throwaway prefix on one sparse peel: confirm tiles emit, all ≤40 MB (except intended sidecar), quantized coords round-trip (spot-check a geometry’s coords sit on the grid), and a _large/peel_N.parquet appears for a peel containing a big lake/forest.
Size win check: compare a cycle-0 dense tile vs the cycle-1 rebuild of the same z/x/y — expect meaningful shrink from zstd-12 + quantization.
Sidecar correctness: pick a known giant (e.g. a large protected area); confirm it appears once in _large, intact (full vertex count), and also clipped in its normal tiles; confirm an index.large_features entry with correct bbox.
latest gating: during the run, https://tiles.ginnung.tech/tiles/latest.json must still resolve to 2026-05-20.0 until finalize; after finalize it flips to 2026-05-20.0-2.
Progress logging: confirm the digest lines stream to stdout/tiler.log so the run is followable with no monitors.
Peel alignment: confirm 0 boundary collisions / 0 seam overlaps reported (merge/prune skipped).
Cycle-1 start command (foreground, followable)
cd /Volumes/POWER/overture-tiler-main
export SENTRY_DSN_OVERTURE='<existing Sentry DSN>'
export OVERTURE_RELEASE=2026-05-20.0        # SOURCE data (unchanged)
export OUTPUT_RELEASE=2026-05-20.0-2        # cycle-1 OUTPUT prefix
export R2_ACCESS_KEY_ID=<...> R2_SECRET_ACCESS_KEY=<...> R2_ACCOUNT_ID=<...>
bash scripts/run_tiler.sh --workers 3 --memory-limit 4GB --no-prefetch
(State currently has cycle:1, completed:[], so this runs as cycle 1 over the 32 new peels and exits cleanly at the end — no auto cycle 2. latest flips to 2026-05-20.0-2 only at finalize.)

Deferred (flagged, not in this build)
Index sharding (monolithic index retained).
O(n²) per-peel verify rework (mostly mooted: fewer peels + merge/prune skipped + latest gated).
theme/theme_1 column rename.