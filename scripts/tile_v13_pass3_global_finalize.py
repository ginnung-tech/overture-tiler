#!/usr/bin/env python3
"""tile_v13_pass3_global_finalize.py — cycle-end z<=5 boundary merge.

Stub for v1. The 24/7 driver invokes this once at the end of each cycle
(after all 36 peels have run pass3-local + uploaded). The full
implementation would:

  1. Read all 36 per-peel manifests from `driver-state/per_peel_manifests/`.
  2. Identify z=6 tiles whose 4 z=5-parent siblings all exist somewhere
     across the global tile set.
  3. Download those z=6 tiles from R2 (per-peel cleanup already wiped
     them locally), merge into the z=5 parent, upload the parent, and
     R2-delete the obsoleted z=6 children.
  4. Recurse up: z=5 → z=4 → z=3 → z=2 → z=1.
  5. Rewrite the global index.

The cardinality is small (≤ ~4000 z=6 tiles → ≤ ~1500 z=5/z=4/z=3
candidates) so the bandwidth cost is bounded.

For v1, this is a stub: the cycle proceeds with z=6 as the floor for
sparse regions. Open ocean stays as ~4000 z=6 tiles (~30 MB total)
rather than collapsing to ~16 z=2/z=3 tiles. Acceptable for first-cut
operation; revisit once we measure storage + first-paint latency on
real viewports.

When the real implementation lands it lives behind the same `run()`
entrypoint, so the driver's call site doesn't change.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tile_v13_helpers import resolve_workdir  # noqa: E402
from tile_v13_sentry import init_sentry, log_event  # noqa: E402


def run(workdir: Path, cycle: int = 0) -> dict:
    """Cycle-end finalize. v1 stub — see module docstring."""
    log_event(
        "tiler.cycle_finalize_skipped",
        level="info",
        component="finalize",
        cycle=cycle,
        reason="v1_stub_z6_floor",
    )
    return {
        "cycle": cycle,
        "tiles_merged": 0,
        "stub": True,
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--workdir", default=None)
    p.add_argument("--cycle", type=int, default=0)
    args = p.parse_args()

    init_sentry("finalize")
    workdir = resolve_workdir(args.workdir)
    result = run(workdir, cycle=args.cycle)
    print(f"v13 pass3 global finalize (v1 stub): {result}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
