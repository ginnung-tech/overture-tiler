"""
Overture Maps downloader — phase 1 of the two-phase pipeline.

Downloads raw Parquet files from the public Overture S3 bucket to a local
workdir so that the tiler (tile.py) can read from fast local NVMe instead of
querying S3 per cell (~150-300ms RTT from Denmark = 18-day projected run).

Download layout:
    <workdir>/raw/<theme>/          — one subdir per Overture theme
        <original_filename>.parquet — partition files, preserving S3 leaf name
        _download.json              — per-file checkpoint manifest

Checkpoint schema (one entry per file):
    {
        "file_key": "s3://overturemaps-us-west-2/release/.../part-00000.zstd.parquet",
        "local_path": "<workdir>/raw/<theme>/part-00000.zstd.parquet",
        "size_bytes": 525570468,
        "sha256": "abc123...",       -- populated on completion
        "status": "done"|"failed"|"in_progress",
        "at": "2026-05-03T14:32:00Z"
    }

Resumability:
    - "done"        -> skip (size check: re-verify local file size matches S3 size)
    - "failed"      -> retry (delete partial file if exists, re-download)
    - "in_progress" -> assumed crashed mid-write; delete partial + retry

CLI flags:
    --theme          Theme name (buildings, segments, land_use, water, land, infrastructure)
    --workdir        Output root. Overrides OVERTURE_WORKDIR env var.
    --release        Overture release tag. Overrides OVERTURE_RELEASE env var.
    --workers        Parallel download workers (default 8)
    --list-only      Print files + sizes, no download
    --dry-run        Log only, write nothing

Verified file counts + sizes for release 2026-04-15.0 (probed 2026-05-03):
    buildings      (theme=buildings/type=building):         512 files, ~269 GB
    segments       (theme=transportation/type=segment):     128 files,  ~60 GB
    land_use       (theme=base/type=land_use):               32 files,  ~20 GB
    water          (theme=base/type=water):                  32 files,  ~53 GB
    land           (theme=base/type=land):                   32 files,  ~37 GB
    infrastructure (theme=base/type=infrastructure):         16 files,  ~13 GB
    TOTAL:                                                  752 files, ~452 GB

Recommended processing order (smallest first):
    infrastructure -> land_use -> land -> water -> segments -> buildings

Dependencies: duckdb, httpx (async HTTP), Python 3.11+, hashlib, asyncio (stdlib)
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# SQL-safety helpers — see _sql_safety.py for the why.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from _sql_safety import q_path  # noqa: E402

# Strict pattern for Overture release tags. Examples: "2026-04-15.0", "2026-05-01.1".
_RELEASE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}(\.\d+)?$")
# Strict pattern for S3 file keys we accept back from `glob('s3://...')`.
# Overture's bucket only emits ASCII paths under release/<release>/.../<file>.parquet.
_S3_KEY_RE = re.compile(r"^s3://[A-Za-z0-9._\-]+/[A-Za-z0-9._\-/=]+\.parquet$")


def _safe_release(s: str) -> str:
    if not _RELEASE_RE.match(s):
        raise ValueError(f"unsafe release tag: {s!r}")
    return s


def _safe_s3_key(s: str) -> str:
    if not _S3_KEY_RE.match(s):
        raise ValueError(f"unsafe S3 key for SQL interpolation: {s!r}")
    return s

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OVERTURE_RELEASE = os.environ.get("OVERTURE_RELEASE", "2026-04-15.0")
S3_BASE = "https://overturemaps-us-west-2.s3.amazonaws.com"
S3_BUCKET = "overturemaps-us-west-2"

# S3 key prefix for each tiler theme (matches THEME_PATHS in tile.py)
# Files are named *.zstd.parquet — glob "**/*.parquet" in DuckDB matches these too.
THEME_S3_PREFIXES: dict[str, str] = {
    "buildings":      "release/{release}/theme=buildings/type=building/",
    "segments":       "release/{release}/theme=transportation/type=segment/",
    "land_use":       "release/{release}/theme=base/type=land_use/",
    "water":          "release/{release}/theme=base/type=water/",
    "land":           "release/{release}/theme=base/type=land/",
    "infrastructure": "release/{release}/theme=base/type=infrastructure/",
}

ALL_THEMES = list(THEME_S3_PREFIXES.keys())

CHECKPOINT_FILENAME = "_download.json"
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB read chunks


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

def checkpoint_path(raw_theme_dir: Path) -> Path:
    return raw_theme_dir / CHECKPOINT_FILENAME


def load_checkpoint(raw_theme_dir: Path) -> dict[str, dict]:
    """Return {file_key: entry} mapping from the checkpoint file."""
    cp = checkpoint_path(raw_theme_dir)
    if not cp.exists():
        return {}
    try:
        data = json.loads(cp.read_text(encoding="utf-8"))
        return {entry["file_key"]: entry for entry in data}
    except Exception as e:
        print(f"[warn] checkpoint read error ({cp}): {e} — starting fresh", flush=True)
        return {}


def save_checkpoint(raw_theme_dir: Path, entries: dict[str, dict]) -> None:
    cp = checkpoint_path(raw_theme_dir)
    cp.write_text(
        json.dumps(list(entries.values()), indent=2),
        encoding="utf-8",
    )


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# S3 file listing (via DuckDB glob — no aws CLI required)
# ---------------------------------------------------------------------------

def list_s3_files(theme: str, release: str) -> list[dict]:
    """
    Return list of {key, size_bytes} for all parquet files in the theme prefix.
    Uses DuckDB parquet_file_metadata to get exact sizes without downloading.
    Falls back to glob-only (no size) if parquet_file_metadata is unavailable.
    """
    import duckdb  # type: ignore

    # `theme` is constrained by argparse `choices=ALL_THEMES`; THEME_S3_PREFIXES
    # is a hard-coded literal. Still validate `release` because it can be set
    # via --release / OVERTURE_RELEASE.
    prefix = THEME_S3_PREFIXES[theme].format(release=_safe_release(release))
    s3_glob = f"s3://{S3_BUCKET}/{prefix}*.parquet"
    # Glob shape: bucket + prefix-from-validated-release + literal `*.parquet`.
    # The components have no character that can escape the SQL string literal.
    _safe_glob = q_path(s3_glob)

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_region='us-west-2';")
    con.execute("SET s3_use_ssl=true;")

    print(f"  Listing: {s3_glob}", flush=True)
    files = [row[0] for row in con.execute(f"SELECT file FROM glob('{_safe_glob}')").fetchall()]

    if not files:
        print(f"  [warn] No files found for theme={theme} release={release}", flush=True)
        return []

    print(f"  Found {len(files)} files — fetching sizes ...", flush=True)

    results = []
    for i, f in enumerate(files, 1):
        try:
            # The S3 key was returned by DuckDB's glob() against a trusted
            # bucket; re-validate before interpolation as a belt-and-braces
            # against a hostile bucket listing.
            safe_key = _safe_s3_key(f)
            row = con.execute(
                f"SELECT file_size_bytes FROM parquet_file_metadata('{safe_key}')"
            ).fetchone()
            size = row[0] if row else 0
        except Exception:
            size = 0
        results.append({"key": f, "size_bytes": size})
        if i % 64 == 0 or i == len(files):
            print(f"  Sizes: {i}/{len(files)}", flush=True)

    return results


# ---------------------------------------------------------------------------
# Async download engine
# ---------------------------------------------------------------------------

async def download_file(
    session,            # httpx.AsyncClient
    file_key: str,      # s3:// URL
    local_path: Path,
    expected_size: int,
    worker_id: int,
    progress: dict,     # shared mutable dict for progress tracking
) -> tuple[bool, str | None]:
    """
    Download one file. Returns (success, sha256_hex | error_message).
    Uses streaming download with chunked writes to handle large files.
    Verifies size on completion.
    """
    # Convert s3:// URL to HTTPS
    https_url = file_key.replace(
        f"s3://{S3_BUCKET}/",
        f"{S3_BASE}/",
    )

    local_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = local_path.with_suffix(".part")

    hasher = hashlib.sha256()
    bytes_written = 0

    try:
        async with session.stream("GET", https_url, timeout=300.0) as response:
            if response.status_code != 200:
                return False, f"HTTP {response.status_code}"
            with tmp_path.open("wb") as f:
                async for chunk in response.aiter_bytes(CHUNK_SIZE):
                    f.write(chunk)
                    hasher.update(chunk)
                    bytes_written += len(chunk)
                    progress["bytes_done"] += len(chunk)

        # Size verification
        actual_size = tmp_path.stat().st_size
        if expected_size > 0 and actual_size != expected_size:
            tmp_path.unlink(missing_ok=True)
            return False, f"size mismatch: got {actual_size}, expected {expected_size}"

        tmp_path.rename(local_path)
        return True, hasher.hexdigest()

    except Exception as e:
        tmp_path.unlink(missing_ok=True)
        return False, str(e)


# ---------------------------------------------------------------------------
# Progress reporter
# ---------------------------------------------------------------------------

async def progress_reporter(progress: dict, total_bytes: int, stop_event: asyncio.Event) -> None:
    """Print cumulative bytes / total + ETA every 10 seconds."""
    start = time.monotonic()
    while not stop_event.is_set():
        await asyncio.sleep(10)
        elapsed = time.monotonic() - start
        done = progress["bytes_done"]
        files_done = progress["files_done"]
        files_total = progress["files_total"]
        pct = done / total_bytes * 100 if total_bytes > 0 else 0
        mbps = done / elapsed / 1e6 if elapsed > 0 else 0
        remaining = (total_bytes - done) / (done / elapsed) if done > 0 else float("inf")
        eta = f"{remaining/3600:.1f}h" if remaining < float("inf") else "?"
        print(
            f"  [{files_done}/{files_total} files] "
            f"{done/1e9:.2f}/{total_bytes/1e9:.2f} GB ({pct:.1f}%) "
            f"@ {mbps:.1f} MB/s  ETA {eta}",
            flush=True,
        )


# ---------------------------------------------------------------------------
# Main download orchestrator
# ---------------------------------------------------------------------------

async def run_download(
    theme: str,
    release: str,
    raw_theme_dir: Path,
    workers: int,
    dry_run: bool,
    list_only: bool,
) -> None:
    try:
        import httpx  # type: ignore
    except ImportError:
        print("[error] httpx is required for download.py. Install: pip install httpx", file=sys.stderr)
        sys.exit(1)

    # List S3 files
    files = list_s3_files(theme, release)
    if not files:
        print(f"  No files found — nothing to do.", flush=True)
        return

    total_size = sum(f["size_bytes"] for f in files)
    print(
        f"\n  Theme: {theme}  |  Files: {len(files)}  |  "
        f"Total: {total_size/1e9:.2f} GB  |  Workers: {workers}",
        flush=True,
    )

    if list_only:
        for f in files:
            print(f"    {f['key']}  ({f['size_bytes']/1e9:.3f} GB)", flush=True)
        return

    if dry_run:
        print("  DRY RUN — no files will be written", flush=True)
        for f in files:
            print(f"  DRY  {Path(f['key']).name}  ({f['size_bytes']/1e6:.1f} MB)", flush=True)
        return

    # Load checkpoint
    raw_theme_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = load_checkpoint(raw_theme_dir)

    # Classify work
    to_download: list[dict] = []
    skipped = 0
    for f in files:
        key = f["key"]
        local_name = Path(key).name
        local_path = raw_theme_dir / local_name
        entry = checkpoint.get(key)

        if entry and entry["status"] == "done":
            # Verify by size
            if local_path.exists() and local_path.stat().st_size == f["size_bytes"]:
                skipped += 1
                continue
            else:
                print(f"  [warn] {local_name}: marked done but file missing/wrong size — requeuing", flush=True)
                entry["status"] = "failed"

        if entry and entry["status"] in ("failed", "in_progress"):
            # Clean up partial file
            local_path.unlink(missing_ok=True)
            part = local_path.with_suffix(".part")
            part.unlink(missing_ok=True)

        to_download.append({**f, "local_path": local_path})

    print(
        f"  Skipping {skipped} already-done files. Queued: {len(to_download)}",
        flush=True,
    )

    if not to_download:
        print("  All files already downloaded.", flush=True)
        return

    # Mark queued files as in_progress in checkpoint
    for f in to_download:
        checkpoint[f["key"]] = {
            "file_key": f["key"],
            "local_path": str(f["local_path"]),
            "size_bytes": f["size_bytes"],
            "sha256": None,
            "status": "in_progress",
            "at": _iso_now(),
        }
    save_checkpoint(raw_theme_dir, checkpoint)

    # Shared progress dict (no locking needed — asyncio single-thread)
    progress = {
        "bytes_done": 0,
        "files_done": skipped,
        "files_total": len(files),
    }
    queued_bytes = sum(f["size_bytes"] for f in to_download)
    stop_reporter = asyncio.Event()

    # Semaphore to cap concurrent workers
    sem = asyncio.Semaphore(workers)

    async def bounded_download(f: dict) -> tuple[str, bool, str | None]:
        async with sem:
            key = f["key"]
            local_path = f["local_path"]
            worker_id = 0  # cosmetic only
            ok, result = await download_file(
                client, key, local_path, f["size_bytes"], worker_id, progress
            )
            return key, ok, result

    async with httpx.AsyncClient(
        follow_redirects=True,
        headers={"User-Agent": "overture-tiler/download.py"},
        limits=httpx.Limits(max_connections=workers + 4, max_keepalive_connections=workers),
    ) as client:
        reporter_task = asyncio.create_task(
            progress_reporter(progress, queued_bytes, stop_reporter)
        )

        tasks = [asyncio.create_task(bounded_download(f)) for f in to_download]
        results = await asyncio.gather(*tasks, return_exceptions=False)

        stop_reporter.set()
        await reporter_task

    # Update checkpoint with results
    for key, ok, result in results:
        local_path = Path(checkpoint[key]["local_path"])
        if ok:
            checkpoint[key]["status"] = "done"
            checkpoint[key]["sha256"] = result
            checkpoint[key]["size_bytes"] = local_path.stat().st_size if local_path.exists() else 0
            progress["files_done"] += 1
        else:
            checkpoint[key]["status"] = "failed"
            print(f"  [fail] {Path(key).name}: {result}", flush=True)
        checkpoint[key]["at"] = _iso_now()

    save_checkpoint(raw_theme_dir, checkpoint)

    done_count = sum(1 for key, ok, _ in results if ok)
    fail_count = len(results) - done_count
    print(
        f"\n  Done: {done_count} downloaded, {fail_count} failed, {skipped} skipped.",
        flush=True,
    )
    if fail_count > 0:
        print(
            f"  Re-run the script to retry {fail_count} failed files.",
            flush=True,
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def resolve_workdir(cli_value: str | None) -> Path:
    if cli_value:
        return Path(cli_value)
    env_val = os.environ.get("OVERTURE_WORKDIR")
    if env_val:
        return Path(env_val)
    # Platform-aware defaults
    if sys.platform == "win32":
        return Path("D:/overture")
    return Path("/Volumes/SSD/overture")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download raw Overture Maps parquet files from S3 to local disk. "
            "Phase 1 of the download → tile pipeline."
        )
    )
    parser.add_argument(
        "--theme",
        required=True,
        choices=ALL_THEMES,
        help=f"Theme to download. Choices: {', '.join(ALL_THEMES)}",
    )
    parser.add_argument(
        "--workdir",
        default=None,
        help=(
            "Root output directory. Overrides OVERTURE_WORKDIR env var. "
            "Files land in <workdir>/raw/<theme>/. "
            "Default: D:\\overture (Windows) or /Volumes/SSD/overture (Mac)"
        ),
    )
    parser.add_argument(
        "--release",
        default=None,
        help=f"Overture release tag (default: env OVERTURE_RELEASE or {OVERTURE_RELEASE}).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Parallel download workers (default: 8).",
    )
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="Print files + sizes, no download.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log planned downloads without writing any files.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    release = args.release or OVERTURE_RELEASE
    workdir = resolve_workdir(args.workdir)
    raw_theme_dir = workdir / "raw" / args.theme

    print(f"Overture downloader  release={release}  theme={args.theme}")
    print(f"Workdir: {workdir}")
    print(f"Raw dir: {raw_theme_dir}")
    if args.dry_run:
        print("DRY RUN — no files will be written")
    if args.list_only:
        print("LIST ONLY — no download")
    print()

    asyncio.run(
        run_download(
            theme=args.theme,
            release=release,
            raw_theme_dir=raw_theme_dir,
            workers=args.workers,
            dry_run=args.dry_run,
            list_only=args.list_only,
        )
    )


if __name__ == "__main__":
    main()
