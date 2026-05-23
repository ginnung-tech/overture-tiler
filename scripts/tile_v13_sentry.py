"""tile_v13_sentry.py — Sentry wiring for the 24/7 Overture tiler.

The 24/7 driver and its sub-passes emit structured Info+ events to the
`overture-tiler` Sentry project so the user can build a dashboard from
the Logs dataset and the Performance/Spans view.

Conventions (every event):
- `component` — one of `driver` / `pass1_per_peel` / `pass2` /
  `pass3_local` / `upload` / `index` / `finalize`. One dashboard
  widget per component.
- `cycle` — int, monotonic from cycle 0.
- `peel.idx` — 0..35, the peel currently being touched.
- `peel.lng_lo` / `peel.lng_hi` — float, derived from peel_idx.
- `host` — `socket.gethostname()`; lets a multi-machine fleet
  coexist on the same project.

Init once per process via :func:`init_sentry`. Without `SENTRY_DSN_OVERTURE`
in the environment the module degrades to a no-op (writes to stderr only)
so the tiler can run locally without a Sentry project.
"""
from __future__ import annotations

import contextlib
import os
import socket
import sys
import time
from datetime import datetime, timezone
from typing import Any, Iterator

try:
    import sentry_sdk  # type: ignore
    _SENTRY_AVAILABLE = True
except ImportError:
    _SENTRY_AVAILABLE = False

DSN_ENV = "SENTRY_DSN_OVERTURE"
ENV_TAG_ENV = "OVERTURE_ENV"
RELEASE_ENV = "OVERTURE_RELEASE_TAG"

# Module-level state. Set by init_sentry(); read by log_event / peel_span.
_initialized = False
_component = "driver"
_base_tags: dict[str, Any] = {}


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _stderr(message: str, **attrs: Any) -> None:
    """Fallback when Sentry is not available — print a structured line to stderr."""
    pairs = " ".join(f"{k}={v}" for k, v in attrs.items())
    print(f"[{_iso_now()}] {message} {pairs}".rstrip(), file=sys.stderr, flush=True)


def init_sentry(component: str) -> None:
    """Initialise sentry_sdk for the current process.

    `component` becomes the default `component` tag on every event. The
    driver should call this once at startup; sub-passes invoked in the
    same Python process inherit the init and override component per-event
    via :func:`log_event` kwargs.

    Safe to call multiple times — re-init updates the component tag but
    keeps the same Hub/transport.
    """
    global _initialized, _component, _base_tags

    _component = component
    _base_tags = {
        "host": socket.gethostname(),
        "env": os.environ.get(ENV_TAG_ENV, "mac-mini-prod"),
    }

    dsn = os.environ.get(DSN_ENV)
    if not dsn:
        # No DSN — degrade to stderr-only. The driver still runs, just no
        # remote telemetry. Useful for local smoke tests.
        if not _initialized:
            _stderr(
                "tiler.sentry_disabled",
                reason=f"{DSN_ENV} not set",
                component=component,
            )
        _initialized = True
        return

    if _initialized:
        # Already configured; just update the component tag and return.
        sentry_sdk.set_tag("component", component)
        return

    if not _SENTRY_AVAILABLE:
        _stderr(
            "tiler.sentry_disabled",
            reason="sentry_sdk import failed",
            component=component,
        )
        _initialized = True
        return

    sentry_sdk.init(
        dsn=dsn,
        environment=_base_tags["env"],
        release=os.environ.get(RELEASE_ENV),
        # Logs dataset: Info+ on staging, Error+ in production per DEBUG_FLOW.
        # Tiler runs on a personal Mac mini; treat as staging-equivalent.
        _experiments={"enable_logs": True},
        # Performance: 100% sample peel transactions. Volume is one
        # transaction per ~minutes, well under any quota.
        traces_sample_rate=1.0,
        # Avoid swamping the queue from a long-running process.
        max_breadcrumbs=100,
    )
    sentry_sdk.set_tag("component", component)
    for k, v in _base_tags.items():
        sentry_sdk.set_tag(k, v)

    _initialized = True
    _stderr("tiler.sentry_initialized", component=component, env=_base_tags["env"])


def log_event(message: str, level: str = "info", **attrs: Any) -> None:
    """Emit a structured event to Sentry Logs (and stderr as backup).

    `message` should be a stable, queryable string like `"tiler.peel_done"`.
    `attrs` become structured tags on the event — visible as columns in
    the Sentry Logs explore view and filterable with Sentry's query syntax.
    """
    if not _initialized:
        init_sentry(_component)

    # Always stderr — keeps a local log when Sentry quota is exhausted
    # or DSN is missing.
    _stderr(message, level=level, **attrs)

    if not _SENTRY_AVAILABLE or not os.environ.get(DSN_ENV):
        return

    # capture_message is the Logs primitive. set_extras / set_tags expose
    # the structured attrs in the Sentry UI.
    with sentry_sdk.push_scope() as scope:
        scope.set_level(level)
        scope.set_tag("component", attrs.pop("component", _component))
        for k, v in attrs.items():
            # Tags must be strings/short scalars; extras can be anything.
            if isinstance(v, (str, int, float, bool)):
                scope.set_tag(k.replace(".", "_"), v)
            scope.set_extra(k, v)
        sentry_sdk.capture_message(message, level=level)


@contextlib.contextmanager
def peel_span(
    peel_idx: int,
    lng_lo: float,
    lng_hi: float,
    cycle: int,
) -> Iterator[dict[str, Any]]:
    """Context manager wrapping one peel in a Sentry transaction.

    Yields a mutable dict the caller can populate with counters
    (`tiles_written`, `bytes_uploaded`, etc.). On exit, the transaction
    is closed and a `tiler.peel_done` Info event is emitted with the
    accumulated counters + computed duration.

    Usage::

        with peel_span(peel_idx=18, lng_lo=0.0, lng_hi=10.0, cycle=0) as ctx:
            ctx['tiles_written'] = run_pass2(...)
            ctx['bytes_uploaded'] = run_upload(...)
    """
    started = time.time()
    counters: dict[str, Any] = {}

    txn_cm = None
    if _SENTRY_AVAILABLE and os.environ.get(DSN_ENV) and _initialized:
        txn_cm = sentry_sdk.start_transaction(
            op="overture.peel",
            name=f"peel_{peel_idx:02d}",
        )
        txn = txn_cm.__enter__()
        txn.set_tag("peel.idx", peel_idx)
        txn.set_tag("peel.lng_lo", lng_lo)
        txn.set_tag("peel.lng_hi", lng_hi)
        txn.set_tag("cycle", cycle)

    log_event(
        "tiler.peel_start",
        component="driver",
        cycle=cycle,
        **{"peel.idx": peel_idx, "peel.lng_lo": lng_lo, "peel.lng_hi": lng_hi},
    )

    try:
        yield counters
    except BaseException as exc:
        log_event(
            "tiler.peel_failed",
            level="error",
            component="driver",
            cycle=cycle,
            error=repr(exc),
            **{"peel.idx": peel_idx},
        )
        raise
    finally:
        duration_sec = round(time.time() - started, 2)
        log_event(
            "tiler.peel_done",
            component="driver",
            cycle=cycle,
            total_duration_sec=duration_sec,
            **{"peel.idx": peel_idx, "peel.lng_lo": lng_lo, "peel.lng_hi": lng_hi},
            **counters,
        )
        if txn_cm is not None:
            txn_cm.__exit__(None, None, None)


@contextlib.contextmanager
def phase_span(
    component: str,
    peel_idx: int | None = None,
    cycle: int | None = None,
) -> Iterator[dict[str, Any]]:
    """Wrap a sub-phase (pass1_per_peel / pass2 / pass3_local / upload / index).

    Emits a `<component>.done` Info event on exit with duration and any
    counters the caller stored in the yielded dict. Use inside :func:`peel_span`
    so the timing nests under the peel transaction in Sentry's Spans view.
    """
    started = time.time()
    counters: dict[str, Any] = {}

    span_cm = None
    if _SENTRY_AVAILABLE and os.environ.get(DSN_ENV) and _initialized:
        span_cm = sentry_sdk.start_span(op=f"overture.{component}", description=component)
        span_cm.__enter__()

    try:
        yield counters
    finally:
        duration_sec = round(time.time() - started, 2)
        attrs: dict[str, Any] = {
            "component": component,
            "duration_sec": duration_sec,
        }
        if peel_idx is not None:
            attrs["peel.idx"] = peel_idx
        if cycle is not None:
            attrs["cycle"] = cycle
        attrs.update(counters)
        log_event(f"tiler.{component}_done", **attrs)
        if span_cm is not None:
            span_cm.__exit__(None, None, None)
