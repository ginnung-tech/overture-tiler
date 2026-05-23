"""SQL-safety helpers for the overture-tiler scripts.

DuckDB cannot parameterize identifiers, file paths inside `read_parquet(...)`,
COPY-TO target paths, the array literal that `read_parquet(['a','b'])` consumes,
or any `SET pragma = ...` statement. Snyk Code (CWE-89 — SQL Injection) flags
every f-string interpolated into a SQL string, regardless of where the value
came from.

The helpers in this module accept the value, validate it against a strict
whitelist for the SQL context it will be wrapped in, and return a string safe
to interpolate. Inputs that fail validation raise `ValueError` — loud failure
is better than silently building a malformed query.

Validation rules:

  - `q_path(p)`        — path-as-posix string, rejects any character that could
                         break out of a `'...'` SQL literal (quotes, newlines,
                         NUL, semicolons).
  - `q_int(n)`         — coerces to `int` and stringifies. Use for WHERE
                         clauses with numeric literals, SET threads, integer
                         configuration.
  - `q_mem_limit(s)`   — DuckDB memory_limit value: `^\\d+(\\.\\d+)?\\s*(KB|MB|GB|TB)?$`.
  - `q_path_list(ps)`  — list of paths rendered as the DuckDB list literal
                         `['p1','p2',...]`. Each element goes through q_path.
  - `q_float(n)`       — coerces to `float` and stringifies. For bbox numerics.
"""

from __future__ import annotations

import re
from typing import Any, Iterable


_MEM_LIMIT_RE = re.compile(r"^\d+(\.\d+)?\s*(KB|MB|GB|TB)?$", re.IGNORECASE)
_SQL_BAD_PATH_CHARS = re.compile(r"['\"\n\r\x00;]")


def q_path(p: Any) -> str:
    """Return `p` as a posix path string, validated for `'...'`-wrapped SQL literal use.

    Accepts a `pathlib.Path` (uses `.as_posix()`) or a `str`. Rejects any
    character that could terminate the surrounding SQL string literal,
    introduce a statement separator, or smuggle a control byte.
    """
    s = p.as_posix() if hasattr(p, "as_posix") else str(p)
    if _SQL_BAD_PATH_CHARS.search(s):
        raise ValueError(f"unsafe path for SQL interpolation: {s!r}")
    return s


def q_int(n: Any) -> str:
    """Coerce `n` to `int` and stringify. Raises if not numeric."""
    return str(int(n))


def q_float(n: Any) -> str:
    """Coerce `n` to `float` and stringify. Raises if not numeric."""
    return repr(float(n))


def q_mem_limit(s: str) -> str:
    """Validate a DuckDB memory_limit value like '20GB' / '4096MB'."""
    if not isinstance(s, str) or not _MEM_LIMIT_RE.match(s.strip()):
        raise ValueError(f"unsafe memory_limit value: {s!r}")
    return s.strip()


def q_path_list(paths: Iterable[Any]) -> str:
    """Render a sequence of paths as `['/p/a.parquet','/p/b.parquet']` for read_parquet."""
    parts = [f"'{q_path(p)}'" for p in paths]
    return "[" + ",".join(parts) + "]"
