"""공용 유틸리티 함수 및 상수."""

from __future__ import annotations

import os
import re
from urllib.parse import quote


def _extract_json_path(data: dict, path: str):
    """Dot-notation path 로 중첩 dict/list 에서 값을 추출한다.

    Examples:
        _extract_json_path({"a": {"b": 1}}, "a.b") → 1
        _extract_json_path({"items": [{"x": 1}]}, "items[0].x") → 1
    """
    current = data
    for part in re.split(r"\.", path):
        m = re.match(r"^(\w+)\[(\d+)\]$", part)
        if m:
            key, idx = m.group(1), int(m.group(2))
            if not isinstance(current, dict) or key not in current:
                return None
            current = current[key]
            if not isinstance(current, list) or idx >= len(current):
                return None
            current = current[idx]
        else:
            if not isinstance(current, dict) or part not in current:
                return None
            current = current[part]
    return current


def _expand_env(value: str) -> str:
    """``${VAR}`` 패턴을 환경변수 값으로 치환한다."""
    return re.sub(
        r"\$\{(\w+)\}",
        lambda m: os.environ.get(m.group(1), m.group(0)),
        value,
    )


def _resolve_url(template: str, row: dict) -> str:
    """``{column_name}`` 플레이스홀더를 row 값으로 치환한다."""

    def _replace(m: re.Match) -> str:
        key = m.group(1)
        val = row.get(key, "")
        return quote(str(val), safe="")

    return re.sub(r"\{(\w+)\}", _replace, template)


# ── 타임스탬프 타입 ───────────────────────────────────────────────

_TIMESTAMP_TYPES: dict[str, str] = {
    "clickhouse": "DateTime64(3)",
    "mssql": "DATETIME2",
    "postgresql": "TIMESTAMP",
    "sqlite": "TIMESTAMP",
}
