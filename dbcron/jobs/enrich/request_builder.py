"""중첩 요청 바디 템플릿 엔진.

쿼리 결과 + S3 로드 객체를 조합해 임의의 중첩 JSON 구조를 생성합니다.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

if TYPE_CHECKING:
    from .context import RowContext

# ── 플레이스홀더 패턴 ────────────────────────────────────────────

# "{column_name}" — 전체 문자열이 단일 플레이스홀더인지 판별
_SINGLE_PH = re.compile(r"^\{(\w+)\}$")
# "{column_name}" — 혼합 문자열 내 플레이스홀더
_MULTI_PH = re.compile(r"\{(\w+)\}")
# "$collections.name" — 컬렉션 참조
_COLL_REF = re.compile(r"^\$collections\.(\w+)$")


def build_request_body(
    template: Any,
    context: RowContext,
) -> Any:
    """템플릿을 재귀적으로 치환하여 요청 바디를 생성한다.

    치환 규칙:
      - "{column}" (전체 문자열이 단일 플레이스홀더) → 원시 타입 유지
      - "prefix_{column}_suffix" (혼합) → 문자열 보간
      - "$collections.name" → context.collections[name] 리스트
      - None, 숫자, bool → 그대로 유지
      - dict/list → 재귀 처리

    Args:
        template: JSON 템플릿 구조 (dict, list, str, 또는 스칼라)
        context: 현재 행 컨텍스트

    Returns:
        치환된 값. template이 None이면 None 반환.
    """
    if template is None:
        return None

    if isinstance(template, dict):
        return {k: build_request_body(v, context) for k, v in template.items()}

    if isinstance(template, list):
        return [build_request_body(item, context) for item in template]

    if isinstance(template, str):
        return _substitute_string(template, context)

    # int, float, bool 등 스칼라
    return template


def _substitute_string(s: str, context: RowContext) -> Any:
    """문자열 템플릿을 치환한다."""
    # $collections.name → 리스트 직접 반환
    m = _COLL_REF.match(s)
    if m:
        return context.collections.get(m.group(1), [])

    flat = context.flat()

    # 단일 플레이스홀더 → 원시 타입 유지
    m = _SINGLE_PH.match(s)
    if m:
        key = m.group(1)
        return flat.get(key)

    # 혼합 문자열 → 문자열 보간
    def _repl(match: re.Match) -> str:
        key = match.group(1)
        val = flat.get(key)
        if val is None:
            return ""
        return str(val)

    return _MULTI_PH.sub(_repl, s)


def build_request_url(url_template: str, context: RowContext) -> str:
    """URL 템플릿을 RowContext 기반으로 치환한다.

    기존 _resolve_url과 동일하지만 RowContext.flat()을 사용.
    """
    flat = context.flat()

    def _replace(m: re.Match) -> str:
        key = m.group(1)
        val = flat.get(key, "")
        return quote(str(val), safe="")

    return re.sub(r"\{(\w+)\}", _replace, url_template)
