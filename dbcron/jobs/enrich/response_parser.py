"""응답 파싱 및 멀티배열 병합.

API 응답에서 값을 추출하고, 여러 배열을 인덱스 기준으로
병합하여 1:N 출력 행을 생성합니다.
"""

from __future__ import annotations

import logging
from typing import Any

from .util import _extract_json_path

logger = logging.getLogger(__name__)


def extract_response_mapping(
    data: dict,
    response_mapping: list[dict],
) -> dict[str, Any]:
    """response_mapping 설정에 따라 응답에서 값을 추출한다.

    Args:
        data: API JSON 응답 (또는 병합된 요소)
        response_mapping: [{"json_path": ..., "column": ..., "type": ...}, ...]

    Returns:
        {column_name: extracted_value}
    """
    result = {}
    for mapping in response_mapping:
        result[mapping["column"]] = _extract_json_path(data, mapping["json_path"])
    return result


def merge_response_arrays(
    response: dict,
    merge_config: list[dict],
) -> list[dict]:
    """여러 배열을 인덱스 기준으로 병합한다.

    Args:
        response: API JSON 응답
        merge_config: [{"json_path": ..., "prefix": ...}, ...]

    Returns:
        병합된 요소 리스트. 각 요소는 {prefix: value_or_dict} 형태.
        빈 배열이면 빈 리스트 반환.
    """
    arrays: dict[str, list] = {}
    min_len = None

    for cfg in merge_config:
        path = cfg["json_path"]
        prefix = cfg["prefix"]
        arr = _extract_json_path(response, path)

        if not isinstance(arr, list):
            logger.warning(
                "response_merge: '%s' is not an array, skipping", path
            )
            return []

        arrays[prefix] = arr
        arr_len = len(arr)
        if min_len is None or arr_len < min_len:
            min_len = arr_len

    if min_len is None or min_len == 0:
        return []

    # 길이 불일치 경고
    lengths = {p: len(a) for p, a in arrays.items()}
    if len(set(lengths.values())) > 1:
        logger.warning(
            "response_merge: array length mismatch %s, using min=%d",
            lengths, min_len,
        )

    # 인덱스 기준 병합
    merged: list[dict] = []
    for i in range(min_len):
        element = {}
        for prefix, arr in arrays.items():
            element[prefix] = arr[i]
        merged.append(element)

    return merged
