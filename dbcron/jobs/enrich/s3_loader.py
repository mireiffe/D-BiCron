"""외부 스토리지 객체 로더 (S3 호환).

S3 호환 스토리지에서 객체를 가져와 base64 인코딩 후
RowContext의 collections에 그룹별로 저장합니다.
"""

from __future__ import annotations

import base64
import logging
from typing import TYPE_CHECKING

from .util import _expand_env, _resolve_url

if TYPE_CHECKING:
    from .context import RowContext

logger = logging.getLogger(__name__)

# ── 최대 기본값 ─────────────────────────────────────────────────

_DEFAULT_MAX_OBJECT_SIZE = 10 * 1024 * 1024  # 10 MB
_DEFAULT_MAX_OBJECTS_PER_GROUP = 50


class RowSkipError(Exception):
    """on_failure == 'skip_row'이고 전체 로드 실패 시 raise."""


def _create_s3_client(s3_config: dict):
    """boto3 S3 클라이언트를 생성한다."""
    import boto3

    kwargs: dict = {}
    endpoint_url = s3_config.get("endpoint_url")
    if endpoint_url:
        kwargs["endpoint_url"] = _expand_env(endpoint_url)

    access_key = s3_config.get("access_key")
    secret_key = s3_config.get("secret_key")
    if access_key:
        kwargs["aws_access_key_id"] = _expand_env(access_key)
    if secret_key:
        kwargs["aws_secret_access_key"] = _expand_env(secret_key)

    region = s3_config.get("region")
    if region:
        kwargs["region_name"] = region

    return boto3.client("s3", **kwargs)


def load_s3_objects(
    context: RowContext,
    s3_config: dict,
) -> dict[str, list[str]]:
    """S3에서 객체를 로드하여 그룹별 base64 문자열 리스트를 반환한다.

    Args:
        context: 현재 행 컨텍스트 (prefix_template 치환용)
        s3_config: 테이블 레벨 s3_objects 설정

    Returns:
        {group_name: [base64_encoded_string, ...]}

    Raises:
        RowSkipError: on_failure='skip_row'이고 모든 객체 로드 실패 시
    """
    bucket = _expand_env(s3_config["bucket"])
    prefix_template = s3_config.get("prefix_template", "")
    group_by = s3_config.get("group_by_substring", {})
    on_failure = s3_config.get("on_failure", "skip_object")
    max_size = s3_config.get("max_object_size_bytes", _DEFAULT_MAX_OBJECT_SIZE)
    max_per_group = s3_config.get("max_objects_per_group", _DEFAULT_MAX_OBJECTS_PER_GROUP)

    # prefix 치환
    prefix = _resolve_url(prefix_template, context.flat())

    # S3 클라이언트 생성
    client = _create_s3_client(s3_config)

    # 객체 목록 조회
    try:
        response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        objects = response.get("Contents", [])
    except Exception:
        logger.exception("S3 list_objects_v2 failed: bucket=%s prefix=%s", bucket, prefix)
        if on_failure == "skip_row":
            raise RowSkipError(f"S3 list failed for {bucket}/{prefix}")
        return {name: [] for name in group_by}

    # 그룹 초기화
    collections: dict[str, list[str]] = {name: [] for name in group_by}
    total_loaded = 0
    total_failed = 0

    for obj in objects:
        key = obj["Key"]
        size = obj.get("Size", 0)

        # 크기 제한
        if size > max_size:
            logger.warning("S3 object too large, skipping: %s (%d bytes)", key, size)
            total_failed += 1
            continue

        # 그룹 분류
        matched_group = None
        filename = key.rsplit("/", 1)[-1] if "/" in key else key
        for group_name, substring in group_by.items():
            if substring in filename:
                matched_group = group_name
                break

        if matched_group is None:
            continue  # 어느 그룹에도 매칭되지 않으면 무시

        # 그룹 최대 개수 제한
        if len(collections[matched_group]) >= max_per_group:
            continue

        # 객체 다운로드 + base64 인코딩
        try:
            resp = client.get_object(Bucket=bucket, Key=key)
            body = resp["Body"].read()
            encoded = base64.b64encode(body).decode("ascii")
            collections[matched_group].append(encoded)
            total_loaded += 1
        except Exception:
            logger.warning("S3 get_object failed, skipping: %s", key, exc_info=True)
            total_failed += 1

    logger.debug(
        "S3 loader: %d loaded, %d failed from %s/%s",
        total_loaded, total_failed, bucket, prefix,
    )

    # 전체 실패 판단
    if total_loaded == 0 and total_failed > 0 and on_failure == "skip_row":
        raise RowSkipError(
            f"All S3 objects failed to load from {bucket}/{prefix}"
        )

    return collections
