"""dags/pg2ch_factory.py 의 copy 동시성 env 파싱 테스트.

airflow 미설치 환경(엔진 테스트 기본)에서도 돌도록, factory 모듈을 파일 경로로
로드해 순수 함수 ``_resolve_copy_concurrency`` 만 검증한다. factory 의 airflow
import 는 lazy 하므로 모듈 로드 자체는 airflow 없이 성공한다.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_FACTORY_PATH = Path(__file__).resolve().parents[1] / "dags" / "pg2ch_factory.py"


def _load_factory():
    spec = importlib.util.spec_from_file_location(
        "pg2ch_factory_under_test", _FACTORY_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


factory = _load_factory()


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch):
    monkeypatch.delenv("PG2CH_COPY_CONCURRENCY", raising=False)


def test_default_when_unset():
    assert factory._resolve_copy_concurrency() == factory._DEFAULT_COPY_CONCURRENCY


@pytest.mark.parametrize(
    "value,expected",
    [("4", 4), ("16", 16), ("  10 ", 10), ("1", 1)],
)
def test_positive_int(monkeypatch, value, expected):
    monkeypatch.setenv("PG2CH_COPY_CONCURRENCY", value)
    assert factory._resolve_copy_concurrency() == expected


@pytest.mark.parametrize(
    "value", ["0", "unlimited", "none", "off", "UNLIMITED", "-3", "-1"]
)
def test_unlimited_returns_none(monkeypatch, value):
    monkeypatch.setenv("PG2CH_COPY_CONCURRENCY", value)
    assert factory._resolve_copy_concurrency() is None


@pytest.mark.parametrize("value", ["", "   "])
def test_blank_falls_back_to_default(monkeypatch, value):
    monkeypatch.setenv("PG2CH_COPY_CONCURRENCY", value)
    assert factory._resolve_copy_concurrency() == factory._DEFAULT_COPY_CONCURRENCY


@pytest.mark.parametrize("value", ["abc", "4.5", "ten"])
def test_invalid_falls_back_to_default(monkeypatch, value):
    monkeypatch.setenv("PG2CH_COPY_CONCURRENCY", value)
    assert factory._resolve_copy_concurrency() == factory._DEFAULT_COPY_CONCURRENCY
