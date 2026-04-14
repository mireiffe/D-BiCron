"""Tests for dbcron/jobs/enrich/s3_loader.py."""

from __future__ import annotations

import base64
from unittest.mock import MagicMock, patch

import pytest

from dbcron.jobs.enrich.context import RowContext
from dbcron.jobs.enrich.s3_loader import RowSkipError, load_s3_objects


def _make_context(**source_kw):
    return RowContext(source=source_kw)


def _mock_s3_client(objects=None, get_bodies=None, list_error=False, get_errors=None):
    """Mock boto3 S3 client.

    Args:
        objects: list of {"Key": str, "Size": int} for list_objects_v2
        get_bodies: dict of key → bytes for get_object
        list_error: if True, list_objects_v2 raises
        get_errors: set of keys that should fail on get_object
    """
    client = MagicMock()

    if list_error:
        client.list_objects_v2.side_effect = Exception("list failed")
    else:
        client.list_objects_v2.return_value = {
            "Contents": objects or [],
        }

    get_errors = get_errors or set()
    get_bodies = get_bodies or {}

    def mock_get_object(Bucket, Key):
        if Key in get_errors:
            raise Exception(f"get failed: {Key}")
        body = MagicMock()
        body.read.return_value = get_bodies.get(Key, b"data")
        return {"Body": body}

    client.get_object.side_effect = mock_get_object
    return client


class TestLoadS3Objects:
    def _run(self, context, s3_config, mock_client):
        with patch(
            "dbcron.jobs.enrich.s3_loader._create_s3_client",
            return_value=mock_client,
        ):
            return load_s3_objects(context, s3_config)

    def test_basic_load_base64(self):
        ctx = _make_context(sku="ABC")
        client = _mock_s3_client(
            objects=[{"Key": "products/ABC/photo_1.jpg", "Size": 100}],
            get_bodies={"products/ABC/photo_1.jpg": b"imagedata"},
        )
        cfg = {
            "bucket": "mybucket",
            "prefix_template": "products/{sku}/",
            "group_by_substring": {"photos": "photo_"},
        }
        result = self._run(ctx, cfg, client)
        assert len(result["photos"]) == 1
        assert result["photos"][0] == base64.b64encode(b"imagedata").decode()

    def test_prefix_template_substitution(self):
        ctx = _make_context(sku="XYZ")
        client = _mock_s3_client(objects=[])
        cfg = {
            "bucket": "b",
            "prefix_template": "data/{sku}/files/",
            "group_by_substring": {"docs": "doc_"},
        }
        self._run(ctx, cfg, client)
        call_args = client.list_objects_v2.call_args
        assert call_args[1]["Prefix"] == "data/XYZ/files/"

    def test_group_by_substring(self):
        ctx = _make_context(sku="A")
        client = _mock_s3_client(
            objects=[
                {"Key": "photo_1.jpg", "Size": 10},
                {"Key": "diagram_1.png", "Size": 10},
                {"Key": "photo_2.jpg", "Size": 10},
                {"Key": "other.txt", "Size": 10},
            ],
            get_bodies={
                "photo_1.jpg": b"p1",
                "diagram_1.png": b"d1",
                "photo_2.jpg": b"p2",
            },
        )
        cfg = {
            "bucket": "b",
            "prefix_template": "",
            "group_by_substring": {"photos": "photo_", "diagrams": "diagram_"},
        }
        result = self._run(ctx, cfg, client)
        assert len(result["photos"]) == 2
        assert len(result["diagrams"]) == 1

    def test_individual_failure_skip_object(self):
        ctx = _make_context()
        client = _mock_s3_client(
            objects=[
                {"Key": "photo_ok.jpg", "Size": 10},
                {"Key": "photo_bad.jpg", "Size": 10},
            ],
            get_bodies={"photo_ok.jpg": b"ok"},
            get_errors={"photo_bad.jpg"},
        )
        cfg = {
            "bucket": "b",
            "prefix_template": "",
            "group_by_substring": {"photos": "photo_"},
            "on_failure": "skip_object",
        }
        result = self._run(ctx, cfg, client)
        assert len(result["photos"]) == 1

    def test_all_failed_skip_row(self):
        ctx = _make_context()
        client = _mock_s3_client(
            objects=[{"Key": "photo_bad.jpg", "Size": 10}],
            get_errors={"photo_bad.jpg"},
        )
        cfg = {
            "bucket": "b",
            "prefix_template": "",
            "group_by_substring": {"photos": "photo_"},
            "on_failure": "skip_row",
        }
        with pytest.raises(RowSkipError):
            self._run(ctx, cfg, client)

    def test_all_failed_skip_object_returns_empty(self):
        ctx = _make_context()
        client = _mock_s3_client(
            objects=[{"Key": "photo_bad.jpg", "Size": 10}],
            get_errors={"photo_bad.jpg"},
        )
        cfg = {
            "bucket": "b",
            "prefix_template": "",
            "group_by_substring": {"photos": "photo_"},
            "on_failure": "skip_object",
        }
        result = self._run(ctx, cfg, client)
        assert result["photos"] == []

    def test_max_object_size_skip(self):
        ctx = _make_context()
        client = _mock_s3_client(
            objects=[{"Key": "photo_huge.jpg", "Size": 20_000_000}],
        )
        cfg = {
            "bucket": "b",
            "prefix_template": "",
            "group_by_substring": {"photos": "photo_"},
            "max_object_size_bytes": 10_000_000,
        }
        result = self._run(ctx, cfg, client)
        assert result["photos"] == []

    def test_no_s3_config_returns_empty(self):
        ctx = _make_context()
        client = _mock_s3_client(objects=[])
        cfg = {
            "bucket": "b",
            "prefix_template": "",
            "group_by_substring": {},
        }
        result = self._run(ctx, cfg, client)
        assert result == {}

    def test_list_error_skip_row(self):
        ctx = _make_context()
        client = _mock_s3_client(list_error=True)
        cfg = {
            "bucket": "b",
            "prefix_template": "",
            "group_by_substring": {"photos": "photo_"},
            "on_failure": "skip_row",
        }
        with pytest.raises(RowSkipError):
            self._run(ctx, cfg, client)

    def test_list_error_skip_object_returns_empty(self):
        ctx = _make_context()
        client = _mock_s3_client(list_error=True)
        cfg = {
            "bucket": "b",
            "prefix_template": "",
            "group_by_substring": {"photos": "photo_"},
            "on_failure": "skip_object",
        }
        result = self._run(ctx, cfg, client)
        assert result["photos"] == []
