"""Tests for dbcron/jobs/enrich/request_builder.py."""

from __future__ import annotations

from dbcron.jobs.enrich.context import RowContext
from dbcron.jobs.enrich.request_builder import build_request_body, build_request_url


def _ctx(**kwargs):
    return RowContext(
        source=kwargs.get("source", {}),
        queries=kwargs.get("queries", {}),
        collections=kwargs.get("collections", {}),
    )


class TestBuildRequestBody:
    def test_simple_string_substitution(self):
        ctx = _ctx(source={"name": "Alice"})
        result = build_request_body("Hello {name}", ctx)
        assert result == "Hello Alice"

    def test_nested_dict(self):
        ctx = _ctx(source={"id": 42, "name": "Bob"})
        template = {
            "user": {"id": "{id}", "display": "User: {name}"},
        }
        result = build_request_body(template, ctx)
        assert result["user"]["id"] == 42  # raw type preserved
        assert result["user"]["display"] == "User: Bob"

    def test_array_substitution(self):
        ctx = _ctx(source={"a": "X", "b": "Y"})
        template = ["{a}", "{b}", "static"]
        result = build_request_body(template, ctx)
        assert result == ["X", "Y", "static"]

    def test_raw_type_preserved(self):
        ctx = _ctx(source={"id": 42, "score": 3.14, "active": True})
        assert build_request_body("{id}", ctx) == 42
        assert build_request_body("{score}", ctx) == 3.14
        assert build_request_body("{active}", ctx) is True

    def test_collections_ref(self):
        ctx = _ctx(collections={"images": ["b64a", "b64b"]})
        result = build_request_body("$collections.images", ctx)
        assert result == ["b64a", "b64b"]

    def test_none_preserved(self):
        assert build_request_body(None, _ctx()) is None

    def test_missing_key_returns_none(self):
        ctx = _ctx(source={"id": 1})
        result = build_request_body("{missing}", ctx)
        assert result is None

    def test_mixed_string_interpolation(self):
        ctx = _ctx(source={"prefix": "pre", "suffix": "suf"})
        result = build_request_body("_{prefix}_{suffix}_", ctx)
        assert result == "_pre_suf_"

    def test_missing_in_mixed_becomes_empty(self):
        ctx = _ctx(source={"a": "X"})
        result = build_request_body("{a}_{missing}", ctx)
        assert result == "X_"

    def test_template_none_returns_none(self):
        result = build_request_body(None, _ctx())
        assert result is None

    def test_static_values_unchanged(self):
        ctx = _ctx()
        template = {"key": "static", "num": 123, "flag": False}
        result = build_request_body(template, ctx)
        assert result == {"key": "static", "num": 123, "flag": False}

    def test_complex_nested_template(self):
        ctx = _ctx(
            source={"id": 1, "desc": "test item"},
            collections={"photos": ["img1", "img2"]},
        )
        template = {
            "model": "v2",
            "inputs": {
                "text": "{desc}",
                "images": "$collections.photos",
                "meta": {"source_id": "{id}"},
            },
        }
        result = build_request_body(template, ctx)
        assert result["model"] == "v2"
        assert result["inputs"]["text"] == "test item"
        assert result["inputs"]["images"] == ["img1", "img2"]
        assert result["inputs"]["meta"]["source_id"] == 1


class TestBuildRequestUrl:
    def test_basic(self):
        ctx = _ctx(source={"id": "123"})
        url = build_request_url("https://api.com/{id}", ctx)
        assert url == "https://api.com/123"

    def test_url_encoding(self):
        ctx = _ctx(source={"q": "hello world"})
        url = build_request_url("https://api.com/{q}", ctx)
        assert url == "https://api.com/hello%20world"

    def test_uses_query_results(self):
        ctx = _ctx(
            source={"id": 1},
            queries={"q1": {"code": "ABC"}},
        )
        url = build_request_url("https://api.com/{code}", ctx)
        assert url == "https://api.com/ABC"
