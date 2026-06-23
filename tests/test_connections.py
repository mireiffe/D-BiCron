"""Tests for pg2ch.connections (loading / env interpolation / b64)."""

from __future__ import annotations

import base64
import json

import pytest

from pg2ch import connections as c


def _write(tmp_path, data):
    p = tmp_path / "connections.json"
    p.write_text(json.dumps(data), encoding="utf-8")
    return str(p)


class TestLoad:
    def test_load_and_get(self, tmp_path):
        path = _write(tmp_path, {"pg": {"type": "postgresql", "host": "h", "dbname": "d"}})
        conns = c.load_connections(path)
        assert conns["pg"]["host"] == "h"
        assert c.get_connection("pg", path)["dbname"] == "d"

    def test_missing_file(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            c.load_connections(str(tmp_path / "nope.json"))

    def test_not_object(self, tmp_path):
        p = tmp_path / "connections.json"
        p.write_text("[]", encoding="utf-8")
        with pytest.raises(ValueError, match="keyed by connection id"):
            c.load_connections(str(p))

    def test_get_missing_id(self, tmp_path):
        path = _write(tmp_path, {"pg": {"host": "h"}})
        with pytest.raises(KeyError, match="not found"):
            c.get_connection("nope", path)

    def test_comment_and_non_dict_entries_skipped(self, tmp_path):
        path = _write(
            tmp_path,
            {"_comment": "a note", "pg": {"type": "postgresql", "host": "h"}},
        )
        conns = c.load_connections(path)
        assert list(conns) == ["pg"]


class TestEnvInterpolation:
    def test_simple_var(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PGPW", "s3cret")
        path = _write(tmp_path, {"pg": {"password": "${PGPW}", "host": "h"}})
        assert c.get_connection("pg", path)["password"] == "s3cret"

    def test_default_when_unset(self, tmp_path, monkeypatch):
        monkeypatch.delenv("NOPE", raising=False)
        path = _write(tmp_path, {"pg": {"password": "${NOPE:-fallback}", "host": "h"}})
        assert c.get_connection("pg", path)["password"] == "fallback"

    def test_empty_default(self, tmp_path, monkeypatch):
        monkeypatch.delenv("NOPE", raising=False)
        path = _write(tmp_path, {"ch": {"password": "${NOPE:-}", "host": "h"}})
        assert c.get_connection("ch", path)["password"] == ""

    def test_undefined_no_default_raises(self, tmp_path, monkeypatch):
        monkeypatch.delenv("NOPE", raising=False)
        path = _write(tmp_path, {"pg": {"password": "${NOPE}", "host": "h"}})
        with pytest.raises(KeyError, match="undefined env var"):
            c.load_connections(path)


class TestB64:
    def test_b64_password_decoded(self, tmp_path):
        enc = base64.b64encode(b"plain").decode()
        path = _write(tmp_path, {"pg": {"_enc": "b64", "password": enc, "host": "h"}})
        assert c.get_connection("pg", path)["password"] == "plain"


class TestPathResolution:
    def test_env_overrides_default(self, tmp_path, monkeypatch):
        p = tmp_path / "conns.json"
        monkeypatch.setenv("PG2CH_CONNECTIONS", str(p))
        assert c.connections_path() == p

    def test_arg_wins(self, monkeypatch):
        monkeypatch.setenv("PG2CH_CONNECTIONS", "/env/path.json")
        assert str(c.connections_path("/arg/path.json")) == "/arg/path.json"
