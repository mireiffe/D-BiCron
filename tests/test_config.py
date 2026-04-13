"""Tests for dbcron.config module.

Covers the _env() helper and load_config() factory function.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from dbcron.config import AppConfig, S3Config, _env, load_config


# ── _env helper ─────────────────────────────────────────────────


class TestEnvHelper:
    def test_env_returns_value_when_set(self):
        """_env returns the environment variable value when it is set."""
        with patch.dict(os.environ, {"TEST_DBCRON_VAR": "hello"}):
            assert _env("TEST_DBCRON_VAR") == "hello"

    def test_env_returns_default_when_missing(self):
        """_env returns the default value when the env var is not set."""
        with patch.dict(os.environ, {}, clear=True):
            assert _env("NONEXISTENT_VAR_12345", "fallback") == "fallback"

    def test_env_raises_when_missing_no_default(self):
        """_env raises EnvironmentError when the env var is missing and no default."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(EnvironmentError, match="Required environment variable"):
                _env("TOTALLY_MISSING_VAR_99999")


# ── load_config ─────────────────────────────────────────────────


class TestLoadConfig:
    def test_load_config_with_all_vars(self):
        """load_config returns a valid AppConfig when all S3 vars are set."""
        env_vars = {
            "S3_ENDPOINT_URL": "https://s3.example.com",
            "S3_ACCESS_KEY": "AKIATEST",
            "S3_SECRET_KEY": "secret123",
            "S3_BUCKET_NAME": "my-bucket",
        }
        with patch.dict(os.environ, env_vars):
            config = load_config()

        assert isinstance(config, AppConfig)
        assert isinstance(config.s3, S3Config)
        assert config.s3.endpoint_url == "https://s3.example.com"
        assert config.s3.access_key == "AKIATEST"
        assert config.s3.secret_key == "secret123"
        assert config.s3.bucket_name == "my-bucket"
