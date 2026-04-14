"""Tests for dbcron/jobs/enrich/response_parser.py."""

from __future__ import annotations

from dbcron.jobs.enrich.response_parser import (
    extract_response_mapping,
    merge_response_arrays,
)


class TestExtractResponseMapping:
    def test_basic(self):
        data = {"status": "ok", "payment": {"amount": 42.5}}
        mapping = [
            {"json_path": "status", "column": "st", "type": "String"},
            {"json_path": "payment.amount", "column": "amt", "type": "Float64"},
        ]
        result = extract_response_mapping(data, mapping)
        assert result == {"st": "ok", "amt": 42.5}

    def test_missing_path_returns_none(self):
        data = {"a": 1}
        mapping = [{"json_path": "missing", "column": "col", "type": "String"}]
        result = extract_response_mapping(data, mapping)
        assert result == {"col": None}

    def test_empty_mapping(self):
        result = extract_response_mapping({"a": 1}, [])
        assert result == {}


class TestMergeResponseArrays:
    def test_two_arrays_basic(self):
        response = {
            "predictions": [{"label": "A"}, {"label": "B"}],
            "scores": [0.9, 0.7],
        }
        config = [
            {"json_path": "predictions", "prefix": "pred"},
            {"json_path": "scores", "prefix": "conf"},
        ]
        result = merge_response_arrays(response, config)
        assert len(result) == 2
        assert result[0] == {"pred": {"label": "A"}, "conf": 0.9}
        assert result[1] == {"pred": {"label": "B"}, "conf": 0.7}

    def test_length_mismatch_uses_shorter(self):
        response = {
            "a": [1, 2, 3],
            "b": [10, 20],
        }
        config = [
            {"json_path": "a", "prefix": "x"},
            {"json_path": "b", "prefix": "y"},
        ]
        result = merge_response_arrays(response, config)
        assert len(result) == 2

    def test_empty_array_returns_empty(self):
        response = {"a": [], "b": [1, 2]}
        config = [
            {"json_path": "a", "prefix": "x"},
            {"json_path": "b", "prefix": "y"},
        ]
        result = merge_response_arrays(response, config)
        assert result == []

    def test_not_array_returns_empty(self):
        response = {"a": "not_an_array"}
        config = [{"json_path": "a", "prefix": "x"}]
        result = merge_response_arrays(response, config)
        assert result == []

    def test_scalar_elements(self):
        response = {"vals": [1, 2, 3], "names": ["a", "b", "c"]}
        config = [
            {"json_path": "vals", "prefix": "v"},
            {"json_path": "names", "prefix": "n"},
        ]
        result = merge_response_arrays(response, config)
        assert result[0] == {"v": 1, "n": "a"}
        assert result[2] == {"v": 3, "n": "c"}

    def test_three_arrays(self):
        response = {"a": [1, 2], "b": [3, 4], "c": [5, 6]}
        config = [
            {"json_path": "a", "prefix": "x"},
            {"json_path": "b", "prefix": "y"},
            {"json_path": "c", "prefix": "z"},
        ]
        result = merge_response_arrays(response, config)
        assert len(result) == 2
        assert result[0] == {"x": 1, "y": 3, "z": 5}

    def test_with_extract_mapping(self):
        """response_merge + extract_response_mapping 통합."""
        response = {
            "predictions": [{"label": "cat"}, {"label": "dog"}],
            "confidence": [0.95, 0.80],
        }
        merge_cfg = [
            {"json_path": "predictions", "prefix": "pred"},
            {"json_path": "confidence", "prefix": "conf"},
        ]
        merged = merge_response_arrays(response, merge_cfg)
        # 각 병합 요소에 extract_response_mapping 적용
        mapping = [
            {"json_path": "pred.label", "column": "prediction", "type": "String"},
            {"json_path": "conf", "column": "score", "type": "Float64"},
        ]
        rows = [extract_response_mapping(elem, mapping) for elem in merged]
        assert rows[0] == {"prediction": "cat", "score": 0.95}
        assert rows[1] == {"prediction": "dog", "score": 0.80}
