from __future__ import annotations

from core.common import utils
from datetime import datetime


def test_hash_string_is_stable_sha256_hex() -> None:
    out1 = utils.hash_string("abc")
    out2 = utils.hash_string("abc")

    assert out1 == out2
    assert len(out1) == 64
    int(out1, 16)  # should be valid hex
    assert out1 != utils.hash_string("abcd")


def test_get_current_timestamp_has_expected_format() -> None:
    ts = utils.get_current_timestamp()
    # basic shape: YYYY-MM-DD HH:MM:SS
    assert len(ts) == 19
    assert ts[4] == "-" and ts[7] == "-" and ts[10] == " " and ts[13] == ":" and ts[16] == ":"


def test_generate_schema_hash_is_order_invariant() -> None:
    schema_a = {"a": 1, "b": {"c": 2}}
    schema_b = {"b": {"c": 2}, "a": 1}

    assert utils.generate_schema_hash(schema_a) == utils.generate_schema_hash(schema_b)


def test_validate_config_checks_required_fields() -> None:
    assert utils.validate_config({"a": 1, "b": 2}, ["a"]) is True
    assert utils.validate_config({"a": 1, "b": 2}, ["a", "c"]) is False


def test_format_timestamp_defaults_to_utc_isoformat() -> None:
    ts = utils.format_timestamp()
    assert "T" in ts  # isoformat


def test_format_timestamp_uses_provided_datetime() -> None:
    dt = datetime(2020, 1, 2, 3, 4, 5)
    assert utils.format_timestamp(dt).startswith("2020-01-02T03:04:05")


def test_merge_metadata_deep_merges_dicts_without_mutating_inputs() -> None:
    existing = {"a": {"x": 1}, "b": 2}
    new = {"a": {"y": 3}, "b": 9, "c": 10}

    merged = utils.merge_metadata(existing, new)

    assert merged == {"a": {"x": 1, "y": 3}, "b": 9, "c": 10}
    assert existing == {"a": {"x": 1}, "b": 2}
    assert new == {"a": {"y": 3}, "b": 9, "c": 10}


def test_merge_metadata_overwrites_when_not_both_dicts() -> None:
    existing = {"a": {"x": 1}}
    new = {"a": 3}
    merged = utils.merge_metadata(existing, new)
    assert merged["a"] == 3


def test_sanitize_entity_id_normalizes_case_and_separators() -> None:
    assert utils.sanitize_entity_id("My-Entity Id") == "my_entity_id"


def test_get_platform_config_missing_returns_empty_dict(tmp_path) -> None:
    # Should not raise; should return {}
    out = utils.get_platform_config("datahub", str(tmp_path))
    assert out == {}


def test_get_platform_config_reads_yaml(tmp_path) -> None:
    (tmp_path / "datahub.yaml").write_text("a: 1\nb:\n  c: 2\n", encoding="utf-8")
    out = utils.get_platform_config("datahub", str(tmp_path))
    assert out == {"a": 1, "b": {"c": 2}}


def test_get_platform_config_invalid_yaml_returns_empty_dict(tmp_path) -> None:
    (tmp_path / "datahub.yaml").write_text("a: [", encoding="utf-8")
    out = utils.get_platform_config("datahub", str(tmp_path))
    assert out == {}


def test_load_json_file_missing_returns_empty_list(tmp_path) -> None:
    missing = tmp_path / "missing.json"
    data = utils.load_json_file(str(missing), entity_type="users")
    assert data == []


def test_load_json_file_invalid_json_returns_none(tmp_path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text("{not valid json", encoding="utf-8")
    data = utils.load_json_file(str(bad), entity_type="users")
    assert data is None


def test_load_json_file_valid_json_returns_data(tmp_path) -> None:
    good = tmp_path / "good.json"
    good.write_text('[{"a": 1}, {"b": 2}]', encoding="utf-8")
    data = utils.load_json_file(str(good), entity_type="users")
    assert data == [{"a": 1}, {"b": 2}]


