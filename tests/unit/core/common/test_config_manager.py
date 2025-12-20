from __future__ import annotations

from core.common.config_manager import ConfigManager


def test_load_config_missing_file_returns_empty_dict(tmp_path) -> None:
    cm = ConfigManager(base_config_dir=str(tmp_path))
    missing = tmp_path / "nope.yaml"
    assert cm.load_config(str(missing)) == {}


def test_load_config_non_dict_yaml_returns_empty_dict(tmp_path) -> None:
    cm = ConfigManager(base_config_dir=str(tmp_path))
    cfg = tmp_path / "cfg.yaml"
    cfg.write_text("- a\n- b\n", encoding="utf-8")
    assert cm.load_config(str(cfg)) == {}


def test_load_config_valid_yaml_is_cached_by_resolved_path(tmp_path) -> None:
    cm = ConfigManager(base_config_dir=str(tmp_path))
    cfg = tmp_path / "cfg.yaml"
    cfg.write_text("a: 1\nb: 2\n", encoding="utf-8")

    first = cm.load_config(str(cfg))
    second = cm.load_config(str(cfg))

    assert first is second
    assert first == {"a": 1, "b": 2}


def test_get_global_config_reads_from_base_config_dir(tmp_path) -> None:
    (tmp_path / "global_settings.yaml").write_text("datahub:\n  gms_server: http://x\n", encoding="utf-8")
    cm = ConfigManager(base_config_dir=str(tmp_path))
    cfg = cm.get_global_config()
    assert cfg["datahub"]["gms_server"] == "http://x"


def test_load_config_yaml_parse_error_returns_empty_dict(tmp_path) -> None:
    cm = ConfigManager(base_config_dir=str(tmp_path))
    cfg = tmp_path / "bad.yaml"
    cfg.write_text("a: [", encoding="utf-8")  # invalid YAML
    assert cm.load_config(str(cfg)) == {}


def test_load_config_directory_path_returns_empty_dict(tmp_path) -> None:
    cm = ConfigManager(base_config_dir=str(tmp_path))
    d = tmp_path / "dir"
    d.mkdir()
    assert cm.load_config(str(d)) == {}


def test_load_config_cache_prevents_reload_until_cleared(tmp_path) -> None:
    cm = ConfigManager(base_config_dir=str(tmp_path))
    cfg = tmp_path / "cfg.yaml"
    cfg.write_text("a: 1\n", encoding="utf-8")

    first = cm.load_config(str(cfg))
    assert first == {"a": 1}

    # Modify file; load_config should still return cached value.
    cfg.write_text("a: 2\n", encoding="utf-8")
    second = cm.load_config(str(cfg))

    assert second is first
    assert second == {"a": 1}


def test_get_global_config_missing_returns_empty_dict(tmp_path) -> None:
    cm = ConfigManager(base_config_dir=str(tmp_path))
    assert cm.get_global_config() == {}


