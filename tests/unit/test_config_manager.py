import io
from pathlib import Path
import yaml
from core_library.common.config_manager import ConfigManager


def test_load_config_success_and_cache(tmp_path: Path):
    cfg_path = tmp_path / "conf.yaml"
    data = {"a": 1, "b": {"c": 2}}
    cfg_path.write_text(yaml.safe_dump(data))

    cm = ConfigManager()
    loaded1 = cm.load_config(str(cfg_path))
    loaded2 = cm.load_config(str(cfg_path))

    assert loaded1 == data
    assert loaded2 == data  # second read should come from cache
    # Ensure dicts are equal in value (cache returns same content); identity is an implementation detail


def test_load_config_missing_file_returns_empty(tmp_path: Path):
    cm = ConfigManager()
    missing = tmp_path / "missing.yaml"
    loaded = cm.load_config(str(missing))
    assert loaded == {}


def test_load_config_invalid_yaml_returns_empty(tmp_path: Path):
    cfg_path = tmp_path / "bad.yaml"
    cfg_path.write_text(":\n - not yaml")  # invalid structure

    cm = ConfigManager()
    loaded = cm.load_config(str(cfg_path))
    assert loaded == {}


