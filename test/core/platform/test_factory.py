from __future__ import annotations

import pytest

from core.platform.factory import PlatformFactory
from core.platform.interface import MetadataPlatformInterface


class _DummyConfigManager:
    def __init__(self, cfg):
        self._cfg = cfg

    def get_global_config(self):
        return self._cfg


class _DummyHandler(MetadataPlatformInterface):
    def __init__(self, config):
        super().__init__(config)
        self.created_with = config

    def emit_mce(self, mce):
        return None

    def emit_mcp(self, mcp):
        return None

    def add_lineage(self, upstream_urn: str, downstream_urn: str) -> bool:
        return True

    def get_aspect_for_urn(self, urn: str, aspect_name: str):
        return None


def test_get_instance_caches_singleton(monkeypatch) -> None:
    monkeypatch.setattr(PlatformFactory, "_instances", {})
    monkeypatch.setattr(PlatformFactory, "_handler_registry", {"datahub": _DummyHandler})

    cm = _DummyConfigManager({"datahub": {"test_mode": True}})
    a = PlatformFactory.get_instance("datahub", cm)
    b = PlatformFactory.get_instance("datahub", cm)
    assert a is b
    assert isinstance(a, _DummyHandler)


def test_get_instance_unsupported_platform_raises(monkeypatch) -> None:
    monkeypatch.setattr(PlatformFactory, "_instances", {})
    monkeypatch.setattr(PlatformFactory, "_handler_registry", {})
    cm = _DummyConfigManager({})

    with pytest.raises(ValueError):
        PlatformFactory.get_instance("nope", cm)


def test_get_instance_missing_platform_config_raises(monkeypatch) -> None:
    monkeypatch.setattr(PlatformFactory, "_instances", {})
    monkeypatch.setattr(PlatformFactory, "_handler_registry", {"datahub": _DummyHandler})
    cm = _DummyConfigManager({"datahub": {}})

    with pytest.raises(ValueError):
        PlatformFactory.get_instance("datahub", cm)


