from __future__ import annotations

from core.common import emitter


def test_get_data_catalog_delegates_to_factory(monkeypatch) -> None:
    calls = []
    sentinel = object()

    class _Factory:
        @staticmethod
        def get_instance(*, platform, config):
            calls.append((platform, config))
            return sentinel

    monkeypatch.setattr(emitter, "DataCatalogFactory", _Factory)
    monkeypatch.setattr(emitter, "GLOBAL_SETTINGS", {"datahub_gms": "http://example:8080"})

    out = emitter.get_data_catalog()

    assert out is sentinel
    assert calls == [("datahub", {"gms_server": "http://example:8080"})]


def test_get_data_catalog_uses_none_when_setting_missing(monkeypatch) -> None:
    calls = []

    class _Factory:
        @staticmethod
        def get_instance(*, platform, config):
            calls.append((platform, config))
            return object()

    monkeypatch.setattr(emitter, "DataCatalogFactory", _Factory)
    monkeypatch.setattr(emitter, "GLOBAL_SETTINGS", {})  # missing datahub_gms

    emitter.get_data_catalog()

    assert calls == [("datahub", {"gms_server": None})]


