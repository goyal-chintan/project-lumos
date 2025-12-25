import pytest

from core.platform.impl import datahub_service


@pytest.mark.unit
class TestDataHubDataCatalog:
    def test_emitter_is_constructed_with_gms_server(self, monkeypatch) -> None:
        created = {"gms": None}

        class _FakeEmitter:
            def __init__(self, *, gms_server: str):
                created["gms"] = gms_server

        monkeypatch.setattr(datahub_service, "DatahubRestEmitter", _FakeEmitter)

        _ = datahub_service.DataHubDataCatalog(gms_server="http://localhost:8080")
        assert created["gms"] == "http://localhost:8080"

    def test_emit_delegates(self, monkeypatch) -> None:
        calls = {"mce": None}

        class _FakeEmitter:
            def __init__(self, *, gms_server: str):
                pass

            def emit(self, mce):
                calls["mce"] = mce

        monkeypatch.setattr(datahub_service, "DatahubRestEmitter", _FakeEmitter)

        catalog = datahub_service.DataHubDataCatalog(gms_server="http://localhost:8080")
        catalog.emit({"mce": 1})
        assert calls["mce"] == {"mce": 1}

    def test_emit_mcp_delegates(self, monkeypatch) -> None:
        calls = {"mcp": None}

        class _FakeEmitter:
            def __init__(self, *, gms_server: str):
                pass

            def emit_mcp(self, mcp):
                calls["mcp"] = mcp

        monkeypatch.setattr(datahub_service, "DatahubRestEmitter", _FakeEmitter)

        catalog = datahub_service.DataHubDataCatalog(gms_server="http://localhost:8080")
        catalog.emit_mcp({"mcp": 1})
        assert calls["mcp"] == {"mcp": 1}

    def test_get_emitter_returns_underlying_emitter(self, monkeypatch) -> None:
        emitter_obj = object()

        monkeypatch.setattr(datahub_service, "DatahubRestEmitter", lambda *, gms_server: emitter_obj)

        catalog = datahub_service.DataHubDataCatalog(gms_server="http://localhost:8080")
        assert catalog.get_emitter() is emitter_obj
