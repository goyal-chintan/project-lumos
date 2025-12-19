from __future__ import annotations

from core.platform.impl import datahub_service


def test_datahub_data_catalog_delegates_to_emitter(monkeypatch) -> None:
    calls = {"emit": 0, "emit_mcp": 0}

    class _DummyEmitter:
        def __init__(self, gms_server):
            self.gms_server = gms_server

        def emit(self, _mce):
            calls["emit"] += 1

        def emit_mcp(self, _mcp):
            calls["emit_mcp"] += 1

    monkeypatch.setattr(datahub_service, "DatahubRestEmitter", _DummyEmitter)

    dc = datahub_service.DataHubDataCatalog(gms_server="http://example:8080")
    assert dc.get_emitter().gms_server == "http://example:8080"

    dc.emit(object())
    dc.emit_mcp(object())
    assert calls == {"emit": 1, "emit_mcp": 1}


