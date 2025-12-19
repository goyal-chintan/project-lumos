from __future__ import annotations

import types

import pytest

from core.platform.impl import datahub_handler


class _DummySnapshot:
    def __init__(self, urn: str, aspects):
        self.urn = urn
        self.aspects = aspects


class _DummyMCE:
    def __init__(self, urn: str, aspects):
        self.proposedSnapshot = _DummySnapshot(urn=urn, aspects=aspects)


def test_init_test_mode_sets_emitter_none() -> None:
    h = datahub_handler.DataHubHandler({"test_mode": True})
    assert h.test_mode is True
    assert h._emitter is None


def test_init_requires_gms_server_when_not_test_mode() -> None:
    with pytest.raises(ValueError):
        datahub_handler.DataHubHandler({})


def test_emit_mce_in_test_mode_does_not_raise(caplog) -> None:
    h = datahub_handler.DataHubHandler({"test_mode": True})
    aspect0 = types.SimpleNamespace(schemaName="s")
    mce = _DummyMCE("urn:li:dataset:(x)", [aspect0, object()])

    caplog.set_level("INFO")
    h.emit_mce(mce)
    assert any("TEST MODE" in r.message for r in caplog.records)


def test_emit_mce_calls_emit_as_mcps_when_not_test_mode(monkeypatch) -> None:
    class _DummyEmitter:
        def __init__(self, gms_server):
            self.gms_server = gms_server

        def emit_mcp(self, _mcp):
            return None

    monkeypatch.setattr(datahub_handler, "DatahubRestEmitter", _DummyEmitter)

    h = datahub_handler.DataHubHandler({"gms_server": "http://example:8080"})

    called = []

    def _fake_emit_as_mcps(_mce):
        called.append(True)
        return True

    monkeypatch.setattr(h, "_emit_as_mcps", _fake_emit_as_mcps)

    mce = _DummyMCE("urn:li:dataset:(x)", [object()])
    h.emit_mce(mce)

    assert called == [True]


def test_emit_as_mcps_emits_each_aspect_and_returns_true_if_any_succeed(monkeypatch) -> None:
    class _DummyEmitter:
        def __init__(self, gms_server):
            self.calls = []

        def emit_mcp(self, mcp):
            # fail when aspect has marker
            if getattr(mcp.aspect, "fail", False):
                raise RuntimeError("boom")
            self.calls.append(mcp)

    class _DummyMCP:
        def __init__(self, entityUrn, aspect):
            self.entityUrn = entityUrn
            self.aspect = aspect

    monkeypatch.setattr(datahub_handler, "DatahubRestEmitter", _DummyEmitter)
    monkeypatch.setattr(datahub_handler, "MetadataChangeProposalWrapper", _DummyMCP)

    h = datahub_handler.DataHubHandler({"gms_server": "http://example:8080"})
    ok = types.SimpleNamespace()
    bad = types.SimpleNamespace(fail=True)
    mce = _DummyMCE("urn:li:dataset:(x)", [ok, bad])

    assert h._emit_as_mcps(mce) is True
    assert len(h._emitter.calls) == 1


def test_emit_as_mcps_returns_false_when_all_fail(monkeypatch) -> None:
    class _DummyEmitter:
        def __init__(self, gms_server):
            pass

        def emit_mcp(self, _mcp):
            raise RuntimeError("boom")

    class _DummyMCP:
        def __init__(self, entityUrn, aspect):
            self.entityUrn = entityUrn
            self.aspect = aspect

    monkeypatch.setattr(datahub_handler, "DatahubRestEmitter", _DummyEmitter)
    monkeypatch.setattr(datahub_handler, "MetadataChangeProposalWrapper", _DummyMCP)

    h = datahub_handler.DataHubHandler({"gms_server": "http://example:8080"})
    mce = _DummyMCE("urn:li:dataset:(x)", [types.SimpleNamespace(fail=True)])

    assert h._emit_as_mcps(mce) is False


def test_emit_via_rest_api_posts_each_aspect(monkeypatch) -> None:
    class _DummyEmitter:
        def __init__(self, gms_server):
            pass

        def emit_mcp(self, _mcp):
            return None

    monkeypatch.setattr(datahub_handler, "DatahubRestEmitter", _DummyEmitter)

    h = datahub_handler.DataHubHandler({"gms_server": "http://example:8080"})
    monkeypatch.setattr(h, "_convert_aspect_to_dict", lambda _aspect: {"x": 1})

    posts = []

    class _Resp:
        status_code = 200

    def _fake_post(url, json, headers, timeout):
        posts.append((url, json, headers, timeout))
        return _Resp()

    monkeypatch.setattr(datahub_handler.requests, "post", _fake_post)

    mce = _DummyMCE("urn:li:dataset:(x)", [types.SimpleNamespace(), types.SimpleNamespace()])
    h._emit_via_rest_api(mce)

    assert len(posts) == 2
    assert posts[0][0] == "http://example:8080/aspects"


def test_convert_aspect_to_dict_schema_aspect() -> None:
    class StringTypeClass:
        pass

    field_type = types.SimpleNamespace(type=StringTypeClass())
    field = types.SimpleNamespace(
        fieldPath="f1",
        nativeDataType="string",
        type=field_type,
        nullable=True,
        recursive=False,
        isPartOfKey=False,
    )
    platform_schema = types.SimpleNamespace(rawSchema="raw")
    aspect = types.SimpleNamespace(
        schemaName="s",
        platform="urn:li:dataPlatform:csv",
        version=0,
        hash="h",
        fields=[field],
        platformSchema=platform_schema,
    )

    h = datahub_handler.DataHubHandler({"test_mode": True})
    out = h._convert_aspect_to_dict(aspect)

    assert out["schemaName"] == "s"
    assert out["fields"][0]["fieldPath"] == "f1"
    assert out["fields"][0]["type"]["type"] == "StringType"


def test_convert_aspect_to_dict_non_schema_aspect() -> None:
    aspect = types.SimpleNamespace(name="n", description="d", customProperties={"a": 1})
    h = datahub_handler.DataHubHandler({"test_mode": True})
    out = h._convert_aspect_to_dict(aspect)
    assert out == {"customProperties": {"a": 1}, "name": "n", "description": "d"}


def test_emit_mcp_delegates_and_raises_on_failure(monkeypatch) -> None:
    class _DummyEmitter:
        def __init__(self, gms_server):
            pass

        def emit_mcp(self, _mcp):
            raise RuntimeError("boom")

    monkeypatch.setattr(datahub_handler, "DatahubRestEmitter", _DummyEmitter)
    h = datahub_handler.DataHubHandler({"gms_server": "http://example:8080"})

    with pytest.raises(RuntimeError):
        h.emit_mcp(types.SimpleNamespace(entityUrn="urn:li:dataset:(x)"))


def test_add_lineage_returns_true_and_calls_emit_mcp(monkeypatch) -> None:
    class _DummyEmitter:
        def __init__(self, gms_server):
            pass

        def emit_mcp(self, _mcp):
            return None

    monkeypatch.setattr(datahub_handler, "DatahubRestEmitter", _DummyEmitter)
    h = datahub_handler.DataHubHandler({"gms_server": "http://example:8080"})

    called = []

    def _fake_emit_mcp(mcp):
        called.append(mcp)

    monkeypatch.setattr(h, "emit_mcp", _fake_emit_mcp)

    upstream = "urn:li:dataset:(urn:li:dataPlatform:csv,up,DEV)"
    downstream = "urn:li:dataset:(urn:li:dataPlatform:csv,down,DEV)"
    assert h.add_lineage(upstream, downstream) is True
    assert len(called) == 1


def test_add_lineage_returns_false_when_emit_mcp_raises(monkeypatch) -> None:
    class _DummyEmitter:
        def __init__(self, gms_server):
            pass

        def emit_mcp(self, _mcp):
            return None

    monkeypatch.setattr(datahub_handler, "DatahubRestEmitter", _DummyEmitter)
    h = datahub_handler.DataHubHandler({"gms_server": "http://example:8080"})

    def _boom(_mcp):
        raise RuntimeError("boom")

    monkeypatch.setattr(h, "emit_mcp", _boom)

    assert h.add_lineage("urn:up", "urn:down") is False


def test_get_aspect_for_urn_delegates_to_emitter(monkeypatch) -> None:
    class _DummyEmitter:
        def __init__(self, gms_server):
            pass

        def emit_mcp(self, _mcp):
            return None

        def get_latest_aspect_or_null(self, entity_urn, aspect_type):
            return (entity_urn, aspect_type)

    monkeypatch.setattr(datahub_handler, "DatahubRestEmitter", _DummyEmitter)
    h = datahub_handler.DataHubHandler({"gms_server": "http://example:8080"})

    out = h.get_aspect_for_urn("urn:x", "upstreamLineage")
    assert out[0] == "urn:x"


