import pytest

from core.platform.impl.datahub_handler import DataHubHandler


@pytest.mark.unit
class TestDataHubHandler:
    def test_convert_aspect_to_dict_schema_like_object(self) -> None:
        class _PlatformSchema:
            rawSchema = "raw"

        class _TypeType:
            pass

        class _Type:
            type = _TypeType()

        class _Field:
            def __init__(self, field_path: str):
                self.fieldPath = field_path
                self.nativeDataType = "string"
                self.type = _Type()
                self.nullable = True
                self.recursive = False
                self.isPartOfKey = False

        class _Aspect:
            schemaName = "s"
            platform = "p"
            version = 1
            hash = "h"
            platformSchema = _PlatformSchema()
            fields = [_Field("a"), _Field("b")]

        handler = DataHubHandler({"test_mode": True})

        result = handler._convert_aspect_to_dict(_Aspect())
        assert result["schemaName"] == "s"
        assert result["platformSchema"]["rawSchema"] == "raw"
        assert [f["fieldPath"] for f in result["fields"]] == ["a", "b"]

    def test_convert_aspect_to_dict_non_schema_extracts_known_fields(self) -> None:
        class _Aspect:
            name = "n"
            description = "d"
            customProperties = {"k": "v"}

        handler = DataHubHandler({"test_mode": True})
        result = handler._convert_aspect_to_dict(_Aspect())
        assert result == {"customProperties": {"k": "v"}, "name": "n", "description": "d"}

    def test_emit_as_mcps_returns_true_when_any_aspect_emits(self, monkeypatch) -> None:
        class _Emitter:
            def __init__(self):
                self.calls = 0

            def emit_mcp(self, _mcp):
                self.calls += 1

        emitter = _Emitter()

        class _Snapshot:
            urn = "urn:li:dataset:(x)"
            aspects = [object(), object()]

        class _MCE:
            proposedSnapshot = _Snapshot()

        handler = DataHubHandler({"test_mode": True})
        handler.test_mode = False
        handler._emitter = emitter

        class _MCP:
            def __init__(self, entityUrn, aspect):
                self.entityUrn = entityUrn
                self.aspect = aspect

        monkeypatch.setattr("core.platform.impl.datahub_handler.MetadataChangeProposalWrapper", _MCP)

        assert handler._emit_as_mcps(_MCE()) is True
        assert emitter.calls == 2

    def test_emit_mcp_noops_in_test_mode(self) -> None:
        handler = DataHubHandler({"test_mode": True})

        class _MCP:
            entityUrn = "urn"
            aspect = object()

        handler.emit_mcp(_MCP())

    def test_add_lineage_returns_true_in_test_mode(self) -> None:
        handler = DataHubHandler({"test_mode": True})
        assert handler.add_lineage("u", "d") is True

    def test_get_aspect_for_urn_returns_none_on_exception(self) -> None:
        class _Emitter:
            def get_latest_aspect_or_null(self, *, entity_urn, aspect_type):
                raise RuntimeError("boom")

        handler = DataHubHandler({"test_mode": True})
        handler.test_mode = False
        handler._emitter = _Emitter()

        assert handler.get_aspect_for_urn("urn", "ignored") is None
