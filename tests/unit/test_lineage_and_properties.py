from typing import Any, Dict, List
from core_library.lineage_services.dataset_lineage_service import DatasetLineageService
from core_library.enrichment_services.properties_service import PropertiesService
from platform_services.metadata_platform_interface import MetadataPlatformInterface


class _CapturingPlatform(MetadataPlatformInterface):
    def __init__(self, config: Dict[str, Any] | None = None):
        self.config = config or {}
        self.lineage_calls: List[tuple[str, str]] = []
        self.emitted_mcps: List[Any] = []

    def emit_mce(self, mce: Any) -> None:  # pragma: no cover
        pass

    def emit_mcp(self, mcp: Any) -> None:
        self.emitted_mcps.append(mcp)

    def add_lineage(self, upstream_urn: str, downstream_urn: str) -> bool:
        self.lineage_calls.append((upstream_urn, downstream_urn))
        return True


def test_dataset_lineage_service_add_lineage():
    platform = _CapturingPlatform({})
    service = DatasetLineageService(platform)
    ok = service.add_lineage("urn:li:dataset:(a)", "urn:li:dataset:(b)")
    assert ok is True
    assert platform.lineage_calls == [("urn:li:dataset:(a)", "urn:li:dataset:(b)")]


def test_dataset_lineage_service_from_config_multiple():
    platform = _CapturingPlatform({})
    service = DatasetLineageService(platform)
    config = {
        "lineage": {
            "downstream": "urn:li:dataset:(b)",
            "upstreams": [{"urn": "urn:li:dataset:(a)"}, {"urn": "urn:li:dataset:(c)"}],
        }
    }
    ok = service.add_lineage_from_config(config)
    assert ok is True
    assert ("urn:li:dataset:(a)", "urn:li:dataset:(b)") in platform.lineage_calls
    assert ("urn:li:dataset:(c)", "urn:li:dataset:(b)") in platform.lineage_calls


def test_properties_service_emits_mcp():
    platform = _CapturingPlatform({})
    svc = PropertiesService(platform)
    ok = svc.update_properties(
        "urn:li:dataset:(plat,name,DEV)",
        {"name": "n", "description": "d", "custom_properties": {"k": "v"}},
    )
    assert ok is True
    assert len(platform.emitted_mcps) == 1


