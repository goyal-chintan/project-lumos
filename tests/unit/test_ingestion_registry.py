from typing import Any, Dict
import pytest
from core_library.common.config_manager import ConfigManager
from core_library.ingestion_handlers.ingestion_service import IngestionService
from core_library.ingestion_handlers.csv_handler import CSVIngestionHandler
from platform_services.metadata_platform_interface import MetadataPlatformInterface


class _DummyPlatformHandler(MetadataPlatformInterface):
    def __init__(self, config: Dict[str, Any] | None = None):
        self.config = config or {}

    def emit_mce(self, mce: Any) -> None:  # pragma: no cover - not used here
        pass

    def emit_mcp(self, mcp: Any) -> None:  # pragma: no cover - not used here
        pass

    def add_lineage(self, upstream_urn: str, downstream_urn: str) -> bool:  # pragma: no cover
        return True


def _service() -> IngestionService:
    return IngestionService(config_manager=ConfigManager(), platform_handler=_DummyPlatformHandler({}))


def test_missing_source_type_raises_value_error():
    service = _service()
    with pytest.raises(ValueError):
        service._get_handler({"source": {}})  # type: ignore[arg-type]


def test_unknown_source_type_raises_not_implemented():
    service = _service()
    with pytest.raises(NotImplementedError):
        service._get_handler({"source": {"type": "unknown"}})  # type: ignore[arg-type]


def test_csv_handler_is_selected():
    service = _service()
    handler = service._get_handler(
        {"source": {"type": "csv"}, "sink": {"env": "DEV"}}
    )
    assert isinstance(handler, CSVIngestionHandler)


