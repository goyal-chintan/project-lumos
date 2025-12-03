from pathlib import Path
import pandas as pd
from typing import Any, Dict, List
from core_library.ingestion_handlers.csv_handler import CSVIngestionHandler
from platform_services.metadata_platform_interface import MetadataPlatformInterface


class _CapturingPlatform(MetadataPlatformInterface):
    def __init__(self, config: Dict[str, Any] | None = None):
        self.config = config or {}
        self.emitted_mces: List[Any] = []
        self.emitted_mcps: List[Any] = []

    def emit_mce(self, mce: Any) -> None:
        self.emitted_mces.append(mce)

    def emit_mcp(self, mcp: Any) -> None:
        self.emitted_mcps.append(mcp)

    def add_lineage(self, upstream_urn: str, downstream_urn: str) -> bool:  # pragma: no cover
        return True


def test_csv_handler_emits_mce(tmp_path: Path):
    csv_path = tmp_path / "data.csv"
    pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}).to_csv(csv_path, index=False)

    config = {
        "source": {"type": "csv", "path": str(csv_path), "dataset_name": "example_dataset"},
        "sink": {"env": "DEV"},
    }
    platform = _CapturingPlatform({})
    handler = CSVIngestionHandler(config, platform)

    handler.ingest()

    assert len(platform.emitted_mces) == 1
    mce = platform.emitted_mces[0]
    assert "example_dataset" in str(mce.proposedSnapshot.urn)


