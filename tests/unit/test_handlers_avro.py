from pathlib import Path
from typing import Any, Dict, List
from fastavro import writer, parse_schema
from core_library.ingestion_handlers.avro_handler import AvroIngestionHandler
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


def _create_avro(dir_path: Path, name: str) -> Path:
    path = dir_path / f"{name}.avro"
    schema = {
        "name": name,
        "type": "record",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": ["null", "string"]},
            {"name": "score", "type": "double"},
        ],
    }
    parsed = parse_schema(schema)
    with open(path, "wb") as out:
        writer(out, parsed, [{"id": 1, "name": "a", "score": 1.0}])
    return path


def test_avro_handler_emits_mce(tmp_path: Path):
    avro_dir = tmp_path / "avros"
    avro_dir.mkdir()
    _create_avro(avro_dir, "dataset_one")

    config = {
        "source": {"type": "avro", "directory_path": str(avro_dir)},
        "sink": {"env": "DEV"},
    }
    platform = _CapturingPlatform({})
    handler = AvroIngestionHandler(config, platform)

    handler.ingest()

    assert len(platform.emitted_mces) == 1
    mce = platform.emitted_mces[0]
    assert "dataset_one" in str(mce.proposedSnapshot.urn)


