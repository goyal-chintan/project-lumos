from typing import Any, Dict, List
import pytest
from core_library.ingestion_handlers.mongo_handler import MongoIngestionHandler
from platform_services.metadata_platform_interface import MetadataPlatformInterface

mongomock = pytest.importorskip("mongomock")


class _CapturingPlatform(MetadataPlatformInterface):
    def __init__(self, config: Dict[str, Any] | None = None):
        self.config = config or {}
        self.emitted_mces: List[Any] = []
        self.emitted_mcps: List[Any] = []

    def emit_mce(self, mce: Any) -> None:
        self.emitted_mces.append(mce)

    def emit_mcp(self, mcp: Any) -> None:  # pragma: no cover
        self.emitted_mcps.append(mcp)

    def add_lineage(self, upstream_urn: str, downstream_urn: str) -> bool:  # pragma: no cover
        return True


def test_mongo_handler_emits_mce(monkeypatch):
    class _MongoClient(mongomock.MongoClient):
        def server_info(self):
            return {}

    # Patch pymongo.MongoClient to our mongomock client
    import pymongo  # type: ignore

    monkeypatch.setattr(pymongo, "MongoClient", _MongoClient)

    # Seed fake DB
    client = _MongoClient()
    db = client["testdb"]
    db["items"].insert_one({"_id": 1, "name": "x", "count": 1})

    config = {
        "source": {"type": "mongodb", "uri": "mongodb://fake:27017", "database": "testdb"},
        "sink": {"env": "DEV"},
    }
    platform = _CapturingPlatform({})
    handler = MongoIngestionHandler(config, platform)
    # Inject our seeded client by temporarily patching inside handler call
    def _client(uri, serverSelectionTimeoutMS=5000):
        return client

    monkeypatch.setattr(pymongo, "MongoClient", lambda *a, **k: client)

    handler.ingest()
    assert len(platform.emitted_mces) >= 1


