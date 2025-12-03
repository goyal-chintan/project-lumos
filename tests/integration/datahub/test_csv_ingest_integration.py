import os
import pytest
from pathlib import Path
import pandas as pd
import yaml
from core_library.common.config_manager import ConfigManager
from core_library.ingestion_handlers.ingestion_service import IngestionService
from platform_services.platform_factory import PlatformFactory


pytestmark = pytest.mark.integration


def _gms_server() -> str | None:
    return os.environ.get("DATAHUB_GMS")


@pytest.mark.skipif(_gms_server() is None, reason="DATAHUB_GMS not set; skipping DataHub integration test")
def test_csv_ingestion_to_datahub(tmp_path: Path):
    # Prepare temporary CSV
    csv_path = tmp_path / "data.csv"
    pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_csv(csv_path, index=False)

    # Prepare job YAML
    cfg_path = tmp_path / "job.yaml"
    job = {
        "source": {"type": "csv", "path": str(csv_path), "dataset_name": "it_example_dataset"},
        "sink": {"type": "datahub", "env": "DEV"},
    }
    cfg_path.write_text(yaml.safe_dump(job))

    # Create platform handler backed by real DataHub
    gms = _gms_server()
    assert gms is not None
    platform_handler = PlatformFactory.get_instance("datahub", {"gms_server": gms})

    # Run ingestion
    service = IngestionService(config_manager=ConfigManager(), platform_handler=platform_handler)
    ok = service.start_ingestion(str(cfg_path))
    assert ok is True


