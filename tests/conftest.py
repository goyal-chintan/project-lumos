"""Pytest configuration and fixtures for Lumos Framework tests."""
import pytest
from pathlib import Path
from typing import Dict, Any


@pytest.fixture
def test_data_dir() -> Path:
    """Provide path to test data directory."""
    return Path(__file__).parent / "test_data"


@pytest.fixture
def sample_csv_config() -> Dict[str, Any]:
    """Provide a sample CSV ingestion configuration."""
    return {
        "source": {
            "type": "csv",
            "path": "test_data/sample.csv",
            "dataset_name": "test_dataset",
            "delimiter": ","
        },
        "sink": {
            "type": "datahub",
            "env": "TEST"
        }
    }


@pytest.fixture
def sample_mongo_config() -> Dict[str, Any]:
    """Provide a sample MongoDB ingestion configuration."""
    return {
        "source": {
            "type": "mongodb",
            "uri": "mongodb://localhost:27017",
            "database": "test_db"
        },
        "sink": {
            "type": "datahub",
            "env": "TEST"
        }
    }


@pytest.fixture
def mock_datahub_config() -> Dict[str, Any]:
    """Provide mock DataHub configuration."""
    return {
        "gms_server": "http://localhost:8080"
    }


@pytest.fixture
def sample_lineage_config() -> Dict[str, Any]:
    """Provide a sample lineage configuration."""
    return {
        "lineage": {
            "downstream": "urn:li:dataset:(urn:li:dataPlatform:hive,test_table,TEST)",
            "upstreams": [
                {"urn": "urn:li:dataset:(urn:li:dataPlatform:csv,source1,TEST)"},
                {"urn": "urn:li:dataset:(urn:li:dataPlatform:mongodb,test_db.collection1,TEST)"}
            ]
        }
    }

