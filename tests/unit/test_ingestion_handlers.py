"""Unit tests for ingestion handlers."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch


class TestCSVHandler:
    """Tests for CSV ingestion handler."""
    
    def test_csv_handler_import(self) -> None:
        """Test that CSV handler can be imported."""
        from feature.ingestion.handlers.csv import CSVIngestionHandler
        assert CSVIngestionHandler is not None

    def test_csv_handler_infer_schema_from_file(self, tmp_path) -> None:
        """Test schema inference from CSV file."""
        from feature.ingestion.handlers.csv import CSVIngestionHandler
        
        # Create a test CSV file
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,name,value\n1,test,100\n2,foo,200\n")
        
        # Full config format that base handler expects
        config = {
            "source": {
                "type": "csv",
                "path": str(csv_file),
            },
            "schema": {},
        }
        
        handler = CSVIngestionHandler(config)
        fields = handler._get_schema_fields()
        
        assert len(fields) == 3
        field_names = [f.fieldPath for f in fields]
        assert "id" in field_names
        assert "name" in field_names
        assert "value" in field_names

    def test_csv_handler_get_raw_schema(self, tmp_path) -> None:
        """Test raw schema extraction from CSV returns empty string (default behavior)."""
        from feature.ingestion.handlers.csv import CSVIngestionHandler
        
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("col1,col2\na,b\n")
        
        config = {
            "source": {
                "type": "csv",
                "path": str(csv_file),
            },
            "schema": {}
        }
        
        handler = CSVIngestionHandler(config)
        raw_schema = handler._get_raw_schema()
        
        # CSV handler inherits default behavior returning empty string
        assert raw_schema == ""


class TestHandlerFactory:
    """Tests for ingestion handler factory."""
    
    def test_handler_factory_import(self) -> None:
        """Test that handler factory can be imported."""
        from feature.ingestion.handlers.factory import HandlerFactory
        assert HandlerFactory is not None

    def test_handler_factory_get_csv_handler(self, tmp_path) -> None:
        """Test factory returns CSV handler for csv source type."""
        from feature.ingestion.handlers.factory import HandlerFactory
        from feature.ingestion.handlers.csv import CSVIngestionHandler
        
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,name\n1,test\n")
        
        config = {
            "source": {"type": "csv", "path": str(csv_file)},
            "schema": {}
        }
        
        handler = HandlerFactory.get_handler(config)
        assert isinstance(handler, CSVIngestionHandler)

    def test_handler_factory_get_avro_handler(self, tmp_path) -> None:
        """Test factory returns Avro handler for avro source type."""
        from feature.ingestion.handlers.factory import HandlerFactory
        from feature.ingestion.handlers.avro import AvroIngestionHandler
        
        config = {
            "source": {"type": "avro", "path": str(tmp_path / "test.avro")},
            "schema": {}
        }
        
        handler = HandlerFactory.get_handler(config)
        assert isinstance(handler, AvroIngestionHandler)

    def test_handler_factory_get_parquet_handler(self, tmp_path) -> None:
        """Test factory returns Parquet handler for parquet source type."""
        from feature.ingestion.handlers.factory import HandlerFactory
        from feature.ingestion.handlers.parquet import ParquetIngestionHandler
        
        config = {
            "source": {"type": "parquet", "path": str(tmp_path / "test.parquet")},
            "schema": {}
        }
        
        handler = HandlerFactory.get_handler(config)
        assert isinstance(handler, ParquetIngestionHandler)

    def test_handler_factory_unknown_source_type(self) -> None:
        """Test factory raises error for unknown source type."""
        from feature.ingestion.handlers.factory import HandlerFactory
        
        config = {
            "source": {"type": "unknown_type", "path": "/tmp/test"},
            "schema": {}
        }
        
        with pytest.raises(NotImplementedError):
            HandlerFactory.get_handler(config)

    def test_handler_factory_missing_type(self) -> None:
        """Test factory raises error when source type is missing."""
        from feature.ingestion.handlers.factory import HandlerFactory
        
        config = {
            "source": {"path": "/tmp/test"},
            "schema": {}
        }
        
        with pytest.raises(ValueError):
            HandlerFactory.get_handler(config)

    def test_handler_factory_get_supported_types(self) -> None:
        """Test get_supported_types returns expected types."""
        from feature.ingestion.handlers.factory import HandlerFactory
        
        supported = HandlerFactory.get_supported_types()
        assert "csv" in supported
        assert "avro" in supported
        assert "parquet" in supported
        assert "mongodb" in supported
        assert "s3" in supported


class TestHandlerConstants:
    """Tests for handler constants."""
    
    def test_constants_import(self) -> None:
        """Test that constants can be imported."""
        from feature.ingestion.handlers import constants
        assert constants.HANDLER_TYPE_CSV == "csv"
        assert constants.HANDLER_TYPE_AVRO == "avro"
        assert constants.HANDLER_TYPE_PARQUET == "parquet"

    def test_supported_types_includes_all_file_types(self) -> None:
        """Test that SUPPORTED_TYPES includes all file-based types."""
        from feature.ingestion.handlers import constants
        
        for file_type in constants.FILE_BASED_TYPES:
            assert file_type in constants.SUPPORTED_TYPES
