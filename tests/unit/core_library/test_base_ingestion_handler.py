"""Unit tests for BaseIngestionHandler."""
import pytest
from unittest.mock import Mock
from core_library.ingestion_handlers.base_ingestion_handler import BaseIngestionHandler


class TestBaseIngestionHandler:
    """Test cases for BaseIngestionHandler class."""
    
    def test_init(self, sample_csv_config):
        """Test BaseIngestionHandler initialization."""
        mock_platform = Mock()
        handler = BaseIngestionHandler(sample_csv_config, mock_platform)
        
        assert handler.config == sample_csv_config
        assert handler.platform_handler == mock_platform
        assert handler.source_config == sample_csv_config.get("source", {})
        assert handler.sink_config == sample_csv_config.get("sink", {})
    
    def test_validate_config_missing_fields(self):
        """Test config validation with missing required fields."""
        config = {"source": {}, "sink": {}}
        mock_platform = Mock()
        handler = BaseIngestionHandler(config, mock_platform)
        handler.required_fields = ["missing_field"]
        
        assert not handler.validate_config()
    
    def test_validate_config_success(self, sample_csv_config):
        """Test successful config validation."""
        mock_platform = Mock()
        handler = BaseIngestionHandler(sample_csv_config, mock_platform)
        # No required fields set
        assert handler.validate_config()
    
    def test_ingest_not_implemented(self, sample_csv_config):
        """Test that ingest method must be implemented by subclasses."""
        mock_platform = Mock()
        handler = BaseIngestionHandler(sample_csv_config, mock_platform)
        
        with pytest.raises(NotImplementedError):
            handler.ingest()

