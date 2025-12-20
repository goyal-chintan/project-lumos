"""Unit tests for lineage services."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock


class TestDatasetLineageService:
    """Tests for DatasetLineageService."""
    
    def test_dataset_lineage_service_import(self) -> None:
        """Test that DatasetLineageService can be imported."""
        from feature.lineage.dataset_lineage_service import DatasetLineageService
        assert DatasetLineageService is not None
    
    def test_dataset_lineage_service_initialization(self) -> None:
        """Test DatasetLineageService initialization."""
        from feature.lineage.dataset_lineage_service import DatasetLineageService
        
        mock_handler = MagicMock()
        mock_config_manager = MagicMock()
        mock_config_manager.get_global_config.return_value = {"default_env": "DEV"}
        
        service = DatasetLineageService(mock_handler, mock_config_manager)
        assert service.platform_handler == mock_handler
        assert service.config_manager == mock_config_manager
        assert service.env == "DEV"
    
    def test_dataset_lineage_service_build_urn(self) -> None:
        """Test URN building for lineage."""
        from feature.lineage.dataset_lineage_service import DatasetLineageService
        
        mock_handler = MagicMock()
        mock_config_manager = MagicMock()
        mock_config_manager.get_global_config.return_value = {"default_env": "DEV"}
        
        service = DatasetLineageService(mock_handler, mock_config_manager)
        urn = service._build_urn("csv", "test_dataset")
        
        assert "csv" in urn
        assert "test_dataset" in urn
        assert "DEV" in urn
    
    def test_dataset_lineage_service_build_urn_requires_both_params(self) -> None:
        """Test that _build_urn requires both data_type and dataset_name."""
        from feature.lineage.dataset_lineage_service import DatasetLineageService
        
        mock_handler = MagicMock()
        mock_config_manager = MagicMock()
        mock_config_manager.get_global_config.return_value = {"default_env": "DEV"}
        
        service = DatasetLineageService(mock_handler, mock_config_manager)
        
        with pytest.raises(ValueError):
            service._build_urn("", "test_dataset")
        
        with pytest.raises(ValueError):
            service._build_urn("csv", "")
    
    def test_dataset_lineage_service_add_lineage_from_config_missing_key(self) -> None:
        """Test add_lineage_from_config returns False when lineage key is missing."""
        from feature.lineage.dataset_lineage_service import DatasetLineageService
        
        mock_handler = MagicMock()
        mock_config_manager = MagicMock()
        mock_config_manager.get_global_config.return_value = {"default_env": "DEV"}
        
        service = DatasetLineageService(mock_handler, mock_config_manager)
        
        config = {"not_lineage": {}}
        result = service.add_lineage_from_config(config)
        assert result is False
    
    def test_dataset_lineage_service_add_lineage_from_config_missing_downstream(self) -> None:
        """Test add_lineage_from_config returns False when downstream is missing."""
        from feature.lineage.dataset_lineage_service import DatasetLineageService
        
        mock_handler = MagicMock()
        mock_config_manager = MagicMock()
        mock_config_manager.get_global_config.return_value = {"default_env": "DEV"}
        
        service = DatasetLineageService(mock_handler, mock_config_manager)
        
        config = {
            "lineage": {
                "upstreams": [{"data_type": "csv", "dataset": "source"}]
            }
        }
        result = service.add_lineage_from_config(config)
        assert result is False


class TestDataJobService:
    """Tests for DataJobService."""
    
    def test_data_job_service_import(self) -> None:
        """Test that DataJobService can be imported."""
        from feature.lineage.data_job_service import DataJobService
        assert DataJobService is not None
    
    def test_data_job_service_initialization(self) -> None:
        """Test DataJobService initialization."""
        from feature.lineage.data_job_service import DataJobService
        
        mock_handler = MagicMock()
        mock_config_manager = MagicMock()
        mock_config_manager.get_global_config.return_value = {"default_env": "DEV"}
        
        service = DataJobService(mock_handler, mock_config_manager)
        assert service.platform_handler == mock_handler
        assert service.config_manager == mock_config_manager

