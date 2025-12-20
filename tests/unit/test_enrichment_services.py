"""Unit tests for enrichment services."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock


class TestEnrichmentFactory:
    """Tests for EnrichmentServiceFactory."""
    
    def test_enrichment_factory_import(self) -> None:
        """Test that enrichment factory can be imported."""
        from feature.enrichment.factory import EnrichmentServiceFactory
        assert EnrichmentServiceFactory is not None

    def test_enrichment_factory_get_description_service(self) -> None:
        """Test factory returns DescriptionService for description type."""
        from feature.enrichment.factory import EnrichmentServiceFactory
        from feature.enrichment.description_service import DescriptionService
        
        mock_handler = MagicMock()
        mock_config_manager = MagicMock()
        mock_config_manager.get_global_config.return_value = {"default_env": "DEV"}
        
        service = EnrichmentServiceFactory.get_service("description", mock_handler, mock_config_manager)
        assert isinstance(service, DescriptionService)

    def test_enrichment_factory_get_tag_service(self) -> None:
        """Test factory returns TagService for tags type."""
        from feature.enrichment.factory import EnrichmentServiceFactory
        from feature.enrichment.tag_service import TagService
        
        mock_handler = MagicMock()
        mock_config_manager = MagicMock()
        mock_config_manager.get_global_config.return_value = {"default_env": "DEV"}
        
        service = EnrichmentServiceFactory.get_service("tags", mock_handler, mock_config_manager)
        assert isinstance(service, TagService)

    def test_enrichment_factory_get_properties_service(self) -> None:
        """Test factory returns PropertiesService for properties type."""
        from feature.enrichment.factory import EnrichmentServiceFactory
        from feature.enrichment.properties_service import PropertiesService
        
        mock_handler = MagicMock()
        mock_config_manager = MagicMock()
        mock_config_manager.get_global_config.return_value = {"default_env": "DEV"}
        
        service = EnrichmentServiceFactory.get_service("properties", mock_handler, mock_config_manager)
        assert isinstance(service, PropertiesService)

    def test_enrichment_factory_get_documentation_service(self) -> None:
        """Test factory returns DocumentationService for documentation type."""
        from feature.enrichment.factory import EnrichmentServiceFactory
        from feature.enrichment.documentation_service import DocumentationService
        
        mock_handler = MagicMock()
        mock_config_manager = MagicMock()
        mock_config_manager.get_global_config.return_value = {"default_env": "DEV"}
        
        service = EnrichmentServiceFactory.get_service("documentation", mock_handler, mock_config_manager)
        assert isinstance(service, DocumentationService)

    def test_enrichment_factory_unknown_type(self) -> None:
        """Test factory raises error for unknown enrichment type."""
        from feature.enrichment.factory import EnrichmentServiceFactory
        
        mock_handler = MagicMock()
        mock_config_manager = MagicMock()
        
        with pytest.raises(ValueError):
            EnrichmentServiceFactory.get_service("unknown_type", mock_handler, mock_config_manager)

    def test_enrichment_factory_case_insensitive(self) -> None:
        """Test factory handles enrichment types case-insensitively."""
        from feature.enrichment.factory import EnrichmentServiceFactory
        from feature.enrichment.description_service import DescriptionService
        
        mock_handler = MagicMock()
        mock_config_manager = MagicMock()
        mock_config_manager.get_global_config.return_value = {"default_env": "DEV"}
        
        service = EnrichmentServiceFactory.get_service("DESCRIPTION", mock_handler, mock_config_manager)
        assert isinstance(service, DescriptionService)


class TestDescriptionService:
    """Tests for DescriptionService."""
    
    def test_description_service_import(self) -> None:
        """Test that DescriptionService can be imported."""
        from feature.enrichment.description_service import DescriptionService
        assert DescriptionService is not None
    
    def test_description_service_initialization(self) -> None:
        """Test DescriptionService initialization."""
        from feature.enrichment.description_service import DescriptionService
        
        mock_handler = MagicMock()
        mock_config_manager = MagicMock()
        mock_config_manager.get_global_config.return_value = {"default_env": "DEV"}
        
        service = DescriptionService(mock_handler, mock_config_manager)
        assert service.platform_handler == mock_handler
        assert service.config_manager == mock_config_manager
    
    def test_description_service_enrich_method_exists(self) -> None:
        """Test that enrich method exists."""
        from feature.enrichment.description_service import DescriptionService
        
        mock_handler = MagicMock()
        mock_config_manager = MagicMock()
        mock_config_manager.get_global_config.return_value = {"default_env": "DEV"}
        
        service = DescriptionService(mock_handler, mock_config_manager)
        assert hasattr(service, 'enrich')
        assert callable(service.enrich)


class TestTagService:
    """Tests for TagService."""
    
    def test_tag_service_import(self) -> None:
        """Test that TagService can be imported."""
        from feature.enrichment.tag_service import TagService
        assert TagService is not None
    
    def test_tag_service_initialization(self) -> None:
        """Test TagService initialization."""
        from feature.enrichment.tag_service import TagService
        
        mock_handler = MagicMock()
        mock_config_manager = MagicMock()
        mock_config_manager.get_global_config.return_value = {"default_env": "DEV"}
        
        service = TagService(mock_handler, mock_config_manager)
        assert service.platform_handler == mock_handler
        assert service.config_manager == mock_config_manager
    
    def test_tag_service_enrich_method_exists(self) -> None:
        """Test that enrich method exists."""
        from feature.enrichment.tag_service import TagService
        
        mock_handler = MagicMock()
        mock_config_manager = MagicMock()
        mock_config_manager.get_global_config.return_value = {"default_env": "DEV"}
        
        service = TagService(mock_handler, mock_config_manager)
        assert hasattr(service, 'enrich')
        assert callable(service.enrich)


class TestPropertiesService:
    """Tests for PropertiesService."""
    
    def test_properties_service_import(self) -> None:
        """Test that PropertiesService can be imported."""
        from feature.enrichment.properties_service import PropertiesService
        assert PropertiesService is not None
    
    def test_properties_service_initialization(self) -> None:
        """Test PropertiesService initialization."""
        from feature.enrichment.properties_service import PropertiesService
        
        mock_handler = MagicMock()
        mock_config_manager = MagicMock()
        mock_config_manager.get_global_config.return_value = {"default_env": "DEV"}
        
        service = PropertiesService(mock_handler, mock_config_manager)
        assert service.platform_handler == mock_handler
        assert service.config_manager == mock_config_manager


class TestBaseEnrichmentService:
    """Tests for BaseEnrichmentService."""
    
    def test_base_enrichment_service_import(self) -> None:
        """Test that BaseEnrichmentService can be imported."""
        from feature.enrichment.base_enrichment_service import BaseEnrichmentService
        assert BaseEnrichmentService is not None
    
    def test_base_enrichment_service_is_abstract(self) -> None:
        """Test that BaseEnrichmentService is abstract."""
        from feature.enrichment.base_enrichment_service import BaseEnrichmentService
        from abc import ABC
        
        assert issubclass(BaseEnrichmentService, ABC)
