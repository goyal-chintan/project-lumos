"""Unit tests for PlatformFactory."""
import pytest
from platform_services.platform_factory import PlatformFactory
from platform_services.metadata_platform_interface import MetadataPlatformInterface


class TestPlatformFactory:
    """Test cases for PlatformFactory class."""
    
    def setup_method(self):
        """Clear singleton instances before each test."""
        PlatformFactory._instances.clear()
    
    def test_get_datahub_instance(self, mock_datahub_config):
        """Test getting DataHub platform instance."""
        instance = PlatformFactory.get_instance("datahub", mock_datahub_config)
        assert instance is not None
        assert isinstance(instance, MetadataPlatformInterface)
    
    def test_singleton_pattern(self, mock_datahub_config):
        """Test that factory returns the same instance for the same platform."""
        instance1 = PlatformFactory.get_instance("datahub", mock_datahub_config)
        instance2 = PlatformFactory.get_instance("datahub", mock_datahub_config)
        assert instance1 is instance2
    
    def test_unsupported_platform(self):
        """Test that factory raises error for unsupported platform."""
        with pytest.raises(ValueError, match="Unsupported data catalog platform"):
            PlatformFactory.get_instance("unsupported_platform", {})
    
    def test_case_insensitive_platform_name(self, mock_datahub_config):
        """Test that platform names are case-insensitive."""
        instance1 = PlatformFactory.get_instance("DataHub", mock_datahub_config)
        instance2 = PlatformFactory.get_instance("datahub", mock_datahub_config)
        assert instance1 is instance2

