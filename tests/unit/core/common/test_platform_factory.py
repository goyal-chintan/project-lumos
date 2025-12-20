"""Unit tests for platform factory."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch


def test_platform_factory_import() -> None:
    """Test that PlatformFactory can be imported."""
    from core.platform.factory import PlatformFactory
    assert PlatformFactory is not None


def test_platform_factory_get_instance_datahub(tmp_path) -> None:
    """Test factory creates DataHub handler for 'datahub' platform."""
    from core.platform.factory import PlatformFactory
    from core.platform.impl.datahub_handler import DataHubHandler
    from core.common.config_manager import ConfigManager
    
    # Create a temp config file
    config_file = tmp_path / "global_settings.yaml"
    config_file.write_text("datahub:\n  gms_server: http://localhost:8080\n  test_mode: true\n")
    
    config_manager = ConfigManager(base_config_dir=str(tmp_path))
    
    # Clear the instances cache to ensure a fresh handler is created
    PlatformFactory._instances.clear()
    
    handler = PlatformFactory.get_instance("datahub", config_manager)
    assert isinstance(handler, DataHubHandler)


def test_platform_factory_caches_instance(tmp_path) -> None:
    """Test factory caches handler instances."""
    from core.platform.factory import PlatformFactory
    from core.common.config_manager import ConfigManager
    
    config_file = tmp_path / "global_settings.yaml"
    config_file.write_text("datahub:\n  gms_server: http://localhost:8080\n  test_mode: true\n")
    
    config_manager = ConfigManager(base_config_dir=str(tmp_path))
    
    PlatformFactory._instances.clear()
    
    handler1 = PlatformFactory.get_instance("datahub", config_manager)
    handler2 = PlatformFactory.get_instance("datahub", config_manager)
    
    assert handler1 is handler2


def test_platform_factory_unknown_platform_raises_error(tmp_path) -> None:
    """Test factory raises error for unknown platform."""
    from core.platform.factory import PlatformFactory
    from core.common.config_manager import ConfigManager
    
    config_file = tmp_path / "global_settings.yaml"
    config_file.write_text("unknown:\n  server: http://localhost\n")
    
    config_manager = ConfigManager(base_config_dir=str(tmp_path))
    
    PlatformFactory._instances.clear()
    
    with pytest.raises(ValueError):
        PlatformFactory.get_instance("unknown_platform", config_manager)


def test_platform_factory_missing_config_raises_error(tmp_path) -> None:
    """Test factory raises error when platform config is missing."""
    from core.platform.factory import PlatformFactory
    from core.common.config_manager import ConfigManager
    
    config_file = tmp_path / "global_settings.yaml"
    config_file.write_text("other:\n  setting: value\n")
    
    config_manager = ConfigManager(base_config_dir=str(tmp_path))
    
    PlatformFactory._instances.clear()
    
    with pytest.raises(ValueError):
        PlatformFactory.get_instance("datahub", config_manager)


class TestDataHubHandler:
    """Tests for DataHubHandler."""
    
    def test_datahub_handler_import(self) -> None:
        """Test that DataHubHandler can be imported."""
        from core.platform.impl.datahub_handler import DataHubHandler
        assert DataHubHandler is not None
    
    def test_datahub_handler_test_mode_initialization(self) -> None:
        """Test DataHubHandler initialization in test mode."""
        from core.platform.impl.datahub_handler import DataHubHandler
        
        config = {
            "gms_server": "http://localhost:8080",
            "test_mode": True
        }
        
        handler = DataHubHandler(config)
        assert handler.test_mode is True
        assert handler._emitter is None
    
    def test_datahub_handler_requires_gms_server_when_not_test_mode(self) -> None:
        """Test DataHubHandler requires gms_server when not in test mode."""
        from core.platform.impl.datahub_handler import DataHubHandler
        
        config = {
            "test_mode": False
        }
        
        with pytest.raises(ValueError):
            DataHubHandler(config)
    
    def test_datahub_handler_emit_mce_test_mode(self) -> None:
        """Test emit_mce in test mode logs instead of emitting."""
        from core.platform.impl.datahub_handler import DataHubHandler
        
        config = {
            "gms_server": "http://localhost:8080",
            "test_mode": True
        }
        
        handler = DataHubHandler(config)
        
        # Create a mock MCE
        mock_mce = MagicMock()
        mock_mce.proposedSnapshot.urn = "urn:li:dataset:(urn:li:dataPlatform:csv,test,DEV)"
        mock_mce.proposedSnapshot.aspects = [MagicMock(schemaName="test")]
        
        # Should not raise in test mode
        handler.emit_mce(mock_mce)
    
    def test_datahub_handler_emit_mcp_test_mode(self) -> None:
        """Test emit_mcp in test mode logs instead of emitting."""
        from core.platform.impl.datahub_handler import DataHubHandler
        
        config = {
            "gms_server": "http://localhost:8080",
            "test_mode": True
        }
        
        handler = DataHubHandler(config)
        
        # Create a mock MCP
        mock_mcp = MagicMock()
        mock_mcp.entityUrn = "urn:li:dataset:(urn:li:dataPlatform:csv,test,DEV)"
        mock_mcp.aspect = MagicMock()
        mock_mcp.aspect.__class__.__name__ = "TestAspect"
        
        # Should not raise in test mode
        handler.emit_mcp(mock_mcp)
    
    def test_datahub_handler_add_lineage_test_mode(self) -> None:
        """Test add_lineage in test mode."""
        from core.platform.impl.datahub_handler import DataHubHandler
        
        config = {
            "gms_server": "http://localhost:8080",
            "test_mode": True
        }
        
        handler = DataHubHandler(config)
        
        upstream = "urn:li:dataset:(urn:li:dataPlatform:csv,source,DEV)"
        downstream = "urn:li:dataset:(urn:li:dataPlatform:csv,target,DEV)"
        
        # Should return True in test mode
        result = handler.add_lineage(upstream, downstream)
        assert result is True


class TestMetadataPlatformInterface:
    """Tests for MetadataPlatformInterface."""
    
    def test_interface_import(self) -> None:
        """Test that MetadataPlatformInterface can be imported."""
        from core.platform.interface import MetadataPlatformInterface
        assert MetadataPlatformInterface is not None
    
    def test_interface_is_abstract(self) -> None:
        """Test that MetadataPlatformInterface is abstract."""
        from core.platform.interface import MetadataPlatformInterface
        from abc import ABC
        
        assert issubclass(MetadataPlatformInterface, ABC)
