"""Unit tests for ConfigManager."""
import pytest
from pathlib import Path
from core_library.common.config_manager import ConfigManager


class TestConfigManager:
    """Test cases for ConfigManager class."""
    
    def test_init_default_config_dir(self):
        """Test ConfigManager initialization with default config directory."""
        config_manager = ConfigManager()
        assert config_manager.base_config_dir == Path("configs")
    
    def test_init_custom_config_dir(self):
        """Test ConfigManager initialization with custom config directory."""
        custom_dir = "custom_configs"
        config_manager = ConfigManager(base_config_dir=custom_dir)
        assert config_manager.base_config_dir == Path(custom_dir)
    
    def test_load_nonexistent_config(self):
        """Test loading a non-existent configuration file."""
        config_manager = ConfigManager()
        config = config_manager.load_config("nonexistent.yaml")
        assert config == {}
    
    def test_load_config_caching(self):
        """Test that loaded configurations are cached."""
        config_manager = ConfigManager()
        # Load the same config twice
        config1 = config_manager.load_config("configs/global_settings.yaml")
        config2 = config_manager.load_config("configs/global_settings.yaml")
        # Should be the same object (cached)
        assert config1 is config2
    
    def test_get_global_config(self):
        """Test getting global configuration."""
        config_manager = ConfigManager()
        global_config = config_manager.get_global_config()
        assert isinstance(global_config, dict)
        # Should contain datahub config
        if global_config:
            assert "datahub" in global_config or "default_env" in global_config

