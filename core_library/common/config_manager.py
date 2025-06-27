import os
import yaml
from typing import Any, Dict, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class ConfigManager:
    """Manages configuration for the framework."""
    
    def __init__(self, config_dir: str = "configs"):
        self.config_dir = Path(config_dir)
        self._config_cache: Dict[str, Any] = {}
        
    def load_config(self, config_name: str) -> Dict[str, Any]:
        """Load a configuration file by name."""
        if config_name in self._config_cache:
            return self._config_cache[config_name]
            
        config_path = self.config_dir / f"{config_name}.yaml"
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                self._config_cache[config_name] = config
                return config
        except Exception as e:
            logger.error(f"Error loading config {config_name}: {str(e)}")
            return {}
            
    def get_platform_config(self, platform: str) -> Dict[str, Any]:
        """Get platform-specific configuration."""
        return self.load_config(f"platforms/{platform}")
        
    def get_ingestion_config(self, source_type: str) -> Dict[str, Any]:
        """Get ingestion configuration for a source type."""
        return self.load_config(f"ingestion/{source_type}")
        
    def get_enrichment_config(self, enrichment_type: str) -> Dict[str, Any]:
        """Get enrichment configuration."""
        return self.load_config(f"enrichment/{enrichment_type}")
        
    def update_config(self, config_name: str, config_data: Dict[str, Any]) -> bool:
        """Update a configuration file."""
        try:
            config_path = self.config_dir / f"{config_name}.yaml"
            with open(config_path, 'w') as f:
                yaml.dump(config_data, f)
            self._config_cache[config_name] = config_data
            return True
        except Exception as e:
            logger.error(f"Error updating config {config_name}: {str(e)}")
            return False
            
    def get_env_config(self) -> Dict[str, Any]:
        """Get environment-specific configuration."""
        env = os.getenv("ENVIRONMENT", "development")
        return self.load_config(f"environments/{env}")
        
    def get_global_config(self) -> Dict[str, Any]:
        """Get global configuration."""
        return self.load_config("global") 