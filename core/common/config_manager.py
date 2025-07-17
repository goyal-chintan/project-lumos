import yaml
from typing import Any, Dict, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class ConfigManager:
    """
    Manages loading YAML configuration for the framework.
    SRP: Its single responsibility is to load and cache configurations.
    """
    
    def __init__(self, base_config_dir: str = "configs"):
        self.base_config_dir = Path(base_config_dir)
        self._config_cache: Dict[str, Any] = {}
        
    def load_config(self, folder_path_str: str) -> Dict[str, Any]:
        """
        Load a configuration file by its relative or absolute path.
        Caches the loaded configuration to avoid redundant file I/O.
        """
        folder_path = Path(folder_path_str)
        absolute_path_str = str(folder_path.resolve())

        if absolute_path_str in self._config_cache:
            return self._config_cache[absolute_path_str]
            
        if not folder_path.exists():
            logger.error(f"Configuration file not found at: {folder_path_str}")
            return {}

        try:
            with open(folder_path, 'r') as f:
                config = yaml.safe_load(f)
                if not isinstance(config, dict):
                    logger.error(f"Config file {folder_path_str} is not a valid dictionary.")
                    return {}
                self._config_cache[absolute_path_str] = config
                logger.info(f"Successfully loaded configuration from {folder_path_str}")
                return config
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file {folder_path_str}: {e}")
            return {}
        except Exception as e:
            logger.error(f"Error loading config {folder_path_str}: {e}")
            return {}
            
    def get_global_config(self) -> Dict[str, Any]:
        """Get the global framework configuration."""
        global_folder_path = self.base_config_dir / "global_settings.yaml"
        return self.load_config(str(global_folder_path))