# feature/enrichment/base_enrichment_service.py
from abc import ABC, abstractmethod
from typing import Dict, Any
from core.platform.interface import MetadataPlatformInterface
from core.common.config_manager import ConfigManager
from datahub.emitter.mce_builder import make_dataset_urn

class BaseEnrichmentService(ABC):
    """
    Abstract base class for all enrichment services.
    """

    def __init__(self, platform_handler: MetadataPlatformInterface, config_manager: ConfigManager):
        self.platform_handler = platform_handler
        self.config_manager = config_manager
        self.env = self.config_manager.get_global_config().get("default_env", "PROD")

    def _build_urn(self, data_type: str, dataset_name: str) -> str:
        """Builds a full dataset URN from a platform and name."""
        if not all([data_type, dataset_name]):
            raise ValueError("Both 'data_type' and 'dataset_name' must be provided in the config.")
        return make_dataset_urn(data_type, dataset_name, self.env)

    @abstractmethod
    def enrich(self, config: Dict[str, Any]) -> bool:
        """
        Applies the enrichment to the target entity.
        """
        pass