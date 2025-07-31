# feature/enrichment/factory.py
from typing import Dict, Any, Type
from .base_enrichment_service import BaseEnrichmentService
from .description_service import DescriptionService
from .documentation_service import DocumentationService
from .properties_service import PropertiesService
from .tag_service import TagService
from core.common.config_manager import ConfigManager

class EnrichmentServiceFactory:
    """
    Factory to create the appropriate enrichment service based on configuration.
    """
    _service_registry: Dict[str, Type[BaseEnrichmentService]] = {
        "description": DescriptionService,
        "documentation": DocumentationService,
        "properties": PropertiesService,
        "tags": TagService,
    }

    @staticmethod
    def get_service(enrichment_type: str, platform_handler: Any, config_manager: ConfigManager) -> BaseEnrichmentService:
        """
        Returns an instance of the correct enrichment service.
        """
        service_class = EnrichmentServiceFactory._service_registry.get(enrichment_type.lower())
        if not service_class:
            raise ValueError(f"Unsupported enrichment type: {enrichment_type}")
        return service_class(platform_handler, config_manager)