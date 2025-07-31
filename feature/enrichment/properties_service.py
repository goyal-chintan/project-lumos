# feature/enrichment/properties_service.py
import logging
from typing import Dict, Any
from .base_enrichment_service import BaseEnrichmentService
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import DatasetPropertiesClass

logger = logging.getLogger(__name__)

class PropertiesService(BaseEnrichmentService):
    """Service to manage dataset properties."""

    def __init__(self, platform_handler: Any, config_manager: Any):
        super().__init__(platform_handler, config_manager)

    def enrich(self, config: Dict[str, Any]) -> bool:
        """
        Updates the properties of a dataset.
        """
        target_urn = None
        try:
            target_urn = self._build_urn(config["data_type"], config["dataset_name"])
            
            properties_aspect = DatasetPropertiesClass(
                name=config.get("name"),
                description=config.get("description"),
                customProperties=config.get("custom_properties")
            )
            
            mcp = MetadataChangeProposalWrapper(entityUrn=target_urn, aspect=properties_aspect)
            self.platform_handler.emit_mcp(mcp)
            
            logger.info(f"Successfully submitted properties update for URN: {target_urn}")
            return True
        except Exception as e:
            logger.error(f"Failed to update properties for URN {target_urn}: {e}")
            return False