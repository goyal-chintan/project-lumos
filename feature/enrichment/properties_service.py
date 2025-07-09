import logging
from typing import Dict, Any
from platform_services.metadata_platform_interface import MetadataPlatformInterface
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import DatasetPropertiesClass

logger = logging.getLogger(__name__)

class PropertiesService:
    """Service to manage dataset properties."""

    def __init__(self, platform_handler: MetadataPlatformInterface):
        self.platform_handler = platform_handler

    def update_properties(self, urn: str, config: Dict[str, Any]) -> bool:
        """
        Updates the properties of a dataset.
        
        Args:
            urn: The URN of the dataset to update.
            config: A dictionary containing the new name, description, and custom properties.
        """
        try:
            properties_aspect = DatasetPropertiesClass(
                name=config.get("name"),
                description=config.get("description"),
                customProperties=config.get("custom_properties")
            )
            
            mcp = MetadataChangeProposalWrapper(entityUrn=urn, aspect=properties_aspect)
            self.platform_handler.emit_mcp(mcp)
            
            logger.info(f"Successfully submitted properties update for URN: {urn}")
            return True
        except Exception as e:
            logger.error(f"Failed to update properties for URN {urn}: {e}")
            return False