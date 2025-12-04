# feature/enrichment/description_service.py
import logging
from typing import Dict, Any
from .base_enrichment_service import BaseEnrichmentService
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import DatasetPropertiesClass

logger = logging.getLogger(__name__)

class DescriptionService(BaseEnrichmentService):
    """Service to add descriptions to datasets."""

    def __init__(self, platform_handler: Any, config_manager: Any):
        super().__init__(platform_handler, config_manager)

    def enrich(self, config: Dict[str, Any]) -> bool:
        """
        Adds a description to a dataset.
        """
        target_urn = None
        try:
            target_urn = self._build_urn(config["data_type"], config["dataset_name"])
            description = config["description"]

            properties_aspect = DatasetPropertiesClass(description=description)
            mcp = MetadataChangeProposalWrapper(entityUrn=target_urn, aspect=properties_aspect)

            self.platform_handler.emit_mcp(mcp)
            logger.info(f"Successfully submitted description update for URN: {target_urn}")
            return True
        except Exception as e:
            logger.error(f"Failed to add description for URN {target_urn}: {e}")
            return False