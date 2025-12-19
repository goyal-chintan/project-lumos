# feature/enrichment/tag_service.py
import logging
from typing import Dict, Any
from .base_enrichment_service import BaseEnrichmentService
from core.common.utils import sanitize_entity_id
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import GlobalTagsClass, TagAssociationClass

logger = logging.getLogger(__name__)

class TagService(BaseEnrichmentService):
    """Service to add tags to datasets."""

    def __init__(self, platform_handler: Any, config_manager: Any):
        super().__init__(platform_handler, config_manager)

    def enrich(self, config: Dict[str, Any]) -> bool:
        """
        Adds tags to a dataset.
        """
        target_urn = None
        try:
            target_urn = self._build_urn(config["data_type"], config["dataset_name"])
            tags = config.get("tags", [])

            tag_associations = [TagAssociationClass(tag=f"urn:li:tag:{sanitize_entity_id(tag)}") for tag in tags]
            tags_aspect = GlobalTagsClass(tags=tag_associations)

            mcp = MetadataChangeProposalWrapper(entityUrn=target_urn, aspect=tags_aspect)

            test_mode = bool(getattr(self.platform_handler, "test_mode", False))
            if config.get("dry_run") or test_mode:
                logger.info(f"DRY_RUN/TEST_MODE: would submit tags update for URN: {target_urn}")
                return True

            self.platform_handler.emit_mcp(mcp)
            logger.info(f"Successfully added tags to URN: {target_urn}")
            return True
        except Exception as e:
            logger.error(f"Failed to add tags to URN {target_urn}: {e}")
            return False