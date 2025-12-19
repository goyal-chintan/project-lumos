# feature/enrichment/description_service.py
import logging
from typing import Dict, Any
from .base_enrichment_service import BaseEnrichmentService
from datahub.emitter.mcp_patch_builder import MetadataPatchProposal

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

            test_mode = bool(getattr(self.platform_handler, "test_mode", False))
            if config.get("dry_run") or test_mode:
                logger.info(f"DRY_RUN/TEST_MODE: would submit description update for URN: {target_urn}")
                return True

            # Use PATCH semantics to avoid overwriting other DatasetProperties fields (e.g. customProperties).
            patch = MetadataPatchProposal(urn=target_urn)
            patch._add_patch("datasetProperties", "add", ("description",), description)
            for mcp in patch.build():
                self.platform_handler.emit_mcp(mcp)
            logger.info(f"Successfully submitted description update for URN: {target_urn}")
            return True
        except Exception as e:
            logger.error(f"Failed to add description for URN {target_urn}: {e}")
            return False