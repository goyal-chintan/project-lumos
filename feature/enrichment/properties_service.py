# feature/enrichment/properties_service.py
import logging
from typing import Dict, Any
from .base_enrichment_service import BaseEnrichmentService
from datahub.emitter.mcp_patch_builder import MetadataPatchProposal

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

            test_mode = bool(getattr(self.platform_handler, "test_mode", False))
            if config.get("dry_run") or test_mode:
                logger.info(f"DRY_RUN/TEST_MODE: would submit properties update for URN: {target_urn}")
                return True

            # Use patch-style updates to avoid clearing other DatasetProperties fields.
            patch = MetadataPatchProposal(urn=target_urn)

            if "name" in config and config.get("name") is not None:
                patch._add_patch("datasetProperties", "add", ("name",), config.get("name"))

            if "description" in config and config.get("description") is not None:
                patch._add_patch("datasetProperties", "add", ("description",), config.get("description"))

            custom_props = config.get("custom_properties") or {}
            if not isinstance(custom_props, dict):
                raise ValueError("'custom_properties' must be a dict of str->str")

            for k, v in custom_props.items():
                patch._add_patch("datasetProperties", "add", ("customProperties", str(k)), str(v))

            for mcp in patch.build():
                self.platform_handler.emit_mcp(mcp)
            
            logger.info(f"Successfully submitted properties update for URN: {target_urn}")
            return True
        except Exception as e:
            logger.error(f"Failed to update properties for URN {target_urn}: {e}")
            return False