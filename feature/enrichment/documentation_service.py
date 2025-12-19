# feature/enrichment/documentation_service.py
import logging
from typing import Dict, Any
from datetime import datetime
from .base_enrichment_service import BaseEnrichmentService
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import AuditStampClass, InstitutionalMemoryClass, InstitutionalMemoryMetadataClass

logger = logging.getLogger(__name__)

class DocumentationService(BaseEnrichmentService):
    """Service to attach documentation links."""

    def __init__(self, platform_handler: Any, config_manager: Any):
        super().__init__(platform_handler, config_manager)

    def enrich(self, config: Dict[str, Any]) -> bool:
        """
        Attaches a documentation link to a dataset.
        """
        target_urn = None
        try:
            target_urn = self._build_urn(config["data_type"], config["dataset_name"])
            doc_url = config["doc_url"]
            description = config.get("description", "Documentation")

            institutional_memory_element = InstitutionalMemoryMetadataClass(
                url=doc_url,
                description=description,
                createStamp=AuditStampClass(
                    time=int(datetime.utcnow().timestamp() * 1000),
                    actor="urn:li:corpuser:ingestion",
                ),
            )
            memory_aspect = InstitutionalMemoryClass(elements=[institutional_memory_element])

            mcp = MetadataChangeProposalWrapper(entityUrn=target_urn, aspect=memory_aspect)

            test_mode = bool(getattr(self.platform_handler, "test_mode", False))
            if config.get("dry_run") or test_mode:
                logger.info(f"DRY_RUN/TEST_MODE: would attach documentation to URN: {target_urn}")
                return True

            self.platform_handler.emit_mcp(mcp)
            logger.info(f"Successfully attached documentation to URN: {target_urn}")
            return True
        except Exception as e:
            logger.error(f"Failed to attach documentation to URN {target_urn}: {e}")
            return False