import logging
from typing import Any, Dict
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import UpstreamClass, UpstreamLineageClass, DatasetLineageTypeClass
from datahub.emitter.mce_builder import make_dataset_urn

from ..interface import MetadataPlatformInterface

logger = logging.getLogger(__name__)

class DataHubHandler(MetadataPlatformInterface):
    """
    DataHub-specific implementation of the MetadataPlatformInterface.
    SRP: Its single responsibility is to handle all communication with a DataHub instance.
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        gms_server = self.config.get("gms_server")
        if not gms_server:
            raise ValueError("DataHub configuration requires 'gms_server'.")
        self._emitter = DatahubRestEmitter(gms_server=gms_server)
        logger.info(f"DataHubHandler initialized for GMS server at {gms_server}")

    def emit_mce(self, mce: Any) -> None:
        """Emits a Metadata Change Event to DataHub."""
        try:
            self._emitter.emit(mce)
            logger.info(f"Successfully emitted MCE to DataHub for URN: {mce.proposedSnapshot.urn}")
        except Exception as e:
            logger.error(f"Failed to emit MCE to DataHub: {e}")
            raise

    def emit_mcp(self, mcp: Any) -> None:
        """Emits a Metadata Change Proposal to DataHub."""
        try:
            self._emitter.emit_mcp(mcp)
            logger.info(f"Successfully emitted MCP to DataHub for URN: {mcp.entityUrn}")
        except Exception as e:
            logger.error(f"Failed to emit MCP to DataHub: {e}")
            raise
    
    def add_lineage(self, upstream_urn: str, downstream_urn: str) -> bool:
        """Adds dataset lineage to DataHub."""
        try:
            lineage_mcp = MetadataChangeProposalWrapper(
                entityUrn=downstream_urn,
                aspect=UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=upstream_urn,
                            type=DatasetLineageTypeClass.TRANSFORMED,
                        )
                    ]
                ),
            )
            self.emit_mcp(lineage_mcp)
            logger.info(f"Successfully added lineage: {upstream_urn} -> {downstream_urn}")
            return True
        except Exception as e:
            logger.error(f"Failed to add lineage to DataHub: {e}")
            return False