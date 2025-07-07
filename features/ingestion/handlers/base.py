from abc import ABC, abstractmethod
from typing import Any, Dict
import logging
from core.platform.interface import MetadataPlatformInterface
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass, DatasetSnapshotClass, SchemaMetadataClass,
    OtherSchemaClass, DatasetPropertiesClass
)
from datahub.emitter.mce_builder import make_dataset_urn

logger = logging.getLogger(__name__)

class BaseIngestionHandler(ABC):
    """
    Abstract Base Class for all ingestion handlers.
    LSP: Any subclass of BaseIngestionHandler can be used by IngestionService without issue.
    OCP: The system is extended by creating new subclasses of this handler.
    """
    
    def __init__(self, config: Dict[str, Any], platform_handler: MetadataPlatformInterface):
        self.source_config = config.get("source", {})
        self.sink_config = config.get("sink", {})
        self.platform_handler = platform_handler
        self.required_fields = ["type"]
        
    def validate_config(self) -> bool:
        """Validate the handler's source configuration."""
        is_valid = all(field in self.source_config for field in self.required_fields)
        if not is_valid:
            logger.error(f"Invalid config for handler. Missing required fields: {self.required_fields}")
        return is_valid
        
    @abstractmethod
    def ingest(self) -> None:
        """
        Main ingestion method.
        This method should orchestrate the extraction, transformation, and emission of metadata.
        """
        pass

    # ---- NEW SHARED METHOD ----
    def _build_and_emit_mce(self, platform: str, dataset_name: str, env: str, schema_fields: list, dataset_properties: dict):
        """
        A standardized method to build and emit the Metadata Change Event (MCE).
        """
        # 1. Create Aspects
        dataset_urn = make_dataset_urn(platform, dataset_name, env)

        schema_metadata = SchemaMetadataClass(
            schemaName=dataset_name,
            platform=f"urn:li:dataPlatform:{platform}",
            version=0, hash="",
            platformSchema=OtherSchemaClass(rawSchema=""), # Can be enhanced further
            fields=schema_fields
        )

        dataset_properties_aspect = DatasetPropertiesClass(**dataset_properties)

        # 2. Create DatasetSnapshot
        snapshot = DatasetSnapshotClass(
            urn=dataset_urn,
            aspects=[schema_metadata, dataset_properties_aspect]
        )

        # 3. Create MCE and emit
        mce = MetadataChangeEventClass(proposedSnapshot=snapshot)
        self.platform_handler.emit_mce(mce)

        logger.info(f"Successfully ingested metadata for dataset: {dataset_name}")