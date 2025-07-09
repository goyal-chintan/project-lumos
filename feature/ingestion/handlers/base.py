# feature/ingestion/handlers/base.py
from abc import ABC, abstractmethod
from typing import Any, Dict, List
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
    This defines the contract for ingestion (LSP) and centralizes common logic (DRY).
    """
    
    def __init__(self, config: Dict[str, Any], platform_handler: MetadataPlatformInterface):
        self.source_config = config.get("source", {})
        self.sink_config = config.get("sink", {})
        self.platform_handler = platform_handler
        # Handlers must define their own required fields.
        self.required_fields = ["type"]
        
    def validate_config(self) -> bool:
        """Validate the handler's source configuration."""
        is_valid = all(field in self.source_config for field in self.required_fields)
        if not is_valid:
            missing = [f for f in self.required_fields if f not in self.source_config]
            logger.error(f"Invalid config for {self.__class__.__name__}. Missing required fields: {missing}")
        return is_valid
        
    @abstractmethod
    def ingest(self) -> None:
        """
        Main ingestion method.
        This method should orchestrate the extraction, transformation, and 
        emission of metadata for a specific source.
        """
        pass

    def _build_and_emit_mce(
        self, 
        platform: str, 
        dataset_name: str, 
        env: str, 
        schema_fields: List, 
        dataset_properties: Dict[str, Any],
        raw_schema: str = ""
    ):
        """
        A standardized helper method to build and emit the Metadata Change Event (MCE).
        This promotes DRY (Don't Repeat Yourself) by handling the boilerplate MCE creation.
        """
        try:
            # 1. Create URN
            dataset_urn = make_dataset_urn(platform, dataset_name, env)

            # 2. Create SchemaMetadata aspect
            schema_metadata = SchemaMetadataClass(
                schemaName=dataset_name,
                platform=f"urn:li:dataPlatform:{platform}",
                version=0, hash="",
                platformSchema=OtherSchemaClass(rawSchema=raw_schema),
                fields=schema_fields
            )

            # 3. Create DatasetProperties aspect
            dataset_properties_aspect = DatasetPropertiesClass(**dataset_properties)

            # 4. Create DatasetSnapshot
            snapshot = DatasetSnapshotClass(
                urn=dataset_urn,
                aspects=[schema_metadata, dataset_properties_aspect]
            )

            # 5. Create MCE and emit
            mce = MetadataChangeEventClass(proposedSnapshot=snapshot)
            self.platform_handler.emit_mce(mce)

            logger.info(f"Successfully emitted MCE for dataset: {dataset_urn}")
        except Exception as e:
            logger.error(f"Failed to build and emit MCE for {dataset_name}: {e}", exc_info=True)
            # Re-raise to allow the service layer to handle the failure
            raise
