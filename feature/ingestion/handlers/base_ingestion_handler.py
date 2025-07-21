import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from datetime import datetime

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaMetadataClass,
)

logger = logging.getLogger(__name__)


class BaseIngestionHandler(ABC):
    """
    Abstract Base Class for all ingestion handlers. Its responsibility is to
    transform source metadata into a MetadataChangeEvent object.
    """

    def __init__(self, config: Dict[str, Any]):
       
        self.source_config = config.get("source", {})
        self.sink_config = config.get("sink", {})

    @abstractmethod
    def _get_schema_fields(self) -> List[SchemaFieldClass]:
        """Handler-specific logic to extract schema fields from the source."""
        pass

    def _get_dataset_properties(self) -> Dict[str, Any]:
        """Creates the dataset properties dictionary."""
        return {
            "name": self.source_config.get("dataset_name"),
            "description": f"Dataset from source: {self.source_config.get('path') or self.source_config.get('collection')}",
            "customProperties": {
                "source_type": self.source_config.get("type"),
                "ingestion_timestamp": datetime.utcnow().isoformat(),
            },
        }

    def _get_raw_schema(self) -> str:
        """Returns the raw schema as a string (optional)."""
        return ""

    def _build_mce(
        self,
        platform: str,
        dataset_name: str,
        env: str,
        schema_fields: List[SchemaFieldClass],
        dataset_properties: Dict[str, Any],
        raw_schema: str = "",
    ) -> Optional[MetadataChangeEventClass]:
        """
        A standardized helper method to build the Metadata Change Event (MCE).
        It no longer emits the MCE, but returns it.
        """
        try:
            dataset_urn = make_dataset_urn(platform, dataset_name, env)
            schema_metadata = SchemaMetadataClass(
                schemaName=dataset_name,
                platform=f"urn:li:dataPlatform:{platform}",
                version=0, hash="",
                platformSchema=OtherSchemaClass(rawSchema=raw_schema),
                fields=schema_fields,
            )
            dataset_properties_aspect = DatasetPropertiesClass(**dataset_properties)
            snapshot = DatasetSnapshotClass(
                urn=dataset_urn, aspects=[schema_metadata, dataset_properties_aspect]
            )
            return MetadataChangeEventClass(proposedSnapshot=snapshot)
        except Exception as e:
            logger.error(f"Failed to build MCE for {dataset_name}: {e}", exc_info=True)
            return None

    def ingest(self) -> Optional[MetadataChangeEventClass]:
        """
        Main ingestion method that orchestrates the MCE creation process.
        It now returns the MCE to the caller (IngestionService).
        """
        platform = self.source_config.get("type")
        dataset_name = self.source_config.get("dataset_name")
        env = self.sink_config.get("env", "PROD")

        if not dataset_name:
            logger.error("dataset_name not found in source config. Cannot create MCE.")
            return None

        schema_fields = self._get_schema_fields()
        dataset_properties = self._get_dataset_properties()
        raw_schema = self._get_raw_schema()

        return self._build_mce(
            platform=platform,
            dataset_name=dataset_name,
            env=env,
            schema_fields=schema_fields,
            dataset_properties=dataset_properties,
            raw_schema=raw_schema,
        )
