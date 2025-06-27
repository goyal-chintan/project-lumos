from typing import Any, Dict, Optional
import logging
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass,
    DatasetSnapshotClass,
    SchemaMetadataClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    NumberTypeClass,
    BooleanTypeClass,
    TimeTypeClass,
    OtherSchemaClass,
    DatasetPropertiesClass,
)
from datahub.emitter.mce_builder import make_dataset_urn
from .platform_interface import PlatformHandler

logger = logging.getLogger(__name__)

class DataHubHandler(PlatformHandler):
    """DataHub-specific platform implementation."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.emitter = DatahubRestEmitter(config.get("gms_server", "http://localhost:8080"))
        
    def ingest(self, source: Any, metadata: Optional[Dict[str, Any]] = None) -> bool:
        try:
            # Create dataset URN
            dataset_urn = make_dataset_urn(
                platform=self.config["platform"],
                name=metadata.get("name", "unknown"),
                env=self.config.get("env", "PROD")
            )
            
            # Create schema metadata
            schema_fields = []
            for field in metadata.get("schema", {}).get("fields", []):
                schema_fields.append(self._create_schema_field(field))
                
            schema_metadata = SchemaMetadataClass(
                schemaName=metadata.get("name", "unknown"),
                platform=f"urn:li:dataPlatform:{self.config['platform']}",
                version=0,
                platformSchema=OtherSchemaClass(rawSchema=str(metadata.get("schema", {}))),
                fields=schema_fields,
            )
            
            # Create dataset properties
            dataset_properties = DatasetPropertiesClass(
                name=metadata.get("name", "unknown"),
                description=metadata.get("description", ""),
                customProperties=metadata.get("custom_properties", {})
            )
            
            # Create snapshot
            snapshot = DatasetSnapshotClass(
                urn=dataset_urn,
                aspects=[schema_metadata, dataset_properties]
            )
            
            # Create and emit MCE
            mce = MetadataChangeEventClass(proposedSnapshot=snapshot)
            self.emitter.emit(mce)
            
            return True
            
        except Exception as e:
            logger.error(f"Error during DataHub ingestion: {str(e)}")
            return False
            
    def enrich(self, entity_id: str, metadata: Dict[str, Any]) -> bool:
        try:
            # Implementation for enriching metadata
            return True
        except Exception as e:
            logger.error(f"Error during DataHub enrichment: {str(e)}")
            return False
            
    def get_lineage(self, entity_id: str) -> Dict[str, Any]:
        try:
            # Implementation for getting lineage
            return {}
        except Exception as e:
            logger.error(f"Error getting DataHub lineage: {str(e)}")
            return {}
            
    def add_lineage(self, source_id: str, target_id: str, metadata: Optional[Dict[str, Any]] = None) -> bool:
        try:
            # Implementation for adding lineage
            return True
        except Exception as e:
            logger.error(f"Error adding DataHub lineage: {str(e)}")
            return False
            
    def get_schema(self, entity_id: str) -> Dict[str, Any]:
        try:
            # Implementation for getting schema
            return {}
        except Exception as e:
            logger.error(f"Error getting DataHub schema: {str(e)}")
            return {}
            
    def update_schema(self, entity_id: str, schema: Dict[str, Any]) -> bool:
        try:
            # Implementation for updating schema
            return True
        except Exception as e:
            logger.error(f"Error updating DataHub schema: {str(e)}")
            return False
            
    def _create_schema_field(self, field: Dict[str, Any]) -> SchemaFieldClass:
        """Create a DataHub schema field from field metadata."""
        field_type = self._map_field_type(field.get("type", "string"))
        return SchemaFieldClass(
            fieldPath=field.get("name", ""),
            type=SchemaFieldDataTypeClass(type=field_type),
            nativeDataType=str(field.get("type", "string")),
            description=field.get("description", ""),
            nullable=field.get("nullable", True)
        )
        
    def _map_field_type(self, field_type: str) -> Any:
        """Map field type to DataHub type."""
        type_mapping = {
            "string": StringTypeClass(),
            "integer": NumberTypeClass(),
            "float": NumberTypeClass(),
            "boolean": BooleanTypeClass(),
            "datetime": TimeTypeClass(),
        }
        return type_mapping.get(field_type.lower(), StringTypeClass()) 