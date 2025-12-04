import json
import logging
from typing import List, Dict, Any

from fastavro import reader
from .base_ingestion_handler import BaseIngestionHandler
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    NumberTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
)

logger = logging.getLogger(__name__)


class AvroIngestionHandler(BaseIngestionHandler):
    """
    Handler for Avro file ingestion.
    Note: Avro files are self-describing; the schema is always read from the file itself.
    The 'infer_schema' flag is ignored for this handler.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.avro_schema = None  # Cache the schema to avoid re-reading the file

    def _get_avro_schema(self):
        """Helper to read and cache the Avro schema from the file."""
        if self.avro_schema:
            return self.avro_schema

        file_path = self.source_config.get("source_path") or self.source_config.get("path")
        if not file_path:
            raise ValueError("No file path specified in source configuration")
            
        logger.info(f"Reading Avro schema from {file_path}")
        try:
            with open(file_path, "rb") as fo:
                avro_reader = reader(fo)
                self.avro_schema = avro_reader.writer_schema
        except FileNotFoundError:
            logger.error(f"Avro file not found at {file_path}")
            raise
        except Exception as e:
            logger.error(f"Failed to read Avro schema from {file_path}: {e}")
            raise
            
        return self.avro_schema

    def _get_schema_fields(self) -> List[SchemaFieldClass]:
        """Extracts schema fields from the Avro file."""
        # For Avro, the schema is always "inferred" from the file content.
        # The `infer_schema` config flag is therefore not applicable.
        avro_schema = self._get_avro_schema()
        schema_fields = []
        type_mapping = {
            "string": StringTypeClass(),
            "int": NumberTypeClass(),
            "long": NumberTypeClass(),
            "float": NumberTypeClass(),
            "double": NumberTypeClass(),
            "boolean": BooleanTypeClass(),
        }

        for field in avro_schema.get("fields", []):
            field_name = field["name"]
            field_type = field["type"]
            
            # Extract the actual type and determine if it's nullable
            dtype, is_nullable = self._extract_field_type(field_type)
            
            # Get the appropriate DataHub type
            datahub_type = type_mapping.get(dtype, StringTypeClass())
            
            schema_fields.append(
                SchemaFieldClass(
                    fieldPath=field_name,
                    nativeDataType=str(field_type),
                    type=SchemaFieldDataTypeClass(type=datahub_type),
                    nullable=is_nullable,
                    recursive=False,
                    isPartOfKey=False,
                )
            )
        return schema_fields
    
    def _extract_field_type(self, field_type):
        """
        Extracts the actual field type and nullability from Avro field type definition.
        Handles simple types, union types, and complex types.
        """
        is_nullable = False
        
        if isinstance(field_type, str):
            # Simple type like "string", "int"
            return field_type, False
            
        elif isinstance(field_type, list):
            # Union type like ["null", "string"] or ["string", "null"]
            non_null_types = [t for t in field_type if t != "null"]
            if "null" in field_type:
                is_nullable = True
            
            if non_null_types:
                # Take the first non-null type
                actual_type = non_null_types[0]
                if isinstance(actual_type, str):
                    return actual_type, is_nullable
                else:
                    # Complex type in union
                    return self._get_type_from_complex(actual_type), is_nullable
            else:
                return "string", True  # Default fallback
                
        elif isinstance(field_type, dict):
            # Complex type like {"type": "record", ...} or {"type": "array", ...}
            return self._get_type_from_complex(field_type), is_nullable
            
        else:
            # Unknown type, fallback to string
            logger.warning(f"Unknown Avro field type: {field_type}, defaulting to string")
            return "string", False
    
    def _get_type_from_complex(self, type_def):
        """
        Extracts type from complex Avro type definitions.
        """
        if isinstance(type_def, dict):
            avro_type = type_def.get("type", "unknown")
            
            if avro_type in ["record", "enum"]:
                return "string"  # Treat complex records/enums as strings in DataHub
            elif avro_type == "array":
                return "string"  # Treat arrays as strings (could be enhanced to show array type)
            elif avro_type == "map":
                return "string"  # Treat maps as strings
            elif avro_type in ["string", "int", "long", "float", "double", "boolean"]:
                return avro_type
            else:
                logger.warning(f"Unknown complex Avro type: {avro_type}, defaulting to string")
                return "string"
        else:
            return "string"

    def _get_raw_schema(self) -> str:
        """Returns the full Avro schema as a JSON string."""
        avro_schema = self._get_avro_schema()
        return json.dumps(avro_schema, indent=2)
