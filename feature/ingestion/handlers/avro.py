import json
import logging
from typing import List

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
    """Handler for Avro file ingestion."""

    def __init__(self, config, platform_handler):
        super().__init__(config, platform_handler)
        self.avro_schema = None  # Cache the schema to avoid re-reading the file

    def _get_avro_schema(self):
        """Helper to read and cache the Avro schema from the file."""
        if self.avro_schema:
            return self.avro_schema

        file_path = self.source_config["path"]
        logger.info(f"Reading Avro schema from {file_path}")
        with open(file_path, "rb") as fo:
            avro_reader = reader(fo)
            self.avro_schema = avro_reader.writer_schema
        return self.avro_schema

    def _get_schema_fields(self) -> List[SchemaFieldClass]:
        """Extracts schema fields from the Avro file."""
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
            dtype = field["type"]
            if isinstance(dtype, list):  # Handle nullable fields like ["null", "string"]
                dtype = next((t for t in dtype if t != "null"), "string")

            schema_fields.append(
                SchemaFieldClass(
                    fieldPath=field["name"],
                    nativeDataType=str(field["type"]),
                    type=SchemaFieldDataTypeClass(
                        type=type_mapping.get(dtype, StringTypeClass())
                    ),
                )
            )
        return schema_fields

    def _get_raw_schema(self) -> str:
        """Returns the full Avro schema as a JSON string."""
        avro_schema = self._get_avro_schema()
        return json.dumps(avro_schema, indent=2)
