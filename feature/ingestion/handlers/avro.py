# feature/ingestion/handlers/avro.py
import os
import json
import logging
from typing import Dict, Any
from fastavro import reader
from .base import BaseIngestionHandler
from core.platform.interface import MetadataPlatformInterface
from datahub.metadata.schema_classes import (
    SchemaFieldClass, SchemaFieldDataTypeClass, StringTypeClass, NumberTypeClass,
    BooleanTypeClass
)

logger = logging.getLogger(__name__)

class AvroIngestionHandler(BaseIngestionHandler):
    """Handler for single Avro file ingestion."""

    def __init__(self, config: Dict[str, Any], platform_handler: MetadataPlatformInterface):
        super().__init__(config, platform_handler)
        self.required_fields.extend(["path", "dataset_name"])

    def ingest(self) -> None:
        """Ingests metadata from a single Avro file."""
        if not self.validate_config():
            raise ValueError("Avro source config validation failed.")

        file_path = self.source_config["path"]
        dataset_name = self.source_config["dataset_name"]
        env = self.sink_config.get("env", "PROD")
        platform = "avro"

        logger.info(f"Processing Avro file: {file_path}")

        if not os.path.isfile(file_path):
            logger.error(f"Provided path is not a file: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")

        with open(file_path, "rb") as fo:
            avro_reader = reader(fo)
            avro_schema = avro_reader.writer_schema

            # 1. Create Schema Fields (Handler-specific logic)
            schema_fields = []
            type_mapping = {
                "string": StringTypeClass(), "int": NumberTypeClass(), "long": NumberTypeClass(),
                "float": NumberTypeClass(), "double": NumberTypeClass(), "boolean": BooleanTypeClass()
            }
            for field in avro_schema.get("fields", []):
                dtype = field["type"]
                if isinstance(dtype, list): # Handle nullable fields like ["null", "string"]
                    dtype = next((t for t in dtype if t != "null"), "string")

                schema_fields.append(SchemaFieldClass(
                    fieldPath=field["name"],
                    nativeDataType=str(field["type"]),
                    type=SchemaFieldDataTypeClass(type=type_mapping.get(dtype, StringTypeClass()))
                ))

            # 2. Define DatasetProperties (Handler-specific logic)
            dataset_properties = {
                "name": dataset_name,
                "description": f"Dataset ingested from Avro file: {os.path.basename(file_path)}"
            }
            
            # 3. Call the shared method from the base class to build and emit the MCE
            self._build_and_emit_mce(
                platform=platform,
                dataset_name=dataset_name,
                env=env,
                schema_fields=schema_fields,
                dataset_properties=dataset_properties,
                raw_schema=json.dumps(avro_schema, indent=2)
            )
