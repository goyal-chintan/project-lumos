import logging
from typing import List

import pymongo
from .base_ingestion_handler import BaseIngestionHandler
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    NumberTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    TimeTypeClass,
)

logger = logging.getLogger(__name__)


class MongoIngestionHandler(BaseIngestionHandler):
    """Handler for MongoDB ingestion."""

    def __init__(self, config, platform_handler):
        super().__init__(config, platform_handler)
        # Override required fields for MongoDB, which doesn't use a 'path'
        self.required_fields = ["type", "uri", "database", "collection", "dataset_name"]

    def _get_schema_fields(self) -> List[SchemaFieldClass]:
        """Extracts schema by sampling one document from the collection."""
        uri = self.source_config["uri"]
        database_name = self.source_config["database"]
        collection_name = self.source_config["collection"]

        try:
            client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
            db = client[database_name]
            collection = db[collection_name]
            logger.info(f"Sampling document from {database_name}.{collection_name}")
            sample = collection.find_one()
        except pymongo.errors.ServerSelectionTimeoutError as e:
            logger.error(f"Could not connect to MongoDB at {uri}: {e}")
            raise
        finally:
            if "client" in locals():
                client.close()

        if not sample:
            logger.warning(
                f"No documents found in collection '{collection_name}'. Skipping."
            )
            return []

        type_mapping = {
            "str": StringTypeClass(),
            "int": NumberTypeClass(),
            "float": NumberTypeClass(),
            "bool": BooleanTypeClass(),
            "datetime": TimeTypeClass(),
        }

        field_info = {field: type(value).__name__ for field, value in sample.items()}
        schema_fields = [
            SchemaFieldClass(
                fieldPath=field,
                nativeDataType=py_type,
                type=SchemaFieldDataTypeClass(
                    type=type_mapping.get(py_type, StringTypeClass())
                ),
            )
            for field, py_type in field_info.items()
        ]

        return schema_fields
