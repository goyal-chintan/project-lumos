import pymongo
import logging
from typing import Dict, Any, List
from .base_ingestion_handler import BaseIngestionHandler
from platform_services.metadata_platform_interface import MetadataPlatformInterface
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass, DatasetSnapshotClass, SchemaMetadataClass,
    SchemaFieldClass, SchemaFieldDataTypeClass, StringTypeClass, NumberTypeClass,
    BooleanTypeClass, TimeTypeClass, OtherSchemaClass, DatasetPropertiesClass
)
from datahub.emitter.mce_builder import make_dataset_urn

logger = logging.getLogger(__name__)

class MongoIngestionHandler(BaseIngestionHandler):
    """Handler for MongoDB ingestion."""
    
    def __init__(self, config: Dict[str, Any], platform_handler: MetadataPlatformInterface):
        super().__init__(config, platform_handler)
        self.required_fields.extend(["uri", "database"])

    def _map_python_type_to_datahub_type(self, py_type: str) -> SchemaFieldDataTypeClass:
        type_mapping = {
            'str': StringTypeClass(), 'int': NumberTypeClass(), 'float': NumberTypeClass(),
            'bool': BooleanTypeClass(), 'datetime': TimeTypeClass(),
        }
        return SchemaFieldDataTypeClass(type=type_mapping.get(py_type, StringTypeClass()))

    def ingest(self) -> None:
        """Ingests metadata from a MongoDB database to the metadata platform."""
        if not self.validate_config():
            raise ValueError("MongoDB source config validation failed.")

        uri = self.source_config["uri"]
        database_name = self.source_config["database"]
        env = self.sink_config.get("env", "PROD")
        platform = "mongodb"
        
        try:
            client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.server_info() # Validate connection
            db = client[database_name]
            logger.info(f"Connected to MongoDB database: {database_name}")

            collections = self.source_config.get("collections", db.list_collection_names())
            logger.info(f"Found {len(collections)} collections to process: {collections}")

            for coll_name in collections:
                self._ingest_collection(db, coll_name, platform, env)

        except pymongo.errors.ServerSelectionTimeoutError as e:
            logger.error(f"Could not connect to MongoDB at {uri}: {e}")
            raise
        finally:
            if 'client' in locals():
                client.close()

    def _ingest_collection(self, db: Any, coll_name: str, platform: str, env: str):
        logger.info(f"Processing collection: {coll_name}")
        collection = db[coll_name]
        sample = collection.find_one()

        if not sample:
            logger.warning(f"No documents found in collection '{coll_name}'. Skipping.")
            return

        field_info = {field: type(value).__name__ for field, value in sample.items()}
        # Normalize certain types to strings for DataHub mapping (e.g., ObjectId)
        normalized = {}
        for k, v in field_info.items():
            if v.lower() in {"objectid"}:
                normalized[k] = "str"
            else:
                normalized[k] = v
        schema_fields = [
            SchemaFieldClass(
                fieldPath=field,
                nativeDataType=py_type,
                type=self._map_python_type_to_datahub_type(py_type)
            ) for field, py_type in normalized.items()
        ]

        dataset_name = f"{db.name}.{coll_name}"
        dataset_urn = make_dataset_urn(platform, dataset_name, env)

        schema_metadata = SchemaMetadataClass(
            schemaName=coll_name,
            platform=f"urn:li:dataPlatform:{platform}",
            version=0, hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=schema_fields
        )
        dataset_properties = DatasetPropertiesClass(name=coll_name)
        snapshot = DatasetSnapshotClass(urn=dataset_urn, aspects=[schema_metadata, dataset_properties])
        mce = MetadataChangeEventClass(proposedSnapshot=snapshot)
        
        self.platform_handler.emit_mce(mce)