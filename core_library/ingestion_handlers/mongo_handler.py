import pymongo
import hashlib
import logging
from typing import List

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
    GlobalTagsClass,
    TagAssociationClass,
)
from datahub.emitter.mce_builder import make_dataset_urn
from core_library.common.emitter import get_data_catalog

MONGO_URI = "mongodb://127.0.0.1:27017"
MONGO_DATABASE = "sample_db"
PLATFORM = "mongodb"
ENV = "PROD"
BASE_VERSION = 130

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_column_description(field_name: str) -> str:
    return f"Column {field_name}"

def generate_schema_hash(collection: str, field_names: List[str]) -> str:
    schema_str = f"{collection}::{','.join(sorted(field_names))}"
    return hashlib.sha256(schema_str.encode("utf-8")).hexdigest()

def map_python_type_to_datahub_type(py_type: str) -> SchemaFieldDataTypeClass:
    type_mapping = {
        'str': StringTypeClass(),
        'int': NumberTypeClass(),
        'float': NumberTypeClass(),
        'bool': BooleanTypeClass(),
        'datetime': TimeTypeClass(),
    }
    return SchemaFieldDataTypeClass(type=type_mapping.get(py_type, StringTypeClass()))

def enrich_metadata():
    data_catalog = get_data_catalog()
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[MONGO_DATABASE]
        print(f"✅ Connected to MongoDB database: {MONGO_DATABASE}")

        collections = db.list_collection_names()
        print(f"📁 Found {len(collections)} collections in '{MONGO_DATABASE}': {collections}")

        for coll_name in collections:
            print(f"\n🔍 Processing collection: {coll_name}")
            collection = db[coll_name]
            sample = collection.find_one()

            if not sample:
                print(f"⚠️ No documents in '{coll_name}'. Skipping.")
                continue

            field_info = {field: type(value).__name__ for field, value in sample.items()}
            print(f"🧬 Inferred fields for '{coll_name}': {field_info}")

            dataset_urn = make_dataset_urn(PLATFORM, f"{MONGO_DATABASE}.{coll_name}", ENV)
            field_names = list(field_info.keys())
            schema_hash = generate_schema_hash(coll_name, field_names)

            schema_fields = []
            for field, py_type in field_info.items():
                schema_fields.append(SchemaFieldClass(
                    fieldPath=field,
                    type=map_python_type_to_datahub_type(py_type),
                    nativeDataType=py_type,
                    description=generate_column_description(field),
                ))

            version = f"S-{BASE_VERSION}"
            tag = version.lower().replace('-', '_')

            schema_metadata = SchemaMetadataClass(
                schemaName=coll_name,
                platform=f"urn:li:dataPlatform:{PLATFORM}",
                version=0,
                platformSchema=OtherSchemaClass(rawSchema=""),
                fields=schema_fields,
                hash=schema_hash,
            )

            dataset_properties = DatasetPropertiesClass(
                name=coll_name,
                customProperties={"version": version}
            )

            global_tags = GlobalTagsClass(
                tags=[TagAssociationClass(tag=f"urn:li:tag:{tag}")]
            )

            snapshot = DatasetSnapshotClass(
                urn=dataset_urn,
                aspects=[schema_metadata, dataset_properties, global_tags],
            )

            mce = MetadataChangeEventClass(proposedSnapshot=snapshot)

            try:
                print(f"📤 Emitting metadata for '{coll_name}'...")
                data_catalog.emit(mce)
                print(f"✅ Successfully emitted metadata for '{coll_name}' with version '{version}' and tag '{tag}'")
            except Exception as emit_err:
                print(f"❌ Error emitting metadata for '{coll_name}': {emit_err}")

    except pymongo.errors.ServerSelectionTimeoutError as mongo_err:
        print(f"❌ Could not connect to MongoDB: {mongo_err}")
    except Exception as err:
        print(f"❌ Unexpected error: {err}")
    finally:
        try:
            client.close()
            print("🔚 MongoDB connection closed.")
        except Exception:
            pass

if __name__ == "__main__":
    enrich_metadata()