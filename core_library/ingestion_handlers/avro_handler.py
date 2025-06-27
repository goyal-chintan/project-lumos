import os
import time
from fastavro import reader
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    DatasetPropertiesClass,
    SchemaMetadataClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    NumberTypeClass,
    BooleanTypeClass,
    AuditStampClass,
    OtherSchemaClass,
)
from datahub.emitter.mce_builder import make_dataset_urn
from core_library.common.emitter import get_data_catalog

def ingest_avro():
    data_catalog = get_data_catalog()
    DATAHUB_GMS_SERVER = "http://localhost:8080"
    PLATFORM = "avro"
    ENV = "DEV"
    AVRO_DIR = "./demo_datasets_avro"

    avro_files = [f for f in os.listdir(AVRO_DIR) if f.endswith(".avro")]

    if not avro_files:
        print(f"No AVRO files found in directory: {AVRO_DIR}")
        return

    for avro_file in avro_files:
        avro_path = os.path.join(AVRO_DIR, avro_file)
        dataset_name = os.path.splitext(avro_file)[0]
        dataset_urn = make_dataset_urn(platform=PLATFORM, name=dataset_name, env=ENV)

        print(f"\n🚀 Processing: {dataset_name}")

        dataset_properties = DatasetPropertiesClass(
            description=f"Dataset '{dataset_name}' ingested from AVRO file.",
            customProperties={},
        )

        schema_field_objs = []
        with open(avro_path, "rb") as fo:
            avro_reader = reader(fo)
            avro_schema = avro_reader.writer_schema

            for field in avro_schema["fields"]:
                name = field["name"]
                dtype = field["type"]

                if isinstance(dtype, list):
                    dtype = [d for d in dtype if d != "null"][0]

                if dtype == "string":
                    data_type = StringTypeClass()
                elif dtype in ["int", "long", "float", "double"]:
                    data_type = NumberTypeClass()
                elif dtype == "boolean":
                    data_type = BooleanTypeClass()
                else:
                    data_type = StringTypeClass()

                schema_field_objs.append(
                    SchemaFieldClass(
                        fieldPath=name,
                        type=SchemaFieldDataTypeClass(type=data_type),
                        nativeDataType=str(dtype),
                        description=f"Field from AVRO: {name}",
                        nullable=True,
                    )
                )

        timestamp = int(time.time() * 1000)
        audit_stamp = AuditStampClass(time=timestamp, actor="urn:li:corpuser:unknown")

        schema_metadata = SchemaMetadataClass(
            schemaName=dataset_name,
            platform=f"urn:li:dataPlatform:{PLATFORM}",
            version=0,
            created=audit_stamp,
            lastModified=audit_stamp,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=str(avro_schema)),
            fields=schema_field_objs,
        )

        snapshot = DatasetSnapshotClass(
            urn=dataset_urn,
            aspects=[dataset_properties, schema_metadata],
        )

        mce = MetadataChangeEventClass(proposedSnapshot=snapshot)
        data_catalog.emit(mce)

        print(f"✅ Ingested dataset: {dataset_name}")

if __name__ == "__main__":
    ingest_avro()