from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging
import pandas as pd
import os
import hashlib
from datahub.ingestion.run.pipeline import Pipeline
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
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.mce_builder import make_dataset_urn

# Configuration
CSV_FILE_PATH = "/Users/skalamani/Desktop/DataHub-/demo_datasets_csv/location_capabilities.csv"
DATAHUB_GMS = "http://localhost:8080"
PLATFORM = "csv"
ENV = "PROD"

# Airflow default args
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

logger = logging.getLogger(__name__)

def generate_column_description(column):
    return f"Column {column.replace('_', ' ').title()} contains relevant information."

def ingest_from_csv():
    logger.info("🚀 Starting DataHub CSV ingestion pipeline...")
    pipeline_config = {
        "source": {
            "type": "file",
            "config": {
                "filename": CSV_FILE_PATH,
                "file_format": "csv",
                "delimiter": ",",
                "csv_header": "infer",
                "platform": PLATFORM,
                "env": ENV,
            },
        },
        "sink": {
            "type": "datahub-rest",
            "config": {
                "server": DATAHUB_GMS,
            },
        },
    }
    pipeline = Pipeline.create(pipeline_config)
    pipeline.run()
    pipeline.raise_from_status()
    logger.info("✅ CSV ingestion completed.")

def enrich_metadata():
    table_name = os.path.splitext(os.path.basename(CSV_FILE_PATH))[0]
    df = pd.read_csv(CSV_FILE_PATH)
    emitter = DatahubRestEmitter(DATAHUB_GMS)

    type_mapping = {
        "int64": NumberTypeClass(),
        "float64": NumberTypeClass(),
        "bool": BooleanTypeClass(),
        "object": StringTypeClass(),
        "datetime64[ns]": TimeTypeClass(),
    }

    fields = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        dh_type = type_mapping.get(dtype, StringTypeClass())
        description = generate_column_description(col)
        fields.append(SchemaFieldClass(
            fieldPath=col,
            type=SchemaFieldDataTypeClass(type=dh_type),
            nativeDataType=dtype,
            description=description,
        ))

    schema_metadata = SchemaMetadataClass(
        schemaName=table_name,
        platform=f"urn:li:dataPlatform:{PLATFORM}",
        version=0,
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=fields,
        hash="",
    )

    dataset_properties = DatasetPropertiesClass(
        name=table_name,
        description=f"Metadata for CSV file: {table_name}",
    )

    snapshot = DatasetSnapshotClass(
        urn=make_dataset_urn(PLATFORM, table_name, env=ENV),
        aspects=[schema_metadata, dataset_properties],
    )

    mce = MetadataChangeEventClass(proposedSnapshot=snapshot)
    emitter.emit(mce)
    logger.info(f"📦 Metadata enriched and pushed for: {table_name}")

with DAG(
    dag_id="datahub_metadata_release",
    default_args=default_args,
    start_date=days_ago(1),
    catchup=False,
    schedule_interval=timedelta(days=1),
    tags=["datahub", "csv"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_from_csv",
        python_callable=ingest_from_csv,
    )

    enrich = PythonOperator(
        task_id="enrich_metadata",
        python_callable=enrich_metadata,
    )

    ingest >> enrich