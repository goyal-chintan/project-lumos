# DataHub Ingestion Framework

A comprehensive, enterprise-ready Python framework for ingesting metadata from various data sources into DataHub. This framework provides a modular, extensible architecture with support for multiple data formats and robust error handling.

## 🚀 Key Features

- **Multi-Format Support**: CSV, Avro, Parquet, MongoDB, S3, PostgreSQL
- **Enterprise-Grade Avro Support**: Handles complex nested records, arrays, maps, and enums
- **Schema Inference**: Automatic schema detection for file-based sources
- **Flexible Configuration**: JSON-based configuration with validation
- **DataHub Integration**: Native support for DataHub's metadata platform
- **CLI Interface**: Easy-to-use command-line interface for operations
- **Modular Architecture**: Extensible handler system for new data sources
- **Comprehensive Logging**: Detailed logging with configurable levels
- **Error Resilience**: Robust exception handling and graceful fallbacks

## 📁 Project Structure

```
datahub_framework/
├── core/                           # Core framework components
│   ├── common/                     # Shared utilities and interfaces
│   ├── controllers/                # Operation controllers
│   └── platform/                   # Platform interface implementations
├── feature/                        # Feature-specific modules
│   ├── ingestion/                  # Data ingestion services
│   │   ├── handlers/               # Source-specific handlers
│   │   │   ├── base_ingestion_handler.py
│   │   │   ├── csv.py              # CSV file handler
│   │   │   ├── avro.py             # Avro file handler
│   │   │   ├── parquet.py          # Parquet file handler
│   │   │   ├── mongo.py            # MongoDB handler
│   │   │   ├── s3.py               # S3 handler
│   │   │   └── factory.py          # Handler factory
│   │   └── ingestion_service.py    # Main ingestion orchestrator
│   ├── enrichment/                 # Metadata enrichment services
│   ├── lineage/                    # Data lineage tracking
│   ├── profiling/                  # Data profiling services
│   ├── dq_services/                # Data quality services
│   └── rbac/                       # Access control services
├── configs/                        # Configuration files
├── sample_configs_and_templates/   # Example configurations
├── orchestration_examples/         # Workflow examples (Airflow)
└── framework_cli.py               # Command-line interface
```

## 🛠 Installation

### Prerequisites

- Python 3.8+
- DataHub instance running (for production use)

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Dependencies Include:
- `acryl-datahub>=0.10.0` - DataHub Python SDK
- `pandas>=1.5.0` - Data manipulation
- `pyyaml>=6.0` - YAML configuration support
- `fastavro>=1.7.0` - Avro file processing
- `pymongo>=4.0.0` - MongoDB connectivity
- `boto3>=1.26.0` - AWS S3 integration

## ⚙️ Configuration

### Global Settings (`configs/global_settings.yaml`)

```yaml
datahub:
  gms_server: http://localhost:8080  # DataHub GMS server URL
  test_mode: false                   # Set to true for validation without emission

default_env: DEV
default_platform: datahub
```

### Ingestion Configuration

Create JSON configuration files for your data sources:

```json
[
  {
    "source_type": "csv",
    "source_path": "/path/to/csv/files/",
    "delimiter": ",",
    "infer_schema": true,
    "schema": {}
  },
  {
    "source_type": "avro",
    "source_path": "/path/to/avro/files/",
    "infer_schema": true,
    "schema": {}
  }
]
```

## 🚀 Usage

### Command Line Interface

The framework provides a unified CLI for all operations:

```bash
# Ingest metadata from data sources
python framework_cli.py ingest:path/to/config.json

# Add dataset lineage
python framework_cli.py add-lineage:path/to/lineage_config.json

# Add data job lineage
python framework_cli.py add-data-job-lineage:path/to/job_lineage_config.json

# Enrich metadata
python framework_cli.py enrich:path/to/enrichment_config.json
```

### Supported Source Types

#### 1. CSV Files
```json
{
  "source_type": "csv",
  "source_path": "/path/to/csv/files/",
  "delimiter": ",",          # Only for CSV
  "infer_schema": true,
  "schema": {}
}
```

#### 2. Avro Files (Enterprise Support)
```json
{
  "source_type": "avro",
  "source_path": "/path/to/avro/files/",
  "infer_schema": true,
  "schema": {}
}
```

**Avro Capabilities:**
- ✅ Simple types (string, int, long, float, double, boolean)
- ✅ Complex nested records
- ✅ Arrays and maps
- ✅ Union types and nullable fields
- ✅ Enums
- ✅ Automatic type mapping to DataHub schema

#### 3. Parquet Files
```json
{
  "source_type": "parquet",
  "source_path": "/path/to/parquet/files/",
  "infer_schema": true,
  "schema": {}
}
```

#### 4. MongoDB
```json
{
  "source_type": "mongodb",
  "fully_qualified_source_name": "database.collection",
  "infer_schema": false,
  "schema": {
    "field1": "string",
    "field2": "int"
  }
}
```

#### 5. S3
```json
{
  "source_type": "s3",
  "data_type": "avro",
  "source_path": "s3://bucket/path/",
  "partitioning_format": "year=YYYY/month=MM/day=dd",
  "infer_schema": false,
  "schema": {}
}
```

#### 6. PostgreSQL
```json
{
  "source_type": "postgres",
  "fully_qualified_source_name": "database.schema.table",
  "infer_schema": false,
  "schema": {}
}
```

### Example: Quick Start

1. **Configure DataHub connection:**
   ```bash
   # Edit configs/global_settings.yaml
   datahub:
     gms_server: http://localhost:8080
     test_mode: false
   ```

2. **Create ingestion config:**
   ```json
   [{
     "source_type": "csv",
     "source_path": "sample-data/",
     "delimiter": ",",
     "infer_schema": true,
     "schema": {}
   }]
   ```

3. **Run ingestion:**
   ```bash
   python framework_cli.py ingest:my_config.json
   ```

4. **View in DataHub:**
   Open http://localhost:9002 to see your ingested datasets

## 🏗 Architecture

### Handler System

The framework uses a factory pattern for extensible data source support:

```python
# Adding a new handler
class MyCustomHandler(BaseIngestionHandler):
    def _get_schema_fields(self) -> List[SchemaFieldClass]:
        # Implement schema extraction logic
        pass
    
    def _get_raw_schema(self) -> str:
        # Return raw schema representation
        pass

# Register in factory.py
_handler_registry = {
    "my_custom_type": MyCustomHandler,
    # ... other handlers
}
```

### Metadata Change Events (MCE)

The framework generates DataHub-compatible MCEs with:
- **Schema Metadata**: Field definitions, types, nullability
- **Dataset Properties**: Custom properties, descriptions, timestamps
- **Audit Information**: Creation and modification timestamps

## 🔧 Development

### Testing

```bash
# Run with test mode enabled
# Edit configs/global_settings.yaml:
test_mode: true

# This validates MCE structure without emitting to DataHub
python framework_cli.py ingest:test_config.json
```

### Adding New Handlers

1. Create new handler in `feature/ingestion/handlers/`
2. Inherit from `BaseIngestionHandler`
3. Implement required abstract methods
4. Register in `factory.py`
5. Add constants in `constants.py`

### Logging

Comprehensive logging with levels:
- **INFO**: Operation progress and success
- **WARNING**: Non-critical issues
- **ERROR**: Critical errors with stack traces
- **DEBUG**: Detailed debugging information

## 🎯 Use Cases

- **Data Discovery**: Automatically catalog datasets across your organization
- **Schema Evolution**: Track schema changes over time
- **Data Lineage**: Understand data flow and dependencies
- **Compliance**: Maintain metadata for regulatory requirements
- **Data Quality**: Profile and monitor data quality metrics

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-handler`)
3. Implement your changes with tests
4. Ensure proper logging and error handling
5. Submit a pull request


## 📞 Support

For issues and questions:
1. Check the sample configurations in `sample_configs_and_templates/`
2. Review logs for detailed error information
3. Ensure DataHub is running and accessible
4. Verify file paths and permissions

---

<<<<<<< HEAD
**Built for Enterprise Data Teams** 🏢
=======
**Built for Enterprise Data Teams** 🏢
>>>>>>> ab3e3ee93d8e54a01437e3fa61a9ffdcd682caa2
