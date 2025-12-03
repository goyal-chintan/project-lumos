# Lumos Framework Examples

This directory contains practical examples demonstrating how to use the Lumos Framework for various data cataloging tasks.

## Directory Structure

```
examples/
├── ingestion/          # Data ingestion examples
├── lineage/           # Lineage tracking examples
├── enrichment/        # Metadata enrichment examples
└── README.md          # This file
```

## Quick Start Examples

### 1. CSV File Ingestion

The simplest way to get started is ingesting metadata from a CSV file:

```python
from core_library.common.config_manager import ConfigManager
from platform_services.platform_factory import PlatformFactory
from core_library.ingestion_handlers.csv_handler import CSVIngestionHandler

# Initialize configuration
config_manager = ConfigManager()
global_config = config_manager.get_global_config()

# Get platform handler
platform = PlatformFactory.get_instance("datahub", global_config["datahub"])

# Configure CSV ingestion
config = {
    "source": {
        "type": "csv",
        "path": "data/employees.csv",
        "dataset_name": "employees",
        "delimiter": ","
    },
    "sink": {
        "type": "datahub",
        "env": "PROD"
    }
}

# Ingest
handler = CSVIngestionHandler(config, platform)
handler.ingest()
```

### 2. MongoDB Collection Ingestion

Ingest metadata from MongoDB collections:

```python
from core_library.ingestion_handlers.mongo_handler import MongoIngestionHandler

config = {
    "source": {
        "type": "mongodb",
        "uri": "mongodb://localhost:27017",
        "database": "analytics",
        "collections": ["users", "events"]
    },
    "sink": {
        "type": "datahub",
        "env": "PROD"
    }
}

handler = MongoIngestionHandler(config, platform)
handler.ingest()
```

### 3. Adding Dataset Lineage

Track data lineage between datasets:

```python
from core_library.lineage_services.dataset_lineage_service import DatasetLineageService

lineage_service = DatasetLineageService(platform)

# Add lineage relationship
upstream_urn = "urn:li:dataset:(urn:li:dataPlatform:csv,raw_users,PROD)"
downstream_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,clean_users,PROD)"

lineage_service.add_lineage(upstream_urn, downstream_urn)
```

### 4. Enriching Metadata with Descriptions

Add descriptions to your datasets:

```python
from core_library.enrichment_services.description_service import DescriptionService

description_service = DescriptionService(platform)

dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,users,PROD)"
description = "This dataset contains user profile information including demographics and preferences."

description_service.add_description(dataset_urn, description)
```

### 5. Adding Tags

Organize datasets with tags:

```python
from core_library.enrichment_services.tag_service import TagService

tag_service = TagService(platform)

dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,users,PROD)"
tags = ["pii", "gdpr", "customer-data"]

for tag in tags:
    tag_service.add_tag(dataset_urn, tag)
```

## Using the CLI

All examples can also be run using the CLI:

### Ingestion via CLI
```bash
lumos ingest --config-path examples/ingestion/csv_example.yaml
```

### Lineage via CLI
```bash
lumos add-lineage --config-path examples/lineage/lineage_example.yaml
```

## Advanced Examples

For more advanced examples, see the individual directories:

- **[Ingestion Examples](ingestion/)**: CSV, MongoDB, Avro, Parquet, S3
- **[Lineage Examples](lineage/)**: Simple and complex lineage relationships
- **[Enrichment Examples](enrichment/)**: Descriptions, tags, properties, documentation

## Configuration Examples

Configuration templates are available in `sample_configs_and_templates/`:

- CSV ingestion: `sample_configs_and_templates/ingestion/csv_ingestion_template.yaml`
- MongoDB ingestion: `sample_configs_and_templates/ingestion/mongo_ingestion_template.yaml`
- Dataset lineage: `sample_configs_and_templates/lineage/dataset_lineage_template.yaml`
- Enrichment: `sample_configs_and_templates/enrichment/add_descriptions_template.yaml`

## Environment Setup

Before running examples, ensure you have:

1. **DataHub running** (or your preferred metadata platform)
   ```bash
   docker run -d -p 9002:9002 -p 8080:8080 linkedin/datahub-gms:latest
   ```

2. **Configuration set up** in `configs/global_settings.yaml`:
   ```yaml
   datahub:
     gms_server: http://localhost:8080
   default_env: DEV
   ```

3. **Dependencies installed**:
   ```bash
   pip install -r requirements.txt
   ```

## Running Examples

Each example can be run as a standalone Python script or through the CLI.

### As Python Script
```bash
python examples/ingestion/csv_ingestion_example.py
```

### Using CLI with Config
```bash
lumos ingest --config-path examples/ingestion/csv_example_config.yaml
```

## Troubleshooting

### Connection Issues
If you encounter connection issues:
- Verify DataHub is running: `curl http://localhost:8080/health`
- Check `configs/global_settings.yaml` has correct URLs
- Ensure network connectivity

### Import Errors
If you encounter import errors:
- Ensure you're in the project root directory
- Install all dependencies: `pip install -r requirements.txt`
- Check Python version (3.8+)

### Configuration Errors
If configuration is not loading:
- Verify YAML syntax is correct
- Check file paths are correct (absolute or relative to working directory)
- Ensure required fields are present in config

## Next Steps

1. **Try the examples**: Start with simple CSV ingestion
2. **Modify configurations**: Adapt examples to your data sources
3. **Build workflows**: Combine examples for end-to-end pipelines
4. **Extend the framework**: Add custom handlers for your specific needs

## Support

- 📖 [Main Documentation](../README.md)
- 🐛 [Report Issues](https://github.com/yourusername/lumos-framework/issues)
- 💬 [Discussions](https://github.com/yourusername/lumos-framework/discussions)

