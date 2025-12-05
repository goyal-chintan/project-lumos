# Quick Reference Guide

## Platform-Agnostic Metadata Management Flow

```
┌──────────────────────────────────────────────────────────────┐
│                    LUMOS FRAMEWORK                           │
│           Platform-Agnostic Metadata Management              │
└──────────────────────────────────────────────────────────────┘

INPUT: JSON/YAML Configuration
    │
    ├─ Source: CSV, MongoDB, Avro, Parquet, S3, etc.
    └─ Sink: DataHub, Amundsen, or other platform

    ▼

┌──────────────────────────────────────────────────────────────┐
│  Controllers (CLI Layer)                                     │
│  • IngestionController    - Orchestrates ingestion           │
│  • LineageController      - Manages lineage                  │
│  • EnrichmentController   - Handles enrichment               │
│  • OwnershipController    - Manages ownership                │
│  • VersionController      - Handles versioning               │
└──────────────────────────────────────────────────────────────┘
    │
    ▼
┌──────────────────────────────────────────────────────────────┐
│  Feature Services                                            │
│  • IngestionService    - Orchestrates data source ingestion  │
│  • LineageService      - Manages dataset lineage             │
│  • EnrichmentServices  - Adds tags, descriptions, docs       │
│  • OwnershipService    - Manages users, groups, ownership    │
│  • VersionManager      - Manages dataset versioning          │
│  • ExtractionServices  - 11 types of metadata extraction     │
└──────────────────────────────────────────────────────────────┘
    │
    ▼
┌──────────────────────────────────────────────────────────────┐
│  Platform Abstraction Layer                                  │
│  MetadataPlatformInterface (Abstract)                        │
│  • emit_mce() - Metadata Change Event                        │
│  • emit_mcp() - Metadata Change Proposal                     │
│  • add_lineage() - Lineage relationships                     │
│  • get_aspect_for_urn() - Get entity aspects                 │
└──────────────────────────────────────────────────────────────┘
    │
    ├──────────────────┬──────────────────┬──────────────────┐
    ▼                  ▼                  ▼                  ▼
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│ DataHub  │    │ Amundsen │    │ Databricks│   │  Other   │
│ Handler  │    │ Handler  │    │  Handler  │   │ Platform │
└──────────┘    └──────────┘    └───────────┘   └──────────┘
    │                  │                  │                  │
    ▼                  ▼                  ▼                  ▼
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│ DataHub  │    │ Amundsen │    │ Databricks│   │  Other   │
│ Instance │    │ Instance │    │ Instance  │   │ Instance │
└──────────┘    └──────────┘    └───────────┘   └──────────┘

OUTPUT: Metadata stored in chosen platform
```

## Key Concepts

### 1. Platform Agnostic
- **Same code** works with any metadata platform
- **Configuration-driven** platform selection
- **Easy to switch** between platforms

### 2. Source Agnostic
- **Multiple data sources** supported (CSV, MongoDB, Avro, etc.)
- **Extensible** - easy to add new sources
- **Consistent interface** across all sources

### 3. Abstraction Layers
- **MetadataPlatformInterface**: Platform abstraction
- **BaseIngestionHandler**: Source handler abstraction
- **BaseExtractionService**: Extraction service abstraction
- **Factory Pattern**: Creates appropriate instances

### 4. Controllers Pattern
- **Controllers**: CLI interface layer (`core/controllers/`)
- **Services**: Business logic layer (`feature/`)
- **Separation**: Controllers orchestrate, services implement

## Common Workflows

### Ingestion Workflow
```bash
# 1. Create config file (JSON)
[
  {
    "source_type": "csv",
    "source_path": "/data/file.csv",
    "dataset_name": "my_dataset",
    "delimiter": ","
  }
]

# 2. Run ingestion
python framework_cli.py ingest:sample_configs_and_templates/ingestion/test_ingestion.json

# Flow:
# Config → IngestionController → IngestionService → CSV Handler → DataHub Handler → DataHub
```

### Ownership Workflow
```bash
# 1. Create users
python framework_cli.py create-users:sample_configs_and_templates/ownership/user_template.json

# 2. Create groups
python framework_cli.py create-groups:sample_configs_and_templates/ownership/group_template.json

# 3. Assign ownership
python framework_cli.py assign-ownership:sample_configs_and_templates/ownership/ownership_assignment_template.json

# Flow:
# Config → OwnershipController → OwnershipService → DataHub Handler → DataHub
```

### Versioning Workflow
```bash
# Update all dataset versions
python framework_cli.py version-update

# Scan datasets
python framework_cli.py datasets-summary-scan

# Flow:
# CLI → VersionController → DatasetScanner + VersionManager → DataHub Handler → DataHub
```

### Extraction Workflow
```bash
# Extract to Excel
python framework_cli.py extract:excel-quality

# Extract to CSV
python framework_cli.py extract:csv-governance

# Extract to JSON
python framework_cli.py extract:json-comprehensive

# Flow:
# CLI → ExtractionFactory → ExtractionService → DataHub Handler → Export
```

### Lineage Workflow
```bash
# 1. Create lineage config (JSON)
[
  {
    "downstream": "urn:li:dataset:(csv,downstream,DEV)",
    "upstreams": [
      {"urn": "urn:li:dataset:(csv,upstream,DEV)"}
    ]
  }
]

# 2. Add lineage
python framework_cli.py add-lineage:sample_configs_and_templates/lineage/dataset_lineage_template.json

# Flow:
# Config → LineageController → DatasetLineageService → DataHub Handler → DataHub
```

## Component Map

| Component | Purpose | Location |
|-----------|---------|----------|
| **CLI** | Entry point for commands | `framework_cli.py` |
| **ConfigManager** | Loads and caches configs | `core/common/config_manager.py` |
| **IngestionController** | Orchestrates ingestion | `core/controllers/ingestion_controller.py` |
| **IngestionService** | Ingestion business logic | `feature/ingestion/ingestion_service.py` |
| **CSV Handler** | Handles CSV files | `feature/ingestion/handlers/csv.py` |
| **PlatformFactory** | Creates platform handlers | `core/platform/factory.py` |
| **DataHubHandler** | DataHub implementation | `core/platform/impl/datahub_handler.py` |
| **LineageService** | Manages lineage | `feature/lineage/dataset_lineage_service.py` |
| **OwnershipService** | Manages ownership | `feature/ownership/ownership_service.py` |
| **VersionManager** | Manages versioning | `feature/versioning/version_service.py` |
| **ExtractionFactory** | Creates extractors | `feature/extraction/extraction_factory.py` |

## Adding New Components

### New Data Source
1. Create handler in `feature/ingestion/handlers/`
2. Extend `BaseIngestionHandler`
3. Implement `ingest()` method
4. Register in `feature/ingestion/handlers/factory.py`

### New Platform
1. Create handler in `core/platform/impl/`
2. Implement `MetadataPlatformInterface`
3. Implement `emit_mce()`, `emit_mcp()`, `add_lineage()`, `get_aspect_for_urn()`
4. Register in `core/platform/factory.py`
5. Add config to `configs/global_settings.yaml`

### New Extraction Type
1. Create extractor in `feature/extraction/`
2. Extend `BaseExtractionService`
3. Implement extraction logic
4. Register in `feature/extraction/extraction_factory.py`

## Configuration Files

### Global Settings (`configs/global_settings.yaml`)
```yaml
datahub:
  gms_server: http://localhost:8080
default_env: DEV
default_platform: datahub
```

### Ingestion Config (JSON)
```json
[
  {
    "source_type": "csv",
    "source_path": "/path/to/file.csv",
    "dataset_name": "my_dataset",
    "delimiter": ","
  }
]
```

### Ownership Assignment Config (JSON)
```json
{
  "operation": "assign_ownership",
  "assignments": [
    {
      "entity_urn": "urn:li:dataset:(csv,my_dataset,DEV)",
      "owner_urn": "urn:li:corpuser:john.doe",
      "ownership_type": "TECHNICAL_OWNER"
    }
  ]
}
```

## CLI Operations Reference

### Ingestion
- `ingest:config.json` - Ingest metadata from data sources

### Lineage
- `add-lineage:config.json` - Add dataset lineage
- `add-data-job-lineage:config.json` - Add data job lineage

### Enrichment
- `enrich:config.json` - Enrich datasets with metadata

### Ownership
- `create-users:config.json` - Create users
- `create-groups:config.json` - Create groups
- `assign-ownership:config.json` - Assign ownership

### Versioning
- `version-update` - Update all dataset versions
- `datasets-summary-scan` - Scan and display datasets

### Extraction
- `extract:json-TYPE` - Extract to JSON
- `extract:excel-TYPE` - Extract to Excel
- `extract:csv-TYPE` - Extract to CSV
- `extract:charts-TYPE` - Extract to visualizations

**Extraction Types**: comprehensive, schema, lineage, governance, properties, usage, quality, assertions, profiling, impact, metadata_diff

## Benefits

✅ **Platform Independence** - Switch platforms via config  
✅ **Source Flexibility** - Support multiple data sources  
✅ **Extensibility** - Easy to add new sources/platforms  
✅ **Consistency** - Same API across all platforms  
✅ **Testability** - Mock platform interface for tests  
✅ **Maintainability** - Clear separation of concerns  
✅ **Enterprise Features** - Ownership, versioning, extraction  

## Documentation

- [Architecture Overview](ARCHITECTURE.md) - Complete architecture details
- [End-to-End Flow](END_TO_END_FLOW.md) - Detailed flow sequences
- [Architecture Diagrams](ARCHITECTURE_DIAGRAM.md) - Visual diagrams
