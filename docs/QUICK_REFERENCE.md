# Quick Reference Guide

## Platform-Agnostic Metadata Management Flow

```
┌──────────────────────────────────────────────────────────────┐
│                    LUMOS FRAMEWORK                            │
│           Platform-Agnostic Metadata Management               │
└──────────────────────────────────────────────────────────────┘

INPUT: YAML Configuration
    │
    ├─ Source: CSV, MongoDB, Avro, Parquet, S3, etc.
    └─ Sink: DataHub, Amundsen, or other platform

    ▼

┌──────────────────────────────────────────────────────────────┐
│  Core Services                                                │
│  • IngestionService    - Orchestrates data source ingestion  │
│  • LineageService      - Manages dataset lineage             │
│  • EnrichmentServices  - Adds tags, descriptions, docs       │
│  • ProfilingServices   - Generates dataset statistics        │
│  • DQServices          - Data quality assertions             │
└──────────────────────────────────────────────────────────────┘
    │
    ▼
┌──────────────────────────────────────────────────────────────┐
│  Platform Abstraction Layer                                   │
│  MetadataPlatformInterface (Abstract)                         │
│  • emit_mce() - Metadata Change Event                        │
│  • emit_mcp() - Metadata Change Proposal                     │
│  • add_lineage() - Lineage relationships                     │
└──────────────────────────────────────────────────────────────┘
    │
    ├──────────────────┬──────────────────┬──────────────────┐
    ▼                  ▼                  ▼                  ▼
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│ DataHub  │    │ Amundsen │    │ Databricks│   │  Other   │
│ Handler  │    │ Handler  │    │  Handler │    │ Platform │
└──────────┘    └──────────┘    └──────────┘    └──────────┘
    │                  │                  │                  │
    ▼                  ▼                  ▼                  ▼
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│ DataHub  │    │ Amundsen │    │ Databricks│   │  Other   │
│ Instance │    │ Instance │    │ Instance │    │ Instance │
└──────────┘    └──────────┘    └──────────┘    └──────────┘

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
- **Factory Pattern**: Creates appropriate instances

## Common Workflows

### Ingestion Workflow
```bash
# 1. Create config file
source:
  type: csv
  path: /data/file.csv
  dataset_name: my_dataset
sink:
  type: datahub
  env: DEV

# 2. Run ingestion
python framework_cli.py ingest --config-path config.yaml

# Flow:
# Config → IngestionService → CSV Handler → DataHub Handler → DataHub
```

### Lineage Workflow
```bash
# 1. Create lineage config
lineage:
  downstream: "urn:li:dataset:(csv,downstream,DEV)"
  upstreams:
    - urn: "urn:li:dataset:(csv,upstream,DEV)"

# 2. Add lineage
python framework_cli.py add-lineage --config-path lineage.yaml

# Flow:
# Config → LineageService → DataHub Handler → DataHub
```

## Component Map

| Component | Purpose | Location |
|-----------|---------|----------|
| **CLI** | Entry point for commands | `framework_cli.py` |
| **ConfigManager** | Loads and caches YAML configs | `core_library/common/config_manager.py` |
| **IngestionService** | Orchestrates ingestion | `core_library/ingestion_handlers/ingestion_service.py` |
| **CSV Handler** | Handles CSV files | `core_library/ingestion_handlers/csv_handler.py` |
| **PlatformFactory** | Creates platform handlers | `platform_services/platform_factory.py` |
| **DataHubHandler** | DataHub implementation | `platform_services/datahub_handler.py` |
| **LineageService** | Manages lineage | `core_library/lineage_services/dataset_lineage_service.py` |

## Adding New Components

### New Data Source
1. Create handler extending `BaseIngestionHandler`
2. Implement `ingest()` method
3. Register in `IngestionService._handler_registry`

### New Platform
1. Create handler implementing `MetadataPlatformInterface`
2. Implement `emit_mce()`, `emit_mcp()`, `add_lineage()`
3. Register in `PlatformFactory._handler_registry`
4. Add config to `global_settings.yaml`

## Configuration Files

### Global Settings (`configs/global_settings.yaml`)
```yaml
datahub:
  gms_server: http://localhost:8080
default_env: DEV
```

### Ingestion Config
```yaml
source:
  type: csv
  path: /path/to/file.csv
  dataset_name: my_dataset
sink:
  type: datahub
  env: DEV
```

### Lineage Config
```yaml
lineage:
  downstream: "urn:li:dataset:(csv,downstream,DEV)"
  upstreams:
    - urn: "urn:li:dataset:(csv,upstream,DEV)"
```

## Benefits

✅ **Platform Independence** - Switch platforms via config  
✅ **Source Flexibility** - Support multiple data sources  
✅ **Extensibility** - Easy to add new sources/platforms  
✅ **Consistency** - Same API across all platforms  
✅ **Testability** - Mock platform interface for tests  
✅ **Maintainability** - Clear separation of concerns  

## Documentation

- [Architecture Overview](ARCHITECTURE.md) - Complete architecture details
- [End-to-End Flow](END_TO_END_FLOW.md) - Detailed flow sequences
- [Architecture Diagrams](ARCHITECTURE_DIAGRAM.md) - Visual diagrams

