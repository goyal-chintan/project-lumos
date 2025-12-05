# End-to-End Flow Documentation

## Complete Flow Diagram

```mermaid
sequenceDiagram
    participant User
    participant CLI as Framework CLI
    participant CM as ConfigManager
    participant CTRL as Controller<br/>(Ingestion/Lineage/etc.)
    participant PF as PlatformFactory
    participant IS as IngestionService
    participant Handler as CSV Handler
    participant PH as Platform Handler<br/>(DataHub)
    participant Platform as DataHub GMS

    User->>CLI: python framework_cli.py ingest:config.json
    
    CLI->>CM: load_config(config_path)
    CM->>CM: Load JSON/YAML file
    CM-->>CLI: Return config dict
    
    CLI->>CM: get_global_config()
    CM->>CM: Load global_settings.yaml
    CM-->>CLI: Return platform config
    
    CLI->>CTRL: IngestionController.run_ingestion(config_path)
    CTRL->>CM: load_config(config_path)
    CM-->>CTRL: Return job config
    
    CTRL->>PF: get_instance("datahub", config_manager)
    PF->>PF: Check if instance exists
    alt Instance doesn't exist
        PF->>PF: Create DataHubHandler
        PF->>PF: Store as singleton
    end
    PF-->>CTRL: Return platform_handler
    
    CTRL->>IS: IngestionService(config, platform_handler)
    IS-->>CTRL: Service initialized
    
    CTRL->>IS: start_ingestion(config)
    IS->>IS: Identify source type: "csv"
    IS->>IS: Get handler class: CSVIngestionHandler
    IS->>Handler: CSVIngestionHandler(config, platform_handler)
    Handler-->>IS: Handler created
    
    IS->>Handler: ingest()
    Handler->>Handler: Read CSV file
    Handler->>Handler: Extract schema (columns, types)
    Handler->>Handler: Create MCE objects
    Note over Handler: DatasetSnapshot<br/>SchemaMetadata<br/>DatasetProperties
    
    Handler->>PH: emit_mce(mce)
    PH->>PH: Create DatahubRestEmitter
    PH->>Platform: HTTP POST /entities/v1/...
    Platform-->>PH: 200 OK
    PH-->>Handler: Success
    
    Handler-->>IS: Complete
    IS-->>CTRL: Return True
    CTRL-->>CLI: Success
    CLI-->>User: "Ingestion completed successfully"
```

## Detailed Step-by-Step Flow

### Phase 1: Initialization

```
1. User invokes CLI
   └─ Command: python framework_cli.py ingest:config.json

2. CLI parses arguments
   ├─ Splits operation:config_path
   ├─ Identifies operation type (ingest, enrich, lineage, etc.)
   └─ Routes to appropriate controller

3. Controller initialization
   ├─ ConfigManager.load_config(config_path)
   │   └─ Parses JSON/YAML: source type, path, dataset name, sink config
   └─ ConfigManager.get_global_config()
       └─ Loads global_settings.yaml: platform connection details

4. Platform handler creation
   ├─ PlatformFactory.get_instance("datahub", config_manager)
   ├─ Checks singleton cache
   ├─ Creates DataHubHandler if not exists
   │   ├─ Initializes DatahubRestEmitter
   │   └─ Connects to GMS server (http://localhost:8080)
   └─ Returns MetadataPlatformInterface instance
```

### Phase 2: Service Orchestration

```
5. Feature service setup
   ├─ IngestionService(config, platform_handler)
   ├─ Registers available handlers:
   │   ├─ "csv" → CSVIngestionHandler
   │   ├─ "mongodb" → MongoIngestionHandler
   │   ├─ "avro" → AvroIngestionHandler
   │   └─ ...
   └─ Ready to process requests

6. Handler selection
   ├─ IngestionService.start_ingestion(config)
   ├─ Loads job configuration
   ├─ Extracts source type: config["source_type"]
   ├─ Looks up handler in factory
   └─ Instantiates appropriate handler
```

### Phase 3: Metadata Extraction

```
7. Source-specific processing (CSV example)
   ├─ CSVIngestionHandler.ingest()
   ├─ Validates configuration
   ├─ Reads CSV file using pandas
   ├─ Extracts schema:
   │   ├─ Column names
   │   ├─ Data types (int64, float64, object)
   │   └─ Type mapping to DataHub types
   └─ Creates metadata objects:
       ├─ SchemaFieldClass for each column
       ├─ SchemaMetadataClass (complete schema)
       ├─ DatasetPropertiesClass (name, description)
       └─ DatasetSnapshotClass (combines all aspects)
```

### Phase 4: Platform Emission

```
8. Metadata transformation
   ├─ Creates MetadataChangeEventClass (MCE)
   ├─ Wraps DatasetSnapshot in MCE
   └─ Generates URN: urn:li:dataset:(csv,dataset_name,DEV)

9. Platform-specific emission
   ├─ Handler calls: platform_handler.emit_mce(mce)
   ├─ DataHubHandler.emit_mce(mce)
   │   ├─ Uses DatahubRestEmitter
   │   ├─ Serializes MCE to JSON
   │   └─ HTTP POST to DataHub GMS API
   └─ Platform receives and stores metadata
```

### Phase 5: Completion

```
10. Success handling
    ├─ Platform returns success response
    ├─ Handler logs success
    ├─ IngestionService returns True
    ├─ Controller logs success
    └─ CLI displays success message

11. Error handling
    ├─ Any exception caught at handler level
    ├─ Logged with context
    ├─ Service returns False
    ├─ Controller handles error
    └─ CLI displays error message
```

## Ownership Flow

```mermaid
sequenceDiagram
    participant User
    participant CLI
    participant CM as ConfigManager
    participant CTRL as OwnershipController
    participant PF as PlatformFactory
    participant OS as OwnershipService
    participant PH as Platform Handler
    participant Platform as DataHub

    User->>CLI: create-users:config.json
    CLI->>CM: load_config(config.json)
    CM-->>CLI: Users config
    
    CLI->>CTRL: run_create_users(config_path)
    CTRL->>CM: get_global_config()
    CM-->>CTRL: Platform config
    
    CTRL->>PF: get_instance("datahub", config_manager)
    PF-->>CTRL: platform_handler
    
    CTRL->>OS: OwnershipService(platform_handler, config_manager)
    CTRL->>OS: create_user(user_data)
    
    OS->>PH: create_user(user_data)
    PH->>Platform: HTTP POST /entities/v1/...
    Platform-->>PH: Success
    PH-->>OS: True
    OS-->>CTRL: Success
    CTRL-->>CLI: Success
    CLI-->>User: "Users created successfully"
```

## Versioning Flow

```mermaid
sequenceDiagram
    participant User
    participant CLI
    participant CTRL as VersionController
    participant DS as DatasetScanner
    participant VM as VersionManager
    participant PH as Platform Handler
    participant Platform as DataHub

    User->>CLI: version-update
    CLI->>CTRL: run_version_update()
    
    CTRL->>DS: scan_all_datasets()
    DS->>PH: Query all datasets
    PH->>Platform: GraphQL query
    Platform-->>PH: Dataset list
    PH-->>DS: Datasets
    DS-->>CTRL: Dataset list
    
    CTRL->>VM: bulk_update_versions(dataset_urns)
    
    loop For each dataset
        VM->>PH: get_aspect_for_urn(urn, "datasetProperties")
        PH->>Platform: Get properties
        Platform-->>PH: Properties with cloud_version
        PH-->>VM: Current version mapping
        
        VM->>VM: Increment versions
        VM->>PH: emit_mcp(version_mcp)
        PH->>Platform: HTTP POST /aspects
        Platform-->>PH: Success
    end
    
    VM-->>CTRL: Update results
    CTRL-->>CLI: Summary
    CLI-->>User: "Version update complete"
```

## Extraction Flow

```mermaid
sequenceDiagram
    participant User
    participant CLI
    participant EF as ExtractionFactory
    participant ES as ExtractionService
    participant PH as Platform Handler
    participant Platform as DataHub
    participant EXP as Exporter

    User->>CLI: extract:excel-quality
    CLI->>EF: get_extractor("quality")
    EF-->>CLI: QualityExtractorService
    
    CLI->>ES: extract()
    ES->>PH: Query datasets
    PH->>Platform: GraphQL queries
    Platform-->>PH: Dataset metadata
    PH-->>ES: Datasets
    
    ES->>ES: Analyze quality metrics
    ES->>ES: Generate extraction result
    
    CLI->>EXP: ExcelExporter.export(result)
    EXP->>EXP: Create Excel workbook
    EXP->>EXP: Write data to sheets
    EXP-->>CLI: File saved
    
    CLI-->>User: "Extraction complete: extracted_data/quality.xlsx"
```

## Key Design Patterns

### 1. Factory Pattern
- **PlatformFactory**: Creates platform handler instances
- **Handler Factory**: Maps source types to handler classes
- **ExtractionFactory**: Maps extraction types to extractor services
- **EnrichmentFactory**: Maps enrichment types to enrichment services
- **Singleton**: One instance per platform

### 2. Strategy Pattern
- **Ingestion Handlers**: Different strategies for different source types
- **Platform Handlers**: Different strategies for different platforms
- **Extraction Services**: Different strategies for different extraction types

### 3. Controller Pattern
- **Controllers**: Act as CLI interface layer
- **Separation**: Controllers orchestrate, services implement logic
- **Validation**: Controllers validate configs before passing to services

### 4. Dependency Inversion
- **High-level services** depend on `MetadataPlatformInterface` abstraction
- **Concrete implementations** (DataHubHandler) implement the interface
- **Easy to swap** platforms without changing core logic

### 5. Template Method
- **BaseIngestionHandler**: Defines skeleton of ingestion algorithm
- **Concrete handlers**: Implement specific steps (CSV, MongoDB, etc.)

## Data Flow Summary

```
YAML/JSON Config
    ↓
ConfigManager
    ↓
Controller (CLI Layer)
    ↓
Feature Service (Business Logic)
    ↓
Source Handler / Extraction Service
    ↓
Metadata Objects (MCE/MCP)
    ↓
Platform Handler (DataHub/Amundsen)
    ↓
Metadata Platform (REST API)
    ↓
Metadata Stored ✓
```

## Platform Agnostic Benefits

1. **Same code** works with DataHub, Amundsen, or any platform
2. **Configuration-driven** platform selection
3. **Easy testing** with mock platform handlers
4. **Future-proof** - add new platforms without code changes
5. **Vendor independence** - not locked into one platform

