# Lumos Technical Architecture

> Complete technical design for the universal metadata management framework.

---

## Architecture Principles

### 1. Separation of Concerns

```
+-------------------------------------------------------------------------+
|                          LUMOS ARCHITECTURE                              |
+-------------------------------------------------------------------------+
|                                                                          |
|  +--------------------------------------------------------------------+ |
|  |                     ENTRY LAYER                                     | |
|  |   CLI  |  REST API  |  Python SDK  |  Airflow Operators  |  CI/CD  | |
|  +--------------------------------------------------------------------+ |
|                                   |                                      |
|  +--------------------------------v-----------------------------------+ |
|  |                   CONTROLLER LAYER                                  | |
|  |  Ingestion | Lineage | Ownership | Version | Extraction | Quality  | |
|  +--------------------------------------------------------------------+ |
|                                   |                                      |
|  +--------------------------------v-----------------------------------+ |
|  |                    SERVICE LAYER                                    | |
|  |  IngestionService | LineageService | VersionManager | CostEngine   | |
|  +--------------------------------------------------------------------+ |
|                                   |                                      |
|  +--------------------------------v-----------------------------------+ |
|  |                    HANDLER LAYER                                    | |
|  |   CSV | Avro | Parquet | MongoDB | S3 | Snowflake | Kafka | etc    | |
|  +--------------------------------------------------------------------+ |
|                                   |                                      |
|  +--------------------------------v-----------------------------------+ |
|  |                PLATFORM ABSTRACTION LAYER                           | |
|  |              MetadataPlatformInterface                              | |
|  |   DataHubHandler | AmundsenHandler | AtlasHandler | CustomHandler   | |
|  +--------------------------------------------------------------------+ |
|                                   |                                      |
|  +--------------------------------v-----------------------------------+ |
|  |                   CATALOG LAYER                                     | |
|  |        DataHub  |  Amundsen  |  Atlas  |  OpenMetadata              | |
|  +--------------------------------------------------------------------+ |
|                                                                          |
+-------------------------------------------------------------------------+
```

### 2. SOLID Principles Applied

| Principle | Implementation |
|-----------|---------------|
| **Single Responsibility** | Each service handles one domain (ingestion, lineage, etc.) |
| **Open/Closed** | Add new handlers without modifying core services |
| **Liskov Substitution** | All handlers implement base interfaces correctly |
| **Interface Segregation** | Specific interfaces for specific operations |
| **Dependency Inversion** | Services depend on abstractions, not concrete implementations |

### 3. Configuration-Driven

```yaml
# All behavior controlled via configuration
lumos_config:
  platform: datahub
  environment: production
  
  sources:
    - type: s3
      config: {...}
    - type: snowflake
      config: {...}
      
  features:
    auto_detection: true
    schema_tracking: true
    cost_attribution: true
```

---

## Core Components

### 1. Platform Abstraction Layer

The heart of Lumos-enables catalog independence.

```python
# core/platform/interface.py

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from dataclasses import dataclass

@dataclass
class LumosDataset:
    """Universal dataset representation"""
    urn: str
    name: str
    platform: str
    schema: Dict[str, Any]
    lineage: Dict[str, List[str]]
    ownership: List[Dict[str, str]]
    tags: List[str]
    properties: Dict[str, Any]
    quality: Optional[Dict[str, Any]] = None
    cost: Optional[Dict[str, Any]] = None


class MetadataPlatformInterface(ABC):
    """
    Abstract interface for metadata platforms.
    Implement to support new catalogs.
    """
    
    @abstractmethod
    def __init__(self, config: Dict[str, Any]):
        """Initialize with platform-specific config"""
        pass
    
    @abstractmethod
    def emit_dataset(self, dataset: LumosDataset) -> bool:
        """Create or update a dataset in the catalog"""
        pass
    
    @abstractmethod
    def emit_lineage(self, upstream_urn: str, downstream_urn: str, 
                     lineage_type: str = "TRANSFORMED") -> bool:
        """Add lineage relationship"""
        pass
    
    @abstractmethod
    def emit_ownership(self, dataset_urn: str, owner: str, 
                       owner_type: str) -> bool:
        """Assign ownership"""
        pass
    
    @abstractmethod
    def get_dataset(self, urn: str) -> Optional[LumosDataset]:
        """Retrieve dataset by URN"""
        pass
    
    @abstractmethod
    def search(self, query: str, filters: Dict = None) -> List[LumosDataset]:
        """Search datasets"""
        pass
    
    @abstractmethod
    def export_all(self) -> Dict[str, Any]:
        """Export all metadata in portable format"""
        pass
```

### 2. Platform Factory

```python
# core/platform/factory.py

class PlatformFactory:
    """Factory for creating platform handler instances"""
    
    _registry: Dict[str, Type[MetadataPlatformInterface]] = {
        "datahub": DataHubHandler,
        "amundsen": AmundsenHandler,  # Planned
        "atlas": AtlasHandler,        # Planned
        "openmetadata": OpenMetadataHandler,  # Planned
    }
    
    _instances: Dict[str, MetadataPlatformInterface] = {}
    
    @classmethod
    def get_instance(cls, platform: str, config: Dict) -> MetadataPlatformInterface:
        """Get or create platform handler (singleton per platform)"""
        if platform not in cls._instances:
            handler_class = cls._registry.get(platform)
            if not handler_class:
                raise ValueError(f"Unknown platform: {platform}")
            cls._instances[platform] = handler_class(config)
        return cls._instances[platform]
    
    @classmethod
    def register(cls, name: str, handler: Type[MetadataPlatformInterface]):
        """Register custom platform handler"""
        cls._registry[name] = handler
```

### 3. DataHub Implementation (Current)

```python
# core/platform/impl/datahub_handler.py

from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import *

class DataHubHandler(MetadataPlatformInterface):
    """DataHub implementation of platform interface"""
    
    def __init__(self, config: Dict[str, Any]):
        self.server = config.get("gms_server", "http://localhost:8080")
        self.emitter = DatahubRestEmitter(self.server)
        
    def emit_dataset(self, dataset: LumosDataset) -> bool:
        """Convert LumosDataset to DataHub MCE and emit"""
        try:
            # Convert to DataHub format
            dataset_urn = self._build_urn(dataset)
            
            # Schema aspect
            schema_metadata = self._build_schema_metadata(dataset)
            
            # Properties aspect
            properties = DatasetPropertiesClass(
                name=dataset.name,
                description=dataset.properties.get("description"),
                customProperties=dataset.properties
            )
            
            # Create MCE
            mce = MetadataChangeEventClass(
                proposedSnapshot=DatasetSnapshotClass(
                    urn=dataset_urn,
                    aspects=[schema_metadata, properties]
                )
            )
            
            self.emitter.emit_mce(mce)
            return True
            
        except Exception as e:
            logger.error(f"Failed to emit dataset: {e}")
            return False
    
    def emit_lineage(self, upstream_urn: str, downstream_urn: str,
                     lineage_type: str = "TRANSFORMED") -> bool:
        """Add lineage using UpstreamLineage aspect"""
        upstream = UpstreamClass(
            dataset=upstream_urn,
            type=DatasetLineageTypeClass[lineage_type]
        )

---

## Mapped to the Current Repo

Current foundation (already in repo):
- Entry points: `framework_cli.py` (chained operations)
- Orchestration: `core/controllers/*`
- Services: `feature/ingestion`, `feature/lineage`, `feature/enrichment`, `feature/extraction`, `feature/versioning`
- Platform abstraction: `core/platform/interface.py` + `core/platform/factory.py`
- Platform implementation today: `core/platform/impl/datahub_handler.py`

Key flows (planned additions to close the loop):
- Snapshot export for portability
- Snapshot diff for breaking change classification
- CI gate integration (Jenkins/GitHub Actions)
        
        lineage = UpstreamLineageClass(upstreams=[upstream])
        
        mcp = MetadataChangeProposalWrapper(
            entityUrn=downstream_urn,
            aspect=lineage
        )
        
        self.emitter.emit(mcp)
        return True
```

---

## Ingestion Architecture

### Handler Pattern

```python
# feature/ingestion/handlers/base_ingestion_handler.py

class BaseIngestionHandler(ABC):
    """Base class for all ingestion handlers"""
    
    def __init__(self, config: Dict[str, Any], 
                 platform_handler: MetadataPlatformInterface):
        self.config = config
        self.platform_handler = platform_handler
        
    @abstractmethod
    def ingest(self) -> Optional[LumosDataset]:
        """Extract metadata and return LumosDataset"""
        pass
    
    @abstractmethod
    def _extract_schema(self) -> Dict[str, Any]:
        """Extract schema from source"""
        pass
    
    def emit(self, dataset: LumosDataset) -> bool:
        """Emit to platform via handler"""
        return self.platform_handler.emit_dataset(dataset)
```

### Handler Implementations

```python
# feature/ingestion/handlers/avro.py

class AvroHandler(BaseIngestionHandler):
    """Handler for Avro files"""
    
    def ingest(self) -> Optional[LumosDataset]:
        file_path = self.config["source"]["path"]
        
        # Read Avro schema
        with open(file_path, "rb") as f:
            reader = fastavro.reader(f)
            avro_schema = reader.writer_schema
        
        # Convert to Lumos schema
        schema = self._convert_avro_schema(avro_schema)
        
        return LumosDataset(
            urn=self._build_urn(),
            name=self._get_dataset_name(),
            platform="avro",
            schema=schema,
            lineage={"upstream": [], "downstream": []},
            ownership=[],
            tags=[],
            properties={
                "format": "avro",
                "avro_schema": json.dumps(avro_schema)
            }
        )
    
    def _convert_avro_schema(self, avro_schema: Dict) -> Dict:
        """Convert Avro schema to Lumos schema format"""
        fields = []
        for field in avro_schema.get("fields", []):
            fields.append({
                "name": field["name"],
                "type": self._map_avro_type(field["type"]),
                "nullable": self._is_nullable(field["type"]),
                "description": field.get("doc", "")
            })
        return {"fields": fields}
```

### Handler Factory

```python
# feature/ingestion/handlers/factory.py

class HandlerFactory:
    """Factory for creating source-specific handlers"""
    
    _registry = {
        "csv": CSVHandler,
        "avro": AvroHandler,
        "parquet": ParquetHandler,
        "mongodb": MongoDBHandler,
        "s3": S3Handler,
        "postgres": PostgreSQLHandler,
        "snowflake": SnowflakeHandler,  # Planned
        "bigquery": BigQueryHandler,     # Planned
        "kafka": KafkaHandler,           # Planned
    }
    
    @classmethod
    def get_handler(cls, config: Dict, 
                    platform_handler: MetadataPlatformInterface) -> BaseIngestionHandler:
        source_type = config["source"]["type"]
        handler_class = cls._registry.get(source_type)
        if not handler_class:
            raise ValueError(f"Unknown source type: {source_type}")
        return handler_class(config, platform_handler)
```

---

## Lineage Architecture

### Lineage Model

```python
# feature/lineage/models.py

@dataclass
class LineageEdge:
    """Represents a lineage relationship"""
    upstream_urn: str
    downstream_urn: str
    lineage_type: str  # TRANSFORMED, COPIED, VIEW
    confidence: float  # 0.0 - 1.0
    created_at: datetime
    created_by: str
    
    # Column-level lineage (optional)
    column_mappings: Optional[List[ColumnMapping]] = None


@dataclass
class ColumnMapping:
    """Column-level lineage"""
    upstream_column: str
    downstream_column: str
    transformation: Optional[str] = None  # SQL expression
```

### Lineage Service

```python
# feature/lineage/dataset_lineage_service.py

class DatasetLineageService:
    """Service for managing dataset lineage"""
    
    def __init__(self, platform_handler: MetadataPlatformInterface):
        self.platform_handler = platform_handler
    
    def add_lineage(self, upstream_urn: str, downstream_urn: str,
                    lineage_type: str = "TRANSFORMED",
                    column_mappings: List[ColumnMapping] = None) -> bool:
        """Add lineage relationship"""
        
        # Dataset-level lineage
        success = self.platform_handler.emit_lineage(
            upstream_urn, downstream_urn, lineage_type
        )
        
        # Column-level lineage (if provided)
        if column_mappings:
            for mapping in column_mappings:
                self._emit_column_lineage(
                    upstream_urn, downstream_urn, mapping
                )
        
        return success
    
    def get_upstream(self, dataset_urn: str) -> List[str]:
        """Get all upstream datasets"""
        dataset = self.platform_handler.get_dataset(dataset_urn)
        return dataset.lineage.get("upstream", []) if dataset else []
    
    def get_downstream(self, dataset_urn: str) -> List[str]:
        """Get all downstream datasets"""
        dataset = self.platform_handler.get_dataset(dataset_urn)
        return dataset.lineage.get("downstream", []) if dataset else []
    
    def get_impact(self, dataset_urn: str, depth: int = 10) -> Dict:
        """Calculate impact of changes to a dataset"""
        visited = set()
        impact = {"direct": [], "indirect": [], "depth": {}}
        
        self._traverse_downstream(dataset_urn, 0, depth, visited, impact)
        
        return impact
```

---

## Version Management Architecture

### Version Model

```python
# feature/versioning/models.py

@dataclass
class DatasetVersion:
    """Represents a dataset version"""
    cloud_version: str      # e.g., "S-312"
    schema_version: str     # e.g., "2.1.0"
    timestamp: datetime
    author: str
    change_type: str        # SCHEMA_CHANGE, DATA_UPDATE, METADATA_UPDATE
    change_details: Dict[str, Any]
    previous_version: Optional[str] = None


@dataclass
class VersionMapping:
    """Maps cloud versions to schema versions"""
    cloud_version: str
    schema_version: str
    
    # Example: {"S-311": "1.0.0", "S-312": "1.1.0", "S-313": "2.0.0"}
```

### Version Manager

```python
# feature/versioning/version_service.py

class VersionManager:
    """Manages dataset versioning"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config = config_manager.get_global_config()
        self.cloud_prefix = self.config.get("cloud_version_prefix", "S-")
        
    def increment_version(self, dataset_urn: str, 
                          change_type: str) -> VersionUpdateResult:
        """Increment dataset version based on change type"""
        
        current = self.get_current_version(dataset_urn)
        
        if change_type == "BREAKING_SCHEMA_CHANGE":
            new_schema = self._increment_major(current.schema_version)
        elif change_type == "SCHEMA_CHANGE":
            new_schema = self._increment_minor(current.schema_version)
        else:
            new_schema = self._increment_patch(current.schema_version)
        
        new_cloud = self._increment_cloud(current.cloud_version)
        
        return self._update_version(dataset_urn, new_cloud, new_schema)
    
    def get_version_history(self, dataset_urn: str) -> List[DatasetVersion]:
        """Get complete version history"""
        pass
    
    def compare_versions(self, dataset_urn: str, 
                         v1: str, v2: str) -> Dict:
        """Compare two versions of a dataset"""
        pass
```

---

## Extraction Architecture

### Extraction Factory

```python
# feature/extraction/extraction_factory.py

class ExtractionFactory:
    """Factory for extraction services"""
    
    EXTRACTORS = {
        "comprehensive": ComprehensiveDatasetExtractor,
        "schema": SchemaExtractorService,
        "lineage": LineageExtractorService,
        "governance": GovernanceExtractorService,
        "properties": PropertiesExtractorService,
        "usage": UsageExtractorService,
        "quality": QualityExtractorService,
        "assertions": AssertionsExtractorService,
        "profiling": ProfilingExtractorService,
        "impact": ImpactExtractorService,
        "metadata_diff": MetadataDiffService,
    }
    
    @classmethod
    def get_extractor(cls, extraction_type: str, 
                      config_manager) -> BaseExtractionService:
        extractor_class = cls.EXTRACTORS.get(extraction_type)
        if not extractor_class:
            raise ValueError(f"Unknown extraction type: {extraction_type}")
        return extractor_class(config_manager)
```

### Export Formats

```python
# feature/extraction/export/

class ExcelExporter:
    """Export to Excel with multiple sheets"""
    
    def export(self, data: ExtractionResult, output_path: str):
        with pd.ExcelWriter(output_path) as writer:
            # Summary sheet
            pd.DataFrame(data.summary).to_excel(writer, sheet_name="Summary")
            
            # Datasets sheet
            pd.DataFrame(data.datasets).to_excel(writer, sheet_name="Datasets")
            
            # Lineage sheet
            pd.DataFrame(data.lineage).to_excel(writer, sheet_name="Lineage")


class CSVExporter:
    """Export to CSV files"""
    pass


class VisualizationExporter:
    """Export to charts and graphs"""
    pass
```

---

## Data Flow Diagrams

### Ingestion Flow

```
+-------------------------------------------------------------------------+
|                         INGESTION FLOW                                   |
+-------------------------------------------------------------------------+

User                CLI                Controller           Service
 |                   |                     |                   |
 |  ingest:config    |                     |                   |
 |------------------>|                     |                   |
 |                   |                     |                   |
 |                   |  load_config()      |                   |
 |                   |-------------------->|                   |
 |                   |                     |                   |
 |                   |  run_ingestion()    |                   |
 |                   |-------------------->|                   |
 |                   |                     |                   |
 |                   |                     |  get_handler()    |
 |                   |                     |------------------>|
 |                   |                     |                   |
 |                   |                     |                   |  +--------+
 |                   |                     |                   |-->| Source |
 |                   |                     |                   |  +--------+
 |                   |                     |                   |  extract
 |                   |                     |                   |  schema
 |                   |                     |                   |<--
 |                   |                     |                   |
 |                   |                     |                   |  +--------+
 |                   |                     |                   |-->|Platform|
 |                   |                     |                   |  |Handler |
 |                   |                     |                   |  +--------+
 |                   |                     |                   |  emit_mce
 |                   |                     |                   |<--
 |                   |                     |                   |
 |                   |                     |<------------------|
 |                   |<--------------------|                   |
 |<------------------|                     |                   |
 |  Success          |                     |                   |
```

### Lineage Flow

```
+-------------------------------------------------------------------------+
|                          LINEAGE FLOW                                    |
+-------------------------------------------------------------------------+

               +-------------+
               | raw_events  |
               |   (S3)      |
               +------+------+
                      |
        +-------------+-------------+
        |             |             |
        v             v             v
+-----------+ +-----------+ +-----------+
| cleaned   | |  metrics  | |  alerts   |
|  events   | | aggregates| | processor |
+-----+-----+ +-----------+ +-----------+
      |
      v
+-----------+
| feature   |
|  store    |
+-----+-----+
      |
      v
+-----------+
|    ML     |
|predictions|
+-----------+

# Lumos tracks this entire graph:
# - Dataset-level: raw_events -> cleaned_events -> feature_store -> predictions
# - Column-level: raw_events.user_id -> cleaned_events.user_id -> features.user_id
# - Cost attribution: flows backward through the graph
```

---

## Deployment Architecture

### Docker Compose (Development)

```yaml
# docker-compose.yml
version: '3.8'

services:
  lumos:
    build: .
    ports:
      - "8000:8000"
    environment:
      - LUMOS_PLATFORM=datahub
      - DATAHUB_GMS_URL=http://datahub-gms:8080
    depends_on:
      - datahub-gms
      
  datahub-gms:
    image: linkedin/datahub-gms:latest
    ports:
      - "8080:8080"
    environment:
      - EBEAN_DATASOURCE_URL=jdbc:mysql://mysql:3306/datahub
    depends_on:
      - mysql
      - elasticsearch
      
  mysql:
    image: mysql:8.0
    environment:
      - MYSQL_ROOT_PASSWORD=datahub
      - MYSQL_DATABASE=datahub
      
  elasticsearch:
    image: elasticsearch:7.17.0
    environment:
      - discovery.type=single-node
```

### Kubernetes (Production)

```yaml
# helm/lumos/values.yaml
replicaCount: 3

image:
  repository: lumos/lumos
  tag: "1.0.0"

config:
  platform: datahub
  datahub:
    gms_server: http://datahub-gms.datahub:8080

resources:
  requests:
    memory: "512Mi"
    cpu: "500m"
  limits:
    memory: "2Gi"
    cpu: "2000m"

autoscaling:
  enabled: true
  minReplicas: 3
  maxReplicas: 10
  targetCPUUtilizationPercentage: 70
```

---

## Extension Points

### Adding a New Source Handler

```python
# 1. Create handler class
class MySourceHandler(BaseIngestionHandler):
    def ingest(self) -> Optional[LumosDataset]:
        # Extract from your source
        pass
    
    def _extract_schema(self) -> Dict:
        # Extract schema
        pass

# 2. Register in factory
HandlerFactory.register("mysource", MySourceHandler)

# 3. Use in config
sources:
  - type: mysource
    connection: {...}
```

### Adding a New Platform Handler

```python
# 1. Implement interface
class MyPlatformHandler(MetadataPlatformInterface):
    def emit_dataset(self, dataset: LumosDataset) -> bool:
        # Convert and emit to your platform
        pass
    
    # Implement all abstract methods...

# 2. Register in factory
PlatformFactory.register("myplatform", MyPlatformHandler)

# 3. Use in config
lumos_config:
  platform: myplatform
```

---

## Next: Feature Roadmap

See [05_roadmap.md](05_roadmap.md) for current and planned features.
