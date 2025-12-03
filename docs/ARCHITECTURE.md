# Lumos Framework Architecture

## Overview

Lumos Framework is built on a modular, extensible architecture that follows SOLID principles and design patterns to ensure maintainability, testability, and scalability.

## Core Design Principles

### 1. Single Responsibility Principle (SRP)
Each class has one well-defined responsibility:
- `ConfigManager`: Load and cache configurations
- `IngestionService`: Orchestrate ingestion process
- `DatasetLineageService`: Manage lineage relationships
- Handlers: Handle specific data source types

### 2. Open/Closed Principle (OCP)
The framework is open for extension but closed for modification:
- New data source handlers can be added without modifying core code
- New platforms can be integrated through the interface
- Services can be extended with new capabilities

### 3. Liskov Substitution Principle (LSP)
All handlers implement common interfaces and can be used interchangeably:
- `BaseIngestionHandler` for all data source handlers
- `MetadataPlatformInterface` for all platform implementations

### 4. Interface Segregation Principle (ISP)
Interfaces are focused and specific:
- Platform interface defines only necessary methods
- Services depend only on interfaces they actually use

### 5. Dependency Inversion Principle (DIP)
High-level modules depend on abstractions:
- Services depend on `MetadataPlatformInterface`, not concrete implementations
- Handlers receive platform through dependency injection

## Architecture Layers

```
┌─────────────────────────────────────────────────────────────┐
│                    Presentation Layer                        │
│                                                              │
│  ┌──────────────┐         ┌──────────────┐                 │
│  │     CLI      │         │  Python API  │                 │
│  │ (framework_  │         │   (Direct    │                 │
│  │    cli.py)   │         │   Imports)   │                 │
│  └──────────────┘         └──────────────┘                 │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────┴──────────────────────────────────────┐
│                    Application Layer                         │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │          Service Orchestrators                          │ │
│  │  • IngestionService                                     │ │
│  │  • DatasetLineageService                                │ │
│  │  • EnrichmentServices                                   │ │
│  └────────────────────────────────────────────────────────┘ │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────┴──────────────────────────────────────┐
│                     Business Layer                           │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │              Data Source Handlers                       │ │
│  │  • CSVIngestionHandler                                  │ │
│  │  • MongoIngestionHandler                                │ │
│  │  • AvroIngestionHandler                                 │ │
│  │  • ParquetIngestionHandler                              │ │
│  │  • S3Handler                                            │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │              Business Services                          │ │
│  │  • Lineage Services                                     │ │
│  │  • Enrichment Services                                  │ │
│  │  • Profiling Services                                   │ │
│  │  • DQ Services                                          │ │
│  │  • RBAC Services                                        │ │
│  │  • Extraction Services                                  │ │
│  └────────────────────────────────────────────────────────┘ │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────┴──────────────────────────────────────┐
│                  Infrastructure Layer                        │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │            Platform Adapters                            │ │
│  │  • DataHubHandler                                       │ │
│  │  • DatabricksHandler (future)                           │ │
│  │  • AmundsenHandler (future)                             │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │              Common Utilities                           │ │
│  │  • ConfigManager                                        │ │
│  │  • Emitters                                             │ │
│  │  • URN Builders                                         │ │
│  │  • Utils                                                │ │
│  └────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

## Component Interactions

### Ingestion Flow

```
User Input (CLI/API)
    │
    ▼
ConfigManager loads configuration
    │
    ▼
PlatformFactory creates platform handler
    │
    ▼
IngestionService orchestrates
    │
    ▼
HandlerFactory creates appropriate handler
    │
    ▼
Handler extracts metadata
    │
    ▼
Handler formats metadata
    │
    ▼
Platform handler emits to catalog
    │
    ▼
Metadata available in catalog
```

### Lineage Flow

```
User Input (CLI/API)
    │
    ▼
ConfigManager loads lineage config
    │
    ▼
PlatformFactory creates platform handler
    │
    ▼
DatasetLineageService processes
    │
    ▼
Service validates URNs
    │
    ▼
Platform handler adds lineage
    │
    ▼
Lineage visible in catalog
```

## Key Design Patterns

### 1. Factory Pattern
**Where**: `PlatformFactory`, Handler registries
**Why**: Centralized object creation, easy to extend

```python
platform = PlatformFactory.get_instance("datahub", config)
```

### 2. Strategy Pattern
**Where**: Different handler implementations
**Why**: Interchangeable algorithms for different data sources

```python
handler = self._handler_registry.get(source_type)
```

### 3. Singleton Pattern
**Where**: Platform handler instances
**Why**: Reuse connections, consistent state

```python
if platform_lower in PlatformFactory._instances:
    return PlatformFactory._instances[platform_lower]
```

### 4. Template Method Pattern
**Where**: `BaseIngestionHandler`
**Why**: Define skeleton of algorithm, let subclasses override steps

```python
class BaseIngestionHandler:
    def validate_config(self):
        # Common validation logic
    
    def ingest(self):
        # Must be implemented by subclasses
        raise NotImplementedError
```

### 5. Dependency Injection
**Where**: Service constructors
**Why**: Loose coupling, testability

```python
class IngestionService:
    def __init__(self, config_manager, platform_handler):
        self.config_manager = config_manager
        self.platform_handler = platform_handler
```

## Extension Points

### Adding a New Data Source

1. **Create Handler**
   ```python
   class NewSourceHandler(BaseIngestionHandler):
       def ingest(self):
           # Implementation
   ```

2. **Register Handler**
   ```python
   self._handler_registry = {
       "newsource": NewSourceHandler
   }
   ```

3. **Add Configuration Template**
   - Create YAML template in `sample_configs_and_templates/`

### Adding a New Platform

1. **Implement Interface**
   ```python
   class NewPlatformHandler(MetadataPlatformInterface):
       def emit_mce(self, mce):
           # Implementation
   ```

2. **Register in Factory**
   ```python
   _handler_registry = {
       "newplatform": NewPlatformHandler
   }
   ```

3. **Update Global Config**
   - Add platform configuration section

## Data Flow

### Metadata Flow
```
Source → Handler → Platform Adapter → Metadata Catalog
```

### Configuration Flow
```
YAML File → ConfigManager → Cache → Services
```

### Lineage Flow
```
Config/API → LineageService → Platform Handler → Catalog
```

## Error Handling Strategy

1. **Validation Errors**: Caught early, return False/empty
2. **Configuration Errors**: Logged, raise ValueError
3. **Network Errors**: Logged with retry logic (future)
4. **Data Errors**: Logged, continue with next item

## Logging Strategy

- **INFO**: Normal operations, successful completions
- **WARNING**: Recoverable issues
- **ERROR**: Failures that prevent operation
- **DEBUG**: Detailed information for troubleshooting

## Configuration Management

### Hierarchy
1. Environment variables (highest priority)
2. Configuration files
3. Default values (lowest priority)

### Files
- `global_settings.yaml`: Platform connections
- Template configs: Operation-specific settings
- `.env`: Sensitive credentials

## Testing Strategy

### Unit Tests
- Test individual components in isolation
- Mock external dependencies
- Fast execution

### Integration Tests
- Test component interactions
- Use real implementations
- May require external services

## Performance Considerations

1. **Configuration Caching**: Avoid redundant file reads
2. **Connection Pooling**: Reuse platform connections (future)
3. **Batch Operations**: Process multiple items together (future)
4. **Async Processing**: Handle concurrent operations (future)

## Security Considerations

1. **Credential Management**: Use environment variables
2. **Input Validation**: Validate all external inputs
3. **Logging**: Never log sensitive data
4. **Dependencies**: Regular security scanning

## Scalability

### Current State
- Single-threaded processing
- Synchronous operations
- In-memory caching

### Future Enhancements
- Multi-threaded ingestion
- Async/await support
- Distributed caching
- Queue-based processing

## Maintainability

### Code Organization
- Clear module boundaries
- Consistent naming conventions
- Comprehensive docstrings
- Type hints throughout

### Documentation
- Architecture docs (this file)
- API documentation
- Example usage
- Contributing guidelines

## Future Architecture Enhancements

1. **Plugin System**: Dynamic handler loading
2. **Event System**: Pub/sub for extensibility
3. **Caching Layer**: Distributed cache (Redis)
4. **API Server**: REST API for remote access
5. **Web UI**: Configuration and monitoring interface
6. **Workflow Engine**: Complex data pipeline orchestration

