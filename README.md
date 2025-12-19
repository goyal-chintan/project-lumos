# Lumos Framework Toolkit

🚀 **Enterprise-grade framework for managing metadata lifecycle in DataHub**

A comprehensive, enterprise-ready Python framework for **ingesting metadata**, **managing ownership**, **enriching datasets**, **tracking lineage**, **versioning**, and **extracting analytics** from DataHub. Built with SOLID principles and modular architecture for maximum extensibility and maintainability.

## 🚀 Key Features

### 👤 **Ownership Management** ⭐ NEW
- **User Creation**: Create users with comprehensive profiles, teams, and skills
- **Group Management**: Organize teams with member/admin hierarchies  
- **Ownership Assignment**: Assign technical, business, and data steward ownership
- **3 Separate Operations**: Modular commands for specific tasks
- **Custom Ownership Types**: Support for organization-specific ownership models

### 📥 **Data Ingestion Pipeline**
- **Multi-Format Support**: CSV, Avro, Parquet, MongoDB, S3, PostgreSQL
- **Enterprise-Grade Avro Support**: Handles complex nested records, arrays, maps, and enums
- **Schema Inference**: Automatic schema detection for file-based sources
- **Flexible Configuration**: JSON-based configuration with validation

### 🏷️ **Metadata Enrichment**
- **Tag Management**: Apply business and technical tags
- **Documentation**: Add descriptions and business context
- **Properties**: Attach custom metadata and business rules
- **Glossary Integration**: Link to business terms

### 🔗 **Lineage Management**
- **Dataset Lineage**: Track data transformations and dependencies
- **Data Job Lineage**: Pipeline and job relationships
- **Impact Analysis**: Understand downstream effects

### 📈 **Version Management**
- **Cloud Version Tracking**: S-311 → S-312 progression
- **Schema Versioning**: Semantic versioning (1.0.0 → 2.0.0)
- **Bulk Version Updates**: Update all datasets simultaneously

### 🔍 **Advanced Extraction & Analytics**
- **11 Extraction Types**: From comprehensive to specialized analysis
- **4 Output Formats**: JSON, Excel, CSV, Visualizations
- **Direct Export**: No intermediate files needed
- **Chained Operations**: Multiple extractions in sequence

### 🏗️ **Framework Features**
- **DataHub Integration**: Native support for DataHub's metadata platform
- **CLI Interface**: Easy-to-use command-line interface for operations
- **Modular Architecture**: Extensible handler system following SOLID principles
- **Comprehensive Logging**: Detailed logging with configurable levels
- **Error Resilience**: Robust exception handling and graceful fallbacks

📖 **For detailed architecture documentation, see:**
- [Architecture Overview](docs/ARCHITECTURE.md) - Complete architecture and component details
- [End-to-End Flow](docs/END_TO_END_FLOW.md) - Detailed flow diagrams and sequences
- [Architecture Diagrams](docs/ARCHITECTURE_DIAGRAM.md) - Visual diagrams and component interactions
- [Quick Reference](docs/QUICK_REFERENCE.md) - Quick reference guide for common operations

## 📁 Project Structure

```
datahub_framework/
├── core/                           # Core framework components
│   ├── common/                     # Shared utilities and interfaces
│   ├── controllers/                # Operation controllers
│   └── platform/                   # Platform interface implementations
├── feature/                        # Feature-specific modules
│   ├── ownership/                  # ⭐ NEW: Ownership management
│   │   ├── base_ownership_service.py    # Abstract base class
│   │   └── ownership_service.py         # DataHub implementation
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
│   ├── extraction/                 # ⭐ Metadata extraction & analytics
│   │   ├── export/                 # Export to Excel, CSV, Charts
│   │   ├── base_extraction_service.py
│   │   ├── extraction_factory.py   # 11 extraction types
│   │   └── comprehensive_dataset_extractor.py
│   ├── enrichment/                 # Metadata enrichment services
│   ├── lineage/                    # Data lineage tracking
│   ├── versioning/                 # Version management
│   ├── profiling/                  # Data profiling services
│   ├── dq_services/                # Data quality services
│   └── rbac/                       # Access control services
├── configs/                        # Configuration files
├── sample_configs_and_templates/   # Example configurations
│   ├── ownership/                  # ⭐ NEW: Ownership config templates
│   │   ├── user_template.json           # User creation template
│   │   ├── group_template.json          # Group creation template
│   │   └── ownership_assignment_template.json  # Ownership assignment
│   ├── ingestion/                  # Ingestion configurations
│   ├── enrichment/                 # Enrichment configurations
│   └── lineage/                    # Lineage configurations
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

#### 👤 **Ownership Operations** ⭐ NEW
```bash
# Create users in DataHub
python framework_cli.py create-users:sample_configs_and_templates/ownership/user_template.json

# Create groups in DataHub
python framework_cli.py create-groups:sample_configs_and_templates/ownership/group_template.json

# Assign ownership to datasets
python framework_cli.py assign-ownership:sample_configs_and_templates/ownership/ownership_assignment_template.json
```

#### 📥 **Ingestion Operations**
```bash
# Ingest metadata from data sources
python framework_cli.py ingest:path/to/config.json
```

#### 🔗 **Lineage Operations**
```bash
# Add dataset lineage
python framework_cli.py add-lineage:path/to/lineage_config.json

# Add data job lineage
python framework_cli.py add-data-job-lineage:path/to/job_lineage_config.json
```

#### 🏷️ **Enrichment Operations**
```bash
# Enrich metadata
python framework_cli.py enrich:path/to/enrichment_config.json
```

#### 📈 **Version Operations**
```bash
# Update versions for all datasets (S-311 → S-312, 1.0.0 → 2.0.0)
python framework_cli.py version-update

# Scan and display dataset summary
python framework_cli.py datasets-summary-scan
```

#### 🔍 **Extraction Operations** ⭐ NEW
```bash
# Extract to Excel format
python framework_cli.py extract:excel-comprehensive
python framework_cli.py extract:excel-governance

# Extract to CSV format
python framework_cli.py extract:csv-schema
python framework_cli.py extract:csv-quality

# Extract to Charts/Visualizations
python framework_cli.py extract:charts-comprehensive

# Extract to JSON (traditional format)
python framework_cli.py extract:json-lineage

# Chain multiple operations
python framework_cli.py create-users:users.json ingest:data.json extract:excel-comprehensive
```

### 👤 **Ownership Configuration Examples**

#### User Creation Configuration
```json
{
  "operation": "create_users",
  "description": "Create users in DataHub",
  "users": [
    {
      "username": "john.doe",
      "display_name": "John Doe",
      "first_name": "John",
      "last_name": "Doe",
      "email": "john.doe@company.com",
      "title": "Senior Data Engineer",
      "department_name": "Data Platform",
      "teams": ["data-platform", "analytics"],
      "skills": ["Python", "SQL", "Apache Spark"],
      "manager_urn": "urn:li:corpuser:jane.smith",
      "custom_properties": {
        "employee_id": "EMP001",
        "location": "San Francisco"
      }
    }
  ]
}
```

#### Group Creation Configuration
```json
{
  "operation": "create_groups",
  "description": "Create groups in DataHub",
  "groups": [
    {
      "name": "data-platform",
      "display_name": "Data Platform Team",
      "description": "Team responsible for data infrastructure",
      "email": "data-platform@company.com",
      "slack_channel": "#data-platform",
      "members": ["john.doe", "jane.smith"],
      "admins": ["jane.smith"],
      "parent_groups": ["engineering"]
    }
  ]
}
```

#### Ownership Assignment Configuration
```json
{
  "operation": "assign_ownership",
  "description": "Assign ownership to datasets",
  "assignments": [
    {
      "owner_name": "john.doe",
      "owner_category": "user",
      "ownership_type": "TECHNICAL_OWNER",
      "entity": {
        "datatype": "csv",
        "dataset_name": "user_behavior_data",
        "env": "PROD"
      }
    },
    {
      "owner_name": "data-platform",
      "owner_category": "group",
      "ownership_type": "DATA_STEWARD",
      "entity": {
        "datatype": "avro",
        "dataset_name": "transaction_events",
        "env": "PROD"
      }
    }
  ]
}
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

4. **Set up ownership (optional):**
   ```bash
   # Create users and assign ownership
   python framework_cli.py create-users:sample_configs_and_templates/ownership/user_template.json
   python framework_cli.py assign-ownership:sample_configs_and_templates/ownership/ownership_assignment_template.json
   ```

5. **Extract and analyze:**
   ```bash
   # Get comprehensive analysis in Excel format
   python framework_cli.py extract:excel-comprehensive
   ```

6. **View in DataHub:**
   Open http://localhost:9002 to see your ingested datasets

## 🏗 Architecture

The framework follows a **modular, service-oriented architecture** with **SOLID principles**:

```
📋 Configuration → 🎮 Controllers → 🔧 Services → 🌐 Platform Handlers → 📊 DataHub
```

### Core Architecture Components

#### **1. Configuration Layer**
- **Global Settings**: `configs/global_settings.yaml` for DataHub connection
- **Operation Configs**: JSON files for specific operations (users, groups, assignments, ingestion, etc.)
- **Templates**: Ready-to-use examples in `sample_configs_and_templates/`

#### **2. Controller Layer** (`core/controllers/`)
- **Ownership Controllers**: 3 separate functions for users, groups, and assignments
- **Ingestion Controller**: Handles data source ingestion
- **Enrichment Controller**: Manages metadata enrichment
- **Lineage Controllers**: Dataset and data job lineage
- **Version Controller**: Schema and cloud version management
- **Extraction Controller**: Metadata extraction and analytics

#### **3. Service Layer** (`feature/`)
- **Ownership Service**: User/group creation, ownership assignment (SOLID design)
- **Ingestion Service**: Data source processing and schema extraction
- **Enrichment Services**: Tag, property, and documentation management
- **Extraction Services**: 11 specialized extractors for different metadata types
- **Lineage Services**: Relationship tracking and impact analysis

#### **4. Platform Layer** (`core/platform/`)
- **DataHub Handler**: REST API integration, authentication, error handling
- **Platform Interface**: Abstract interface for future platform support
- **Factory Pattern**: Platform-agnostic service instantiation

### Data Flow Patterns

#### **Ownership System (WRITE to DataHub)**
```
User JSON → Validation → OwnershipService → DataHub Handler → DataHub GMS → Users/Groups/Ownership
```

#### **Extraction System (READ from DataHub)**
```
CLI Command → ExtractionFactory → Specific Extractor → DataHub Query → Processing → Export (Excel/CSV/Charts/JSON)
```

#### **Ingestion System (WRITE to DataHub)**
```
Source Config → Handler Factory → Source-Specific Handler → Schema Extraction → DataHub Emission
```

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

### **Organizational Setup**
- **User Management**: Create comprehensive user profiles with teams and skills
- **Team Organization**: Set up groups with proper member/admin hierarchies
- **Ownership Assignment**: Assign clear data ownership for governance and accountability

### **Data Operations**
- **Data Discovery**: Automatically catalog datasets across your organization
- **Schema Evolution**: Track schema changes over time
- **Data Lineage**: Understand data flow and dependencies
- **Compliance**: Maintain metadata for regulatory requirements
- **Data Quality**: Profile and monitor data quality metrics

### **Analytics & Reporting**
- **Executive Dashboards**: Visual reports on data governance and quality
- **Business Analysis**: Excel exports for business stakeholders
- **Technical Analysis**: CSV exports for data scientists and analysts
- **Comprehensive Audits**: Complete metadata extraction for compliance

## 🤝 Contributing

We welcome contributions! Please follow our contribution guidelines:

1. **Fork the repository**
2. **Set up development environment:**
   ```bash
   pip install -r requirements-dev.txt
   pre-commit install
   ```
3. **Create a feature branch** following naming conventions:
   - `feature/19-your-feature-name`
   - `fix/15-your-bug-fix`
   - `docs/8-your-documentation-update`
4. **Make your changes** with tests
5. **Ensure code quality:**
   - All pre-commit hooks pass
   - Code follows style guidelines (Black, Ruff)
   - Tests pass with 70%+ coverage
   - Commit messages follow Conventional Commits
6. **Submit a pull request** that links the issue (e.g., `Closes #19`)

📖 **See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines**  
📋 **See [CODE_QUALITY_STANDARDS.md](CODE_QUALITY_STANDARDS.md) for quality standards**


## 👥 Authors

- **Chintan Goyal** ([@goyal-chintan](https://github.com/goyal-chintan)) - *Creator & Architect*
  - Conceived the framework based on 8+ years of data industry experience
  - Designed the platform-agnostic architecture and modular system design
  - Lead developer

### Contributors

Special thanks to the developers who helped build and implement this vision:
- **Sushant** - *Developer*
- **Tanaya** - *Developer*
- **Nagesh** - *Developer*

## 📞 Support

For issues and questions:

### **Configuration Help**
1. Check the sample configurations in `sample_configs_and_templates/`
2. Use provided templates as starting points for your configurations
3. Review operation-specific JSON structure requirements

### **Troubleshooting**
1. Review logs for detailed error information
2. Ensure DataHub is running and accessible at the configured URL
3. Verify file paths and permissions for data sources
4. Check network connectivity to DataHub GMS server

### **Ownership System Issues**
- **Known Limitations** (see `ownership_limitations.txt`):
  - User assignment to groups is not working
  - New ownership type cannot be created
  - User assignment to role is not working

### **Getting Help**
1. Check framework CLI help: `python framework_cli.py --help`
2. Use test mode for validation: Set `test_mode: true` in global settings
3. Run operations individually to isolate issues
4. Check DataHub UI at http://localhost:9002 for results

---

**Built for Enterprise Data Teams** 🏢  
**🚀 Complete Metadata Lifecycle Management | 👤 Ownership | 📊 Analytics | 🔗 Lineage | ⚡ SOLID Architecture**
