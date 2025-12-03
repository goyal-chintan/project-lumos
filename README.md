# 🌟 Lumos Framework

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)

> A pluggable, extensible framework for enterprise data cataloging, lineage tracking, and metadata management.

Lumos Framework empowers organizations to maintain comprehensive data governance by providing a platform-agnostic solution for cataloging data assets, tracking lineage, ensuring data quality, and managing metadata across diverse data sources.

## ✨ Key Features

- 🔌 **Platform-Agnostic**: Seamlessly integrate with multiple metadata platforms (DataHub, Databricks, and more)
- 🏗️ **Modular Architecture**: Clean separation of concerns following SOLID principles
- 🔄 **Extensible Design**: Easily add new data sources and platforms through handler pattern
- 📊 **Multi-Source Support**: Built-in handlers for CSV, MongoDB, Avro, Parquet, S3, and more
- 🔗 **Lineage Tracking**: Comprehensive data lineage with upstream/downstream relationships
- ✅ **Data Quality**: Built-in data quality assertion services
- 📝 **Metadata Enrichment**: Tag, document, and describe your data assets
- 🎯 **Type-Safe**: Extensive use of type hints for better code quality
- 🛡️ **RBAC Support**: Role-based access control services
- 📈 **Data Profiling**: Statistical profiling for datasets
- ⚙️ **YAML Configuration**: Simple, declarative configuration files
- 🚀 **CLI Interface**: Easy-to-use command-line interface

## 🏛️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        CLI Interface                         │
│                      (framework_cli.py)                      │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────┴──────────────────────────────────────┐
│                    Core Library                              │
├──────────────────────────────────────────────────────────────┤
│  ┌────────────────────────────────────────────────────────┐ │
│  │  Ingestion Handlers                                     │ │
│  │  (CSV, MongoDB, Avro, Parquet, S3, ...)               │ │
│  └────────────────────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  Services Layer                                         │ │
│  │  • Lineage Services    • Enrichment Services           │ │
│  │  • Profiling Services  • DQ Services                   │ │
│  │  • RBAC Services       • Extraction Services           │ │
│  └────────────────────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  Common Utilities                                       │ │
│  │  (Config Manager, Emitters, URN Builders, ...)        │ │
│  └────────────────────────────────────────────────────────┘ │
└──────────────────────┬──────────────────────────────────────┘
                       │
┌──────────────────────┴──────────────────────────────────────┐
│              Platform Services (Adapters)                    │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │   DataHub    │  │  Databricks  │  │   Custom     │     │
│  │   Handler    │  │   Handler    │  │   Platforms  │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└──────────────────────────────────────────────────────────────┘
```

### Design Principles

- **Single Responsibility Principle (SRP)**: Each class has one well-defined purpose
- **Open/Closed Principle (OCP)**: Easily extend with new handlers without modifying core code
- **Dependency Inversion Principle (DIP)**: Services depend on abstractions, not concrete implementations
- **Interface Segregation Principle (ISP)**: Clean, focused interfaces for each concern

## 🚀 Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/lumos-framework.git
cd lumos-framework

# Install dependencies
pip install -r requirements.txt

# Or install as a package
pip install -e .
```

### Configuration

1. **Configure your metadata platform** in `configs/global_settings.yaml`:

```yaml
datahub:
  gms_server: http://localhost:8080

default_env: DEV
```

2. **Create an ingestion configuration** (e.g., `my_ingestion.yaml`):

```yaml
source:
  type: csv
  path: "data/sample_data.csv"
  dataset_name: "my_dataset"
  delimiter: ","

sink:
  type: datahub
  env: DEV
```

### Basic Usage

#### Command Line Interface

```bash
# Ingest data from a CSV file
lumos ingest --config-path sample_configs_and_templates/ingestion/csv_ingestion_template.yaml

# Add dataset lineage
lumos add-lineage --config-path sample_configs_and_templates/lineage/dataset_lineage_template.yaml
```

#### Python API

```python
from core_library.ingestion_handlers.csv_handler import CSVIngestionHandler
from core_library.common.config_manager import ConfigManager
from platform_services.platform_factory import PlatformFactory

# Initialize configuration
config_manager = ConfigManager()
global_config = config_manager.get_global_config()

# Get platform handler
platform_handler = PlatformFactory.get_instance("datahub", global_config["datahub"])

# Create and configure CSV handler
csv_config = {
    "source": {
        "type": "csv",
        "path": "data/example.csv",
        "dataset_name": "my_dataset",
        "delimiter": ","
    },
    "sink": {
        "type": "datahub",
        "env": "DEV"
    }
}

# Ingest data
csv_handler = CSVIngestionHandler(csv_config, platform_handler)
csv_handler.ingest()
```

## 📚 Documentation

### Supported Data Sources

| Data Source | Handler | Status |
|-------------|---------|--------|
| CSV Files | `CSVIngestionHandler` | ✅ Stable |
| MongoDB | `MongoIngestionHandler` | ✅ Stable |
| Apache Avro | `AvroIngestionHandler` | ✅ Stable |
| Apache Parquet | `ParquetIngestionHandler` | ✅ Stable |
| Amazon S3 | `S3Handler` | ✅ Stable |

### Supported Platforms

| Platform | Handler | Status |
|----------|---------|--------|
| DataHub | `DataHubHandler` | ✅ Stable |
| Databricks | - | 🚧 Planned |
| Amundsen | - | 🚧 Planned |
| Custom | `MetadataPlatformInterface` | ✅ Extensible |

### Core Services

#### 📊 Ingestion Services
Ingest metadata from various data sources into your catalog.

#### 🔗 Lineage Services
Track and visualize data lineage relationships between datasets.

#### 🏷️ Enrichment Services
- **Description Service**: Add descriptions to datasets and fields
- **Documentation Service**: Attach comprehensive documentation
- **Tag Service**: Organize with tags and labels
- **Properties Service**: Add custom properties

#### ✅ Data Quality Services
Define and track data quality assertions and checks.

#### 📈 Profiling Services
Generate statistical profiles for your datasets.

#### 🔐 RBAC Services
Manage access control policies for data assets.

## 🛠️ Adding Custom Handlers

### Adding a New Data Source

1. Create a new handler inheriting from `BaseIngestionHandler`:

```python
from core_library.ingestion_handlers.base_ingestion_handler import BaseIngestionHandler

class MyCustomHandler(BaseIngestionHandler):
    def __init__(self, config, platform_handler):
        super().__init__(config, platform_handler)
        self.required_fields.extend(["custom_field"])
    
    def ingest(self) -> None:
        # Your ingestion logic here
        pass
```

2. Register it in `IngestionService`:

```python
self._handler_registry = {
    "csv": CSVIngestionHandler,
    "mongodb": MongoIngestionHandler,
    "mycustom": MyCustomHandler,  # Add your handler
}
```

### Adding a New Platform

1. Create a handler implementing `MetadataPlatformInterface`:

```python
from platform_services.metadata_platform_interface import MetadataPlatformInterface

class MyPlatformHandler(MetadataPlatformInterface):
    def __init__(self, config):
        self.config = config
    
    def emit_mce(self, mce):
        # Implementation
        pass
    
    def emit_mcp(self, mcp):
        # Implementation
        pass
    
    def add_lineage(self, upstream_urn, downstream_urn):
        # Implementation
        pass
```

2. Register in `PlatformFactory`:

```python
_handler_registry = {
    "datahub": DataHubHandler,
    "myplatform": MyPlatformHandler,  # Add your platform
}
```

## 🧪 Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=core_library --cov=platform_services

# Run specific test file
pytest tests/test_ingestion.py
```

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

1. Fork the repository
2. Create a feature branch (`git checkout -b feat/amazing-feature`)
3. Commit your changes (`git commit -m 'feat: add amazing feature'`)
4. Push to the branch (`git push origin feat/amazing-feature`)
5. Open a Pull Request

Please read our [Code of Conduct](CODE_OF_CONDUCT.md) before contributing.

## 📋 Roadmap

- [ ] Support for Databricks Unity Catalog
- [ ] Apache Atlas integration
- [ ] Amundsen platform support
- [ ] PostgreSQL/MySQL ingestion handlers
- [ ] Kafka metadata extraction
- [ ] Advanced data quality rules engine
- [ ] Web UI for configuration management
- [ ] REST API server mode
- [ ] Docker containerization
- [ ] Kubernetes operator

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built with ❤️ for the data engineering community
- Inspired by modern data governance needs
- Special thanks to all contributors

## 📞 Support

- 📖 [Documentation](https://github.com/yourusername/lumos-framework#readme)
- 🐛 [Issue Tracker](https://github.com/yourusername/lumos-framework/issues)
- 💬 [Discussions](https://github.com/yourusername/lumos-framework/discussions)

## 🌟 Star History

If you find this project useful, please consider giving it a star ⭐

---

**Made with ❤️ by the Lumos Framework community**
