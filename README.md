# Lumos Framework Toolkit

A modular and extensible framework for data ingestion, metadata management, and lineage tracking. The framework is designed to be platform-agnostic, allowing easy switching between different metadata platforms (e.g., DataHub, Databricks).

## Architecture

The framework follows a modular architecture with clear separation of concerns:

- `platform_services/`: Platform-specific implementations (DataHub, Databricks, etc.)
- `core_library/`: Core framework components
  - `common/`: Shared utilities and interfaces
  - `ingestion_handlers/`: Data source ingestion handlers
  - `extraction_services/`: Schema and metadata extraction
  - `enrichment_services/`: Metadata enrichment
  - `lineage_services/`: Lineage tracking
  - `dq_services/`: Data quality
  - `profiling_services/`: Data profiling
  - `rbac_services/`: Access control
- `configs/`: Configuration files
- `sample_configs_and_templates/`: Example configurations
- `orchestration_examples/`: Example orchestration workflows

## Key Features

- Platform-agnostic design
- Modular and extensible architecture
- Type-safe interfaces
- Comprehensive error handling
- Configurable through YAML
- Support for multiple data sources
- Built-in logging and monitoring

## Installation

```bash
pip install -r requirements.txt
```

## Usage

1. Configure your platform and data sources in `configs/`
2. Create an ingestion handler for your data source
3. Use the framework to ingest and manage metadata

Example:

```python
from core_library.ingestion_handlers.csv_handler import CSVIngestionHandler
from core_library.common.config_manager import ConfigManager

# Initialize config manager
config_manager = ConfigManager()

# Create CSV handler
csv_handler = CSVIngestionHandler({
    "file_path": "data/example.csv",
    "delimiter": ",",
    "platform": "datahub"
}, config_manager)

# Ingest data
csv_handler.ingest("data/example.csv")
```

## Development

1. Install development dependencies:
```bash
pip install -r requirements-dev.txt
```

2. Run tests:
```bash
pytest
```

3. Format code:
```bash
black .
```

4. Type checking:
```bash
mypy .
```

## Running tests

- Unit tests (fast): 
```bash
pytest -q
```
- Integration tests with your local DataHub:
```bash
export DATAHUB_GMS=http://localhost:8080
pytest -m integration -q
```
CI runs unit tests by default and skips `integration` unless `DATAHUB_GMS` is provided.

## Recommended Cursor model setup

- Primary model: Claude 3.7 Sonnet (best for long-context multi-file refactors)
- Secondary model: OpenAI o4-mini (or GPT-4o) for fast codegen and test-writing

Tip: keep edits small, run `pytest -q` after significant changes, and ask the model to propose tests before refactors.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

MIT License

Ingestion
Enrichment
DQ
