# Changelog

All notable changes to the Lumos Framework will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial open-source release preparation
- Comprehensive documentation
- Package configuration for PyPI distribution

## [0.1.0] - 2025-12-03

### Added
- Core framework architecture with modular design
- Platform-agnostic metadata platform interface
- DataHub platform handler implementation
- Multiple data source ingestion handlers:
  - CSV file handler
  - MongoDB handler
  - Apache Avro handler
  - Apache Parquet handler
  - Amazon S3 handler
- Lineage tracking services
  - Dataset lineage service
  - Data job lineage service
- Enrichment services
  - Description service
  - Documentation service
  - Properties service
  - Tag service
- Data quality assertion service
- Data profiling service
- RBAC policy service
- Schema extraction service
- CLI interface for ingestion and lineage operations
- YAML-based configuration system
- Comprehensive logging and error handling
- Configuration templates for all supported sources
- Example Airflow DAG for orchestration
- MIT License
- Code of Conduct (Contributor Covenant)
- Contributing guidelines
- Development tools configuration (black, isort, mypy, pytest)

### Changed
- Fixed typo: renamed `data-catalouge.py` to `data-catalog.py`
- Updated configuration templates to use generic examples instead of personal paths
- Improved README with architecture diagrams and comprehensive documentation

### Infrastructure
- Added `.gitignore` for Python projects
- Added `setup.py` for package distribution
- Added `pyproject.toml` for modern Python packaging
- Added `MANIFEST.in` for including non-Python files
- Added development dependencies in `requirements-dev.txt`
- Removed `__pycache__` directories from version control

### Documentation
- Enhanced README with badges, architecture diagrams, and examples
- Added CONTRIBUTING.md with development guidelines
- Added CODE_OF_CONDUCT.md
- Added configuration template examples
- Added inline documentation and docstrings

## Release Notes

### Version 0.1.0 - Initial Release

This is the first public release of Lumos Framework, providing a solid foundation for enterprise data cataloging and metadata management. The framework is production-ready with the following capabilities:

**Core Capabilities:**
- Ingest metadata from multiple data sources
- Track lineage relationships between datasets
- Enrich metadata with descriptions, tags, and properties
- Profile datasets for statistical information
- Define and track data quality assertions
- Manage access control policies

**Platform Support:**
- DataHub (stable)
- Extensible architecture for adding new platforms

**Data Source Support:**
- CSV files
- MongoDB collections
- Apache Avro files
- Apache Parquet files
- Amazon S3 objects

**Known Limitations:**
- Databricks Unity Catalog support is planned but not yet implemented
- Web UI not available (CLI and Python API only)
- Limited to DataHub platform in this release

**Migration Notes:**
- This is the initial release, no migrations needed

---

## How to Contribute

See [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to contribute to this project.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/yourusername/lumos-framework/tags).

