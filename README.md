## Lumos Framework Toolkit

Pluggable framework to catalog, version, and govern data with lineage, ownership and accountability — across any organization, any source. Lumos is platform-agnostic (works with DataHub today) and extensible via clean interfaces.

### Highlights
- Modular, plug-and-play services (ingestion, lineage, enrichment, profiling, DQ)
- Clear abstractions for metadata platforms (DataHub today) and sources (CSV, MongoDB, Avro)
- YAML-driven configs with a simple, typed `ConfigManager`
- CLI for common workflows
- Extensible handler and platform factories

### Repository Layout
- `platform_services/` — Platform implementations and factory (`DataHubHandler`, etc.)
- `core_library/`
  - `common/` — config management, utils, base interfaces
  - `ingestion_handlers/` — handlers for sources (CSV, MongoDB, Avro), `IngestionService`
  - `extraction_services/`, `enrichment_services/`, `lineage_services/`, `dq_services/`, `profiling_services/`, `rbac_services/`
- `configs/` — global settings (e.g. DataHub GMS)
- `sample_configs_and_templates/` — sample ingestion and lineage configs
- `orchestration_examples/` — example Airflow DAG
- `framework_cli.py` — CLI entrypoint (packaged later as `lumos`)

## Quickstart

Prereqs: Python 3.10+

1) Install dependencies
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

2) Configure DataHub endpoint
Edit `configs/global_settings.yaml`:
```yaml
datahub:
  gms_server: http://localhost:8080
default_env: DEV
```

3) Run ingestion via CLI
```bash
python framework_cli.py ingest --config-path sample_configs_and_templates/ingestion/csv_ingestion_template.yaml
```

4) Add lineage via CLI
```bash
python framework_cli.py add-lineage --config-path sample_configs_and_templates/lineage/dataset_lineage_template.yaml
```

## Configuration
- Global platform settings: `configs/global_settings.yaml`
- Ingestion job config: see `sample_configs_and_templates/ingestion/`
  - Example (CSV):
    ```yaml
    source:
      type: csv
      path: path/to/file.csv
      dataset_name: sample_dataset
      delimiter: ","
    sink:
      type: datahub
      env: DEV
    ```

## Extending
- Add a new source: create `<YourSource>IngestionHandler` in `core_library/ingestion_handlers/` extending `BaseIngestionHandler`, then register in `IngestionService._handler_registry`.
- Add a new metadata platform: implement `MetadataPlatformInterface` (e.g., `AmundsenHandler`), then register in `PlatformFactory._handler_registry`.

## Roadmap
- Additional sources: Parquet, S3, Kafka, DBs
- Additional platforms: Amundsen, Unity Catalog, OpenMetadata
- Packaging and `lumos` console script
- Pre-commit hooks, ruff/black/isort/mypy, CI

## Development
```bash
# (after packaging & tooling are added)
pre-commit run -a
pytest
```

## Contributing
See `CONTRIBUTING.md`. We follow Conventional Commits and welcome PRs!

## Code of Conduct
See `CODE_OF_CONDUCT.md`.

## License
MIT — see `LICENSE`.
