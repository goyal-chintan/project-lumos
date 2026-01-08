# Lumos

Not a catalog—it's the **middleware layer** that automates metadata hygiene across any catalog (DataHub today, catalog-agnostic by design).

Lumos keeps catalog metadata accurate without manual effort: ingest from sources, enrich with governance context, attach lineage and ownership, version metadata changes, and extract reports.

## Why Lumos

Metadata becomes stale because it depends on people and scattered tooling. Stale metadata leads to duplicated datasets, broken downstream pipelines, unclear ownership, and governance drift.

One key pain we explicitly target: **release-time metadata refresh** (releases can happen mid-week even when pipelines run weekly).

## Who It’s For

- CTO / engineering leadership: inventory, ownership coverage, change risk, and (eventually) cost/impact visibility.
- Data platform + data engineering: repeatable ingestion, schema/change hygiene, lineage/ownership automation.
- Governance/security: clearer accountability, documented context, and safer migrations over time.
- OSS contributors: modular architecture and clear extension points (handlers + platform adapters).

## What Makes Lumos Different

- Catalog-backed: Lumos complements a catalog UI (DataHub today) rather than replacing it.
- Middleware, not another UI: focus on automation, enforcement, and portability primitives.
- Configuration-driven: repeatable runs and consistent behavior across environments.
- Storage-first: file and lake coverage alongside traditional sources.
- Practical operations: designed to be cheap to run for hundreds to low-thousands of datasets.

## What Exists Today (Python Framework)

- Ingestion: CSV, Avro, Parquet, S3, MongoDB, PostgreSQL
- Enrichment: tags, documentation, properties
- Lineage: dataset + data job lineage
- Ownership: users, groups, assignments
- Versioning: scan + bulk updates
- Extraction: multiple extractors + Excel/CSV/Charts outputs

## Business Value (Cost-Effective by Design)

Lumos is open source and intentionally lightweight. For many organizations, the main “cost” isn’t storage of metadata; it’s:
- engineering time lost to finding data, understanding schemas, and chasing owners
- duplicated datasets because “we couldn’t find the right one”
- incidents caused by untracked changes and unclear impact

**Simple ROI model (use your org numbers):**
- Saved engineering time = `(engineers × hours/week saved × hourly rate × 52)`
- Reduced duplicate compute/storage = `(duplicate datasets avoided × avg annual cost per dataset)`
- Reduced incident cost = `(incidents avoided × hours/incident × hourly rate)`

## Architecture (Today + Direction)

- Today: Python CLI → controllers → feature services → handlers → DataHub adapter.
- Planned: Spring Boot control plane (scheduler/job registry/APIs) + Python workers (ingestion/extraction), connected via Redis Streams.

Docs:
- Vision: `docs/VISION.md`
- Architecture: `docs/ARCHITECTURE.md`
- Decision log: `docs/DECISIONS.md`
 - Deep dives: `docs/vision/01_problem_statement.md`, `docs/vision/07_competitive_analysis.md`, `docs/vision/08_business_value.md`, `docs/vision/10_ai_roadmap.md`, `docs/vision/12_migration_guide.md`, `docs/vision/13_cost_attribution.md`

## Why Spring Boot + Python (Planned)

This is a control-plane vs data-plane split:
- Spring Boot is for long-running control-plane concerns (APIs, auth/RBAC, retries, audit trails, scheduling).
- Python stays for file/format-heavy ingestion and extraction (high-leverage ecosystem; existing handlers).
- Redis Streams is the lightweight job queue (acks + consumer groups) without Kafka-level operational overhead.

## Design Goals

- Automation-first metadata hygiene (minimize manual updates).
- Configuration-driven operations (repeatable runs, easy adoption).
- Platform adapters to avoid catalog lock-in over time.
- Cheap to operate for small-to-mid footprints (hundreds to low-thousands of datasets).

## Roadmap (High Level)

- Release workflows (release-triggered refresh + backfill patterns).
- Control plane + workers (Spring Boot + Python) with Redis Streams.
- Portable snapshot + diff primitives (to power versioning and CI gates).
- Optional AI layer: RAG over metadata snapshots + agents for impact/ownership/quality (after snapshot/diff foundations).

## FAQ

**Why not Kafka?** For Lumos job dispatch, Redis Streams is cheaper and simpler to operate on small footprints; Kafka becomes attractive when you need high throughput, long retention/replay, and multiple independent consumers.

**Why not “all Java” or “all Python”?** File/format-heavy ingestion is high leverage in Python; long-running orchestration and APIs are high leverage in Spring Boot. The split keeps each concern in its best ecosystem.

**Do both services need to run all the time?** In the planned model, the Spring Boot control plane runs continuously; Python runs as an always-on worker for simplicity (on-demand execution can be added later).

## 10-Minute Demo (CTO-Friendly)

1) Ingest a sample dataset
```bash
python framework_cli.py ingest:sample_configs_and_templates/ingestion/test_ingestion.json
```

2) Add ownership
```bash
python framework_cli.py create-users:sample_configs_and_templates/ownership/user_template.json
python framework_cli.py assign-ownership:sample_configs_and_templates/ownership/ownership_assignment_template.json
```

3) Add lineage
```bash
python framework_cli.py add-lineage:sample_configs_and_templates/lineage/dataset_lineage_template.json
```

4) Extract a report
```bash
python framework_cli.py extract:excel-comprehensive
```

5) Validate in DataHub UI
- Open `http://localhost:9002` and inspect dataset metadata, lineage, and ownership.

## Quickstart

### Prerequisites
- Python 3.8+
- A DataHub instance (GMS URL)

### Install
```bash
pip install -r requirements.txt
```

### Configure DataHub
Edit `configs/global_settings.yaml`:
```yaml
datahub:
  gms_server: http://localhost:8080
  test_mode: false

default_env: DEV
default_platform: datahub
```

### Run a sample ingestion
```bash
python framework_cli.py ingest:sample_configs_and_templates/ingestion/test_ingestion.json
```

## CLI Operations (Examples)

```bash
# Ownership
python framework_cli.py create-users:sample_configs_and_templates/ownership/user_template.json
python framework_cli.py create-groups:sample_configs_and_templates/ownership/group_template.json
python framework_cli.py assign-ownership:sample_configs_and_templates/ownership/ownership_assignment_template.json

# Lineage
python framework_cli.py add-lineage:sample_configs_and_templates/lineage/dataset_lineage_template.json
python framework_cli.py add-data-job-lineage:sample_configs_and_templates/lineage/data_job_lineage_template.json

# Enrichment
python framework_cli.py enrich:sample_configs_and_templates/enrichment/csv_enrichments.json

# Versioning
python framework_cli.py version-update

# Extraction
python framework_cli.py extract:excel-comprehensive
python framework_cli.py extract:csv-quality
python framework_cli.py extract:charts-comprehensive
```

## Contributing

- Contribution guide: `CONTRIBUTING.md`
- Code quality standards: `CODE_QUALITY_STANDARDS.md`
- Changelog: `CHANGELOG.md`

## License

Apache-2.0. See `LICENSE`.
