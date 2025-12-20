# Roadmap (Vision-Aligned, No Fixed Dates)

This roadmap consolidates the phased plans across proposal packs. Phases are ordered by dependency and value, not by committed dates.

## Guiding Constraints

- Preserve the current architecture: `framework_cli.py` -> `core/controllers/*` -> `feature/*` -> `core/platform/*`
- Catalog-backed (DataHub today; adapters later)
- Metadata-only versioning for MVP
- Focus on portability, automation, and CI enforcement

## Phase A: Portable Snapshots + Real Diffs

Deliverables:
- Canonical Lumos snapshot schema
- Snapshot export/import (filesystem store first)
- Diff engine for schema/ownership/docs/governance/lineage
- JSON-first reports (reuse existing exporters later)

Acceptance:
- Export snapshots for all datasets
- Diff two snapshots and classify breaking vs non-breaking vs info
- Demonstrate migration safety with portable artifacts

## Phase B: Platform-Agnostic Read-Path

Deliverables:
- Extend `core/platform/interface.py` for read/search APIs
- Implement read-path in DataHub handler
- Refactor feature services to avoid direct DataHub API calls

Acceptance:
- Platform adapter is the only read-path for extraction and snapshot export

## Phase C: CI-Driven Versioning + Change Gates

Deliverables:
- Diff-based semantic version bumps
- Jenkins and GitHub Actions templates
- Fail CI on breaking changes unless approved

Acceptance:
- CI job can export snapshots, diff, and gate changes

## Phase D: Storage-First Auto-Discovery

Deliverables:
- S3/local discovery crawlers
- Auto-generate ingestion configs
- Schema inference on discovery

Acceptance:
- Given a prefix, Lumos discovers datasets and updates the catalog without manual registration

## Phase E: Cost + Impact Allocation

Deliverables:
- Cost input model (config-driven first)
- Lineage-based allocation engine
- Executive reports: top costly datasets, cost by team/domain, feature-off savings

Acceptance:
- Export a FinOps report with cost per output dataset and major cost drivers

## Phase F: Second Catalog Adapter

Deliverables:
- OpenMetadata adapter (recommended first)
- Prove portability across two catalogs using snapshots

Acceptance:
- Same Lumos workflow works with two catalogs using only config changes

## Priority Bands (From Gap Analysis)

P0 (must-have):
- Automatic change detection
- Real schema tracking (history + diff)
- Snapshot format + export/import
- CTO-grade documentation

P1 (high value):
- Cost attribution enablement
- Additional platform adapters (Amundsen, Atlas, OpenMetadata, Snowflake)
- CI/CD integration

P2 (nice to have):
- Cross-platform catalog sync
- Governance dashboard
- Streaming ingestion connectors

## Success Metrics (Targets)

Technical:
- 95%+ auto-detection of new datasets
- <5 minute schema-change detection latency
- 99% snapshot import/export success
- 90%+ cost attribution accuracy (config-driven first)

Business:
- CTO understanding in <30 seconds
- Time to first value <1 hour
- 10+ enterprise adoptions in 6 months
