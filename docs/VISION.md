# Lumos Vision

Lumos is a metadata automation middleware layer that keeps a catalog accurate (DataHub today) without relying on manual updates. It is designed to be catalog-agnostic: the catalog remains the UI/registry; Lumos is the automation + portability + enforcement plane.

## What Lumos Is
- A **middleware layer** between sources (files, DBs, pipelines) and any catalog.
- Automation + portability + CI enforcement for metadata: ingestion, enrichment, lineage, ownership, versioning, and extraction/reporting.
- A foundation for portable snapshots + diffs (roadmap) so metadata can outlive any single catalog.

## What Lumos Is Not
- Not a replacement for the catalog UI.
- Not full data artifact version control (MVP is metadata-focused).
- Not a manual governance workflow that depends on humans to keep things updated.

## The Core Problem
Metadata becomes stale because it depends on people and scattered tooling. Stale metadata leads to:
- low discoverability and duplicated datasets
- broken downstream pipelines due to untracked changes
- unclear ownership and governance drift

Lumos is automation-first: “point it at sources/systems, keep the catalog current.”

## Differentiators (Why This Is “One of a Kind”)
- Middleware, not another catalog UI.
- Catalog migrations become safer when history is portable (snapshots + diffs, roadmap).
- Storage-first coverage for raw files/lakes alongside traditional sources.
- CI-friendly primitives (diff + classification) to support change gates (roadmap).
- Cost/impact attribution becomes possible when lineage is first-class (roadmap).

## Current State (This Repo)
Today, Lumos is a Python framework with a modular architecture:
- `framework_cli.py` entrypoint → controllers → feature services → handlers → platform adapter.

It supports:
- ingestion: CSV, Avro, Parquet, S3, MongoDB, PostgreSQL
- enrichment: tags, documentation, properties
- lineage: dataset lineage and data job lineage
- ownership: users, groups, ownership assignment
- versioning: version scanning + bulk updates
- extraction: multiple extractors + Excel/CSV/Charts exports

## Release-Time Metadata Refresh (Key Use Case)
Problem: a release happens mid-week, but ingestion is scheduled weekly, so metadata is stale at release time.

Approach:
- store dataset schedule metadata (cron, timezone, and a configurable “availability lag” like +6h/+24h)
- at release time, compute the next relevant run window and schedule a follow-up refresh
- run ingestion/extraction using the correct partition timestamp so the catalog reflects the release quickly

## Business Value (How Lumos Saves Money)

Metadata is small; the waste is operational:
- time spent finding datasets, inferring schemas, and asking “who owns this?”
- duplicated datasets and redundant compute because discoverability is poor
- incidents caused by untracked changes and unclear downstream impact

**A practical, defensible way to estimate ROI** is to track before/after on:
- time-to-find and time-to-understand for datasets
- percentage of “unowned” datasets
- schema-related incidents and mean time to resolve
- duplicate datasets (or duplicate pipelines) discovered and retired

**Illustrative ROI example (replace with your numbers):**
- Team: 10 data engineers
- Time saved: 3 hours/week/engineer
- Loaded cost: $100/hour
- Annual savings from time alone: `10 * 3 * 52 * 100 = $156,000`

Add avoided incident cost and duplicate dataset savings to get a more complete ROI.

## Build vs Buy vs Lumos (Positioning)

- Build in-house: expensive and slow; typically becomes a long-running internal product with ongoing maintenance cost.
- Buy a commercial catalog: fast time-to-value but high cost and higher lock-in risk.
- Lumos: open source middleware layer that complements any catalog and prioritizes portability, automation, and CI enforcement.

## Target Architecture Direction (Planned)
Keep responsibilities clean and operationally cheap:
- Control plane (Spring Boot): scheduling, job registry, retries, auditing, and APIs.
- Data plane (Python): file/format-heavy ingestion + extraction (high-leverage Python ecosystem).
- Communication: Redis Streams job queue (lightweight, low ops).

All key architectural choices are tracked in `docs/DECISIONS.md`.

## Roadmap (Condensed)

- Portable snapshots + real diffs (breaking vs non-breaking vs informational).
- Release workflows (release-triggered refresh + backfills).
- CI-driven versioning and change gates.
- More platform adapters beyond DataHub.
- Optional AI layer: RAG over snapshots and agents for impact/ownership/quality insights (built on the snapshot/diff foundation).
