# Executive Summary

Lumos is the platform-agnostic metadata automation layer for modern data stacks. It sits between data-producing systems (storage, pipelines, jobs) and a catalog (DataHub today), keeping metadata accurate and current without relying on humans. It also makes metadata portable so organizations can change catalogs without losing history.

## The $50B Metadata Problem

Enterprise data management is broken:
- 73% of enterprise data goes unused due to discoverability issues
- $12.9M is the average annual cost of poor data quality per organization
- 30% of data engineer time is spent finding and understanding data
- 60% of data projects fail due to data management issues
- 40% of enterprise data is duplicated across systems

Root cause: metadata is fragmented, stale, and locked inside vendor silos.

## Why Current Solutions Fail

| Solution Type | Examples | The Gap |
|--------------|----------|---------|
| Service-specific catalogs | Snowflake Catalog, Databricks Unity | Only see one platform; no unified view |
| Paid catalog SaaS | Atlan, Alation, Collibra | Expensive, vendor lock-in, limited portability |
| OSS catalogs | DataHub, Amundsen, Atlas | Strong UI but platform-bound, no portable history |
| Manual docs | Confluence, wikis | Stale, inconsistent, and not enforced |

## The Lumos Approach

Lumos is the automation and portability plane for metadata:
- Automatic capture from sources (files, lakes, DBs, pipelines)
- Portable snapshots for migration safety and diffing
- CI/CD-native versioning and change enforcement
- Catalog-backed: the catalog remains the UI and registry

```
Sources (S3, DBs, Files, Pipelines)
          |
          v
      Lumos Core
  - Auto-detect, ingest, enrich
  - Snapshot + diff + version
  - Lineage + ownership + DQ
  - Cost and impact (planned)
          |
          v
Catalog (DataHub now, others later)
          |
          v
Consumers (CTO, DE, Governance, Security)
```

## Differentiators

- Middleware, not another catalog UI
- Portable snapshots as first-class artifacts
- Real diffs and CI gates for breaking changes
- Storage-first coverage for raw files
- Lineage-based cost attribution (planned)
- Platform-agnostic adapters (DataHub today)

## What Exists Today (v0.1.0)

- Multi-source ingestion: CSV, Avro, Parquet, MongoDB, S3, PostgreSQL
- Enrichment: tags, docs, properties
- Lineage: dataset and data job relationships
- Ownership: users, groups, assignments
- Versioning: cloud + schema versions
- Extraction: 11 extractors, 4 export formats
- CLI orchestration and SOLID architecture

## Gaps to Close the Vision

- Automatic discovery and change detection
- Portable snapshot export/import and real diffs
- Schema evolution tracking with breaking change alerts
- CI/CD gating for metadata changes
- Lineage-based cost attribution and FinOps reporting
- Additional platform adapters (Amundsen, Atlas, OpenMetadata, Snowflake)

## The Outcome

With Lumos, a CTO can ask: "What data do we have, who owns it, what changed, what breaks if we change X, and what does it cost?" and get a reliable answer. Catalog migrations become measured in days, not quarters, because metadata history is portable.

## AI-Ready Extension (Roadmap)

An optional roadmap layer adds:
- RAG over metadata snapshots and catalog views
- Agents for impact analysis, ownership gaps, and quality checks
- LLM gateway with routing, caching, and FinOps observability
