## Product Definition

### What Lumos is
Lumos is a platform-agnostic metadata automation layer (a "middle layer") that:
- discovers and ingests metadata from sources
- synchronizes metadata into a catalog platform (DataHub today)
- exports canonical "Lumos Snapshots" to preserve portability
- computes diffs and drives metadata versioning and CI gates

### What Lumos is not
- Not a replacement for DataHub/OpenMetadata UI in v0.x
- Not a full data artifact version-control system in the MVP
- Not a "humans must fill everything" metadata workflow

### Core operating model (catalog-backed)
- The catalog is the registry and UI.
- Lumos is the automation and enforcement plane.
- Lumos snapshots are the portability layer (migrate catalogs without losing history).

### Key personas
- Data platform / infrastructure: wants inventory, ownership, lineage, and automation
- Analytics / data engineering: wants safe schema evolution and impact visibility
- Governance/security: wants ownership, classification, DQ visibility, auditability
- CTO: wants cost/impact visibility and reduced vendor risk

### Core concepts (canonical entities)
Lumos standardizes these concepts across platforms:
- Dataset
- Schema (fields, types, nullability)
- LineageEdge (dataset-dataset, job-dataset)
- Ownership (user/group/type)
- Governance (tags, domains, glossary)
- Documentation (institutional memory)
- Operational metadata (size, rows, last modified, usage)
- DQAssertion (definition + status + lineage to dataset)
- Snapshot (portable representation at time T)
- Version (metadata-only MVP, derived from diff rules)

### The Lumos Snapshot (portability layer)
A Lumos Snapshot is a canonical JSON representation of a dataset's metadata at a point in time.

Properties:
- platform-agnostic format
- stable schema with versioning (snapshot schema version)
- suitable for:
  - diffing over time
  - exporting/importing during catalog migrations
  - building reports independent of the underlying catalog

### Metadata-only versioning (MVP definition)
For each dataset, version changes are driven by metadata diffs (schema/lineage/ownership/docs/governance), not data content.

Rules (illustrative):
- Breaking:
  - remove a field
  - change field type
  - change nullability from nullable to non-nullable
- Non-breaking:
  - add a nullable field
  - add documentation/tags
  - change ownership metadata
- Informational:
  - description edits

### "No developer dependency" principle
Lumos prioritizes automation:
- storage-first discovery + extraction
- snapshot-based diffing and completeness checks
- optional "request missing context" workflow (ping owners when required fields are missing)

### Bird's-eye view outputs (what a CTO sees)
- Inventory:
  - number of datasets by platform/domain/team
  - stale datasets (not modified / not queried)
  - unowned datasets
- Change and risk:
  - what changed this week (breaking vs non-breaking)
  - top risky datasets by downstream fanout
- Governance:
  - coverage of ownership, docs, tags
  - DQ coverage and freshness (when implemented)
- Cost and impact (later phase):
  - cost per dataset (storage + compute allocation)
  - "feature off savings" estimates using lineage grouping

### Non-goals for MVP
- full data artifact versioning/restore
- building a new catalog UI
- perfect cost attribution without cost inputs (requires billing signals or configured costs)

### AI-Ready Extension (Roadmap)
- RAG index over metadata snapshots and catalog views
- Agents for impact analysis, ownership gaps, and quality checks
- LLM gateway with cache, batching, routing, and FinOps/observability

