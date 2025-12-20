# Lumos Architecture & AI Roadmap

Note: timeline references below are illustrative and not commitments.

## Current Architecture (Platform-Agnostic Core)
- **MetadataPlatformInterface**: stable contract to any catalog backend (DataHub today; Atlas/Amundsen/OpenMetadata planned).
- **Ingestion Layer**: handlers for CSV/Avro/Parquet/S3/Mongo/Postgres; auto-schema extraction and MCE/MCP emission.
- **Lineage Layer**: dataset and column-level lineage via platform interface.
- **Versioning Layer**: dataset and schema version management; CI-friendly (e.g., Jenkins).
- **Extraction & Analytics**: 11 extractors, multi-format exports (JSON/CSV/Excel/visualizations).
- **Snapshots**: vendor-neutral persisted snapshots to enable migration and lock-in avoidance.
- **Governance & Quality**: ownership, tags, documentation, properties, profiling, DQ integrations (extensible).

### High-Level Flow
```mermaid
flowchart TD
    src[Sources: files/dbs/pipelines] --> ingest[Ingestion Handlers]
    ingest --> mce[MCE/MCP]
    mce --> platform[Platform Interface]
    platform --> datahub[(DataHub)]
    platform --> atlas[(Atlas)]
    platform --> amundsen[(Amundsen)]
    platform --> openmeta[(OpenMetadata)]
    platform --> snapshot[Snapshots (vendor-neutral)]
    ingest --> lineage[Lineage/Versioning]
    lineage --> platform
    extract[Extraction/Analytics] --> platform
```

## Target Extensions
- **More Platform Adapters**: Atlas, Amundsen, OpenMetadata, custom in-house catalogs.
- **Streaming Ingestion (optional)**: CDC/event-driven metadata capture; Flink/Spark/Kafka connectors.
- **Cost & Impact Attribution**: connect lineage + usage + cost to estimate per-dataset/feature spend and savings on feature toggles.
- **Quality Signals**: first-class quality aspects surfaced in catalog and exports.

## AI Layer (Roadmap)
1) **RAG over Metadata**
   - Index schemas, lineage, ownership, docs, quality, cost hints.
   - Hybrid retrieval (BM25 + vector) per tenant/domain.
   - Answer "what feeds X," "who owns Y," "what breaks if we change Z," "where is PII," etc.
2) **Agents for Data Operations**
   - LangGraph agents with tools:
     - `get_lineage`, `get_schema_diff`, `get_owners`, `get_quality`, `estimate_cost_impact`.
   - Use cases: impact analysis before a PR, ownership/coverage checks, auto-doc refresh.
3) **LLM Gateway**
   - Prefix/KV-cache-aware routing, dynamic batching (vLLM/TensorRT-LLM), model routing (SLM vs LLM), per-tenant policies.
   - FinOps and observability: tokens, cost, latency (TTFT hot/cold), cache hit rate.
4) **Evals & SLOs**
   - RAG evals (faithfulness, context recall) on a metadata QA goldset.
   - Latency SLOs per route; fail CI on regressions.

## Deployment Notes
- **OSS-first**: Python core; adapters extend via interface; deployable on any infra.
- **Vendor Neutrality**: snapshots keep metadata portable; switch backends without rework.
- **Observability**: Prometheus + OTel traces for gateway/agents; dashboards for cache/cost/latency.
- **Multi-Tenant Readiness**: namespaces/filters for tenants/domains; per-tenant model/policy configs in gateway.

## Phased Roadmap (Example)
- **Phase 1 (Now)**: Solidify platform adapters; strengthen snapshots; cost/impact lineage surfacing; concise CTO-facing README.
- **Phase 2 (Near)**: RAG over metadata + eval harness; initial LangGraph agent for impact/ownership; observability for AI calls.
- **Phase 3 (Next)**: LLM gateway with KV cache + batching + model routing; FinOps dashboards; multi-tenant controls.
- **Phase 4 (Stretch)**: Streaming ingestion connectors; optional fine-tuning (QLoRA) for domain-specialized metadata Q&A. 


---

## Delivery Plan (Optional Extension)

# Lumos Delivery Plan (Platform-Agnostic + AI Roadmap)

## Objectives
- Deliver a platform-agnostic metadata backbone with automatic ingestion, lineage, versioning, snapshots, and governance/quality visibility.
- Keep metadata portable (no lock-in) and CI-friendly.
- Add AI capabilities: RAG over metadata, agents for impact/ownership/quality, LLM gateway with observability and FinOps.

## Phases & Milestones

### Phase 1 - Platform Core Hardening (2-3 weeks)
- Finalize platform interface and add at least one more adapter spec (skeletons for Atlas/Amundsen/OpenMetadata).
- Snapshot format hardened for migration; CLI commands to export/import snapshots.
- Strengthen lineage + versioning paths; Jenkins-friendly tasks; sample configs.
- Outcomes: reproducible demos; updated CTO-facing README.
- SLOs: ingestion p95 < 300 ms for small configs; snapshot round-trip verified.

### Phase 2 - RAG over Metadata (2-3 weeks)
- Build metadata indexer (schemas, lineage, ownership, docs, quality) -> hybrid retrieval.
- Expose `/query` API (question -> answer + citations + URNs).
- Goldset + RAG evals (faithfulness, context recall) in CI.
- SLOs: p95 <= 2.0 s at 1-2 QPS (hosted/base model); AnswerFaithfulness >= 0.7, ContextRecall >= 0.8 on goldset.

### Phase 3 - Agents & Observability (2-3 weeks)
- LangGraph agent with tools: `get_lineage`, `get_schema_diff`, `get_owners`, `get_quality`, `estimate_cost_impact`.
- Tracing + metrics: tool durations, retrieval latency, model latency; Prometheus/Grafana dashboards.
- SLOs: p95 end-to-end <= 2.5 s at 2-3 QPS; traces show tool sequencing; ownership/impact checks correct on seeded cases.

### Phase 4 - LLM Gateway & FinOps (3-4 weeks)
- Gateway with prefix/KV-cache-aware routing; dynamic batching (vLLM/TensorRT-LLM); model routing (SLM vs LLM).
- FinOps metrics: tokens, cost, latency, cache hit rate; dashboards for hot vs cold TTFT.
- SLOs: >=3-5x throughput vs naive; cache hit >=70-80% on seeded workloads; p95 <= 700 ms at 4 QPS with cache/batching.

### Phase 5 - Stretch (Optional)
- Streaming ingestion (CDC/event-driven) via Flink/Spark/Kafka connectors.
- Optional fine-tuning (QLoRA) for domain-specific metadata Q&A.

## Workstreams
- **Adapters & Core**: interfaces, additional platform implementations, snapshot tooling.
- **Metadata Index & RAG**: chunking, hybrid retrieval, eval harness.
- **Agents**: tool catalog + LangGraph flows for impact/ownership/quality.
- **Serving**: LLM gateway, KV cache, routing, batching, FinOps metrics.
- **Observability**: OTel traces, Prometheus, Grafana dashboards.
- **Deployment**: Helm/compose examples; Jenkins-friendly tasks; multi-tenant configs.

## Risks & Mitigations
- **Vendor API drift**: keep adapters thin; contract tests per platform.
- **Metadata freshness**: optional streaming/CDC; scheduled backfills; alerts on stale snapshots.
- **Cost/latency regressions**: CI evals + perf benches; cache/batching defaults.
- **Adoption friction**: strong docs, examples, snapshots for migration, CLI-first UX.

## Acceptance (per phase)
- Tagged releases per phase; one-command demo (compose/helm).
- CI: unit + integration + RAG evals + perf smoke (where feasible).
- Dashboards: latency, cache hit, cost; traces showing agent tool calls.

## Immediate Next Steps
- Lock README/brief/roadmap messaging.
- Add adapter skeletons and snapshot export/import command.
- Stand up minimal metadata RAG indexer + goldset for evals. 
