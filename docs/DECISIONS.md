# Decision Log

Keep entries concise and high-signal.

## Accepted

### Architecture boundaries
- Control plane vs data plane: keep orchestration in a control plane and keep file/format-heavy work in a data plane.
- Catalog-backed model: the catalog (DataHub today) remains the UI/registry; Lumos automates and enforces metadata hygiene.
- Platform abstraction: feature services depend on `core/platform/interface.py` and use `core/platform/factory.py` to select the implementation.

### Configuration + UX
- Config-driven operations: job inputs are configuration files (JSON/YAML) rather than hard-coded pipelines.
- CLI-first (today): a single entrypoint (`framework_cli.py`) routes operations to controllers/services.

### DataHub integration (today)
- Use the DataHub Python SDK emitter for emission; the platform adapter owns DataHub communication.
- Prefer MCP emission per-aspect (more granular) while keeping MCE creation in ingestion handlers.
- Test mode: allow validating the shape of emissions without sending to DataHub.

### Partitioned ingestion semantics (today)
- Partition selection uses an explicit timestamp (CLI arg) + `partitioning_format`.
- `partition_cron` is stored as metadata but is informational (does not drive selection/alignment).

### Runtime orchestration (planned direction)
- Control plane: add a Spring Boot service for scheduling, job registry, retries, auditability, and APIs.
- Data plane: keep Python workers for ingestion/extraction because Python has high-leverage ecosystems for files and formats.
 - Spring Boot motivation: control-plane concerns (APIs, auth/RBAC, audits, retries, operational maturity) are the main showcase and are cleaner to implement as a long-running service.

### Queue choice
- Queue: use Redis Streams (lightweight, low ops) instead of Kafka; replay is not a requirement for the baseline.
- Delivery semantics: prefer at-least-once with idempotent job execution and explicit job state (RUNNING/SUCCEEDED/FAILED).
- Execution mode: keep Python workers always-on for now; skip on-demand execution initially for simplicity.
 - Cost bias: choose components that run cheaply on small footprints (single-host EC2 is a supported baseline).

### Documentation policy
- Docs: keep documentation minimal, crisp, and non-redundant; optimize for “README-first” consumption.
- Doc set: keep essential product narrative in `README.md` and `docs/VISION.md`, and keep technical grounding in `docs/ARCHITECTURE.md`.
