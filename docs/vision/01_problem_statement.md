# The 8 Unsolved Problems in Enterprise Data Management

> Every data-driven organization faces these challenges. Current tools address symptoms, not root causes. Lumos was built to solve them fundamentally.

---

## Problem 1: Raw Storage Data is Invisible

### The Scenario

Your organization stores 80% of its data in cloud object storage-S3, GCS, Azure Data Lake. These are Avro files from Kafka, Parquet files from Spark jobs, CSV exports from various systems.

**Ask your team:**
- How many datasets exist in your S3 buckets?
- What's the schema of `s3://data-lake/raw/events/2024/01/`?
- Who created the `customer_attributes.parquet` file?

**Most organizations cannot answer these questions.**

### Why Current Tools Fail

| Tool | Limitation |
|------|------------|
| DataHub | Requires manual registration or complex ingestion recipes |
| Snowflake Catalog | Only sees Snowflake tables, not S3 files |
| Atlan | Manual crawling, expensive compute costs |
| AWS Glue Catalog | AWS-only, limited metadata |

### The Real Cost

- **Dark data**: Unknown datasets consuming storage costs
- **Duplication**: Teams recreate datasets that already exist
- **Compliance risk**: PII in files you don't know about
- **Wasted effort**: Data engineers spend hours finding data

### How Lumos Solves It

```python
# Point Lumos at any storage location
lumos ingest --source s3://data-lake/raw/ --recursive

# Lumos automatically:
# 1. Scans all files
# 2. Detects file formats (Avro, Parquet, CSV, JSON)
# 3. Extracts schemas
# 4. Registers in your catalog
# 5. Tracks as versioned datasets
```

**Result:** Every file in your data lake is automatically cataloged with full schema information.

---

## Problem 2: Schema Changes Are Untracked

### The Scenario

It's Monday morning. The ML pipeline failed overnight. After 4 hours of investigation, you discover:

1. A data engineer added a column to `user_events` on Friday
2. This changed the Avro schema
3. 15 downstream Spark jobs expected the old schema
4. Weekend batch jobs failed silently
5. Monday dashboards show wrong numbers

**No one knew the schema changed. No one was notified. No one approved it.**

### Why Current Tools Fail

| Tool | Limitation |
|------|------------|
| DataHub | Stores current schema, no change detection |
| Git | Only tracks code, not data schemas |
| dbt | Only tracks dbt models, not raw data |
| Great Expectations | Validates data, not schema changes |

### The Real Cost

- **4.2 hours average** to trace schema-related failures
- **3-5 downstream teams** impacted per schema change
- **Lost trust** in data pipelines
- **Reactive firefighting** instead of proactive management

### How Lumos Solves It

```yaml
# Lumos Schema Tracking
dataset: user_events
schema_version: 2.3.0
changes:
  - version: 2.3.0
    timestamp: 2024-01-15T10:30:00Z
    author: john.doe
    type: COLUMN_ADDED
    details:
      column: loyalty_tier
      type: STRING
    breaking: false
    downstream_impact:
      - ml_feature_pipeline (COMPATIBLE)
      - analytics_dashboard (COMPATIBLE)
      - reporting_etl (NEEDS_UPDATE)
    
# Automatic alerts sent to:
# - Dataset owner
# - All downstream consumers
# - CI/CD pipeline (optional gate)
```

**Result:** Every schema change is tracked, versioned, and communicated before it causes failures.

---

## Problem 3: Developers Never Document (And Never Will)

### The Scenario

Senior data engineer leaves the company. They built 200+ datasets over 3 years.

**What you have:**
- Table names like `cust_attr_v2_final_new`
- No descriptions
- No business context
- No column definitions
- Scattered Slack messages and forgotten Confluence pages

**What you need:**
- What business problem does this solve?
- What does each column mean?
- Who should I contact about this data?
- Is this data still being used?

### Why Current Tools Fail

| Tool | Limitation |
|------|------------|
| DataHub | Provides UI for documentation-but someone must write it |
| Confluence | Becomes outdated within weeks |
| Code comments | Disconnected from catalog |
| README files | Never updated after initial creation |

### The Root Cause

**Expecting busy developers to maintain documentation is fantasy.**

- Documentation is not their job
- No time allocated for it
- No enforcement mechanism
- No connection to their workflow

### How Lumos Solves It

```yaml
# Lumos Documentation Strategy

# 1. Auto-generate from schema
auto_documentation:
  enabled: true
  sources:
    - column_names -> business terms (via glossary mapping)
    - file_path -> data domain (via path patterns)
    - schema_evolution -> change history (automatic)
    - query_patterns -> usage context (via logs)

# 2. CI/CD enforcement
ci_integration:
  require_description: true
  require_owner: true
  require_business_context: true
  block_merge_if_missing: true

# 3. Stakeholder nudges
notifications:
  on_missing_documentation:
    - owner: "Your dataset {name} needs a description"
    - frequency: weekly
    - escalate_after: 30_days
```

**Result:** Documentation is captured automatically or enforced through CI/CD-never dependent on developer goodwill.

---

## Problem 4: Data Cost Attribution is Impossible

### The Scenario

CFO asks: "We spend $2M/month on data infrastructure. What are we getting for it?"

**You cannot answer:**
- How much does `customer_360` cost to produce?
- Which datasets are most expensive?
- If we deprecate `legacy_reports`, how much do we save?
- What's the ROI of the ML feature pipeline?

### Why Current Tools Fail

| Tool | Limitation |
|------|------------|
| Cloud billing | Shows compute costs, not per-dataset costs |
| DataHub | No cost integration |
| dbt Cloud | dbt jobs only, not full pipeline |
| Atlan | Basic cost info, not lineage-based attribution |

### The Calculation Challenge

Costs flow through lineage:

```
raw_events (S3 storage: $500/month)
    v
cleaned_events (Spark job: $200/run x 30 runs = $6,000/month)
    v
feature_store (Databricks: $3,000/month)
    v
ml_predictions (Model serving: $2,000/month)
```

**Total cost of `ml_predictions`: $11,500/month**

Without lineage-based attribution, you only see individual component costs.

### How Lumos Solves It

```yaml
# Lumos Cost Attribution

dataset: ml_predictions
cost_breakdown:
  direct:
    storage: $150/month
    compute: $2,000/month
  upstream_attributed:
    feature_store: $1,500/month (50% allocation)
    cleaned_events: $3,000/month (50% allocation)
    raw_events: $250/month (50% allocation)
  total: $6,900/month

cost_drivers:
  - cleaned_events Spark job (43% of total)
  - Model serving (29% of total)
  
optimization_opportunities:
  - "cleaned_events runs 30x/month but only 5 runs have consumers"
  - "Potential savings: $5,000/month by reducing to 5 runs"
```

**Result:** Every dataset has a true cost, enabling data product ROI decisions.

---

## Problem 5: No Single Pane of Glass

### The Scenario

Your organization has:
- **Finance team**: Snowflake (1,000 tables)
- **ML team**: Databricks + S3 (500 datasets)
- **Analytics**: BigQuery (300 tables)
- **Streaming**: Kafka (50 topics)
- **Legacy**: PostgreSQL (200 tables)

**The problem:**
- Finance can't see ML team's feature definitions
- ML team recreates data that Finance already has
- Analytics builds dashboards on inconsistent sources
- No one knows what "customer" means across systems

### Why Current Tools Fail

| Tool | Limitation |
|------|------------|
| Snowflake Catalog | Snowflake only |
| Databricks Unity | Databricks only |
| DataHub | Connects to all, but separate ingestion recipes |
| Atlan | Expensive to connect everything |

### The Real Cost

- **40% data duplication** across teams
- **Inconsistent metrics**: "Revenue" means different things
- **Shadow IT**: Teams build their own solutions
- **Governance gaps**: PII scattered across platforms

### How Lumos Solves It

```yaml
# Lumos Unified Configuration

sources:
  - type: snowflake
    connection: ${SNOWFLAKE_CONN}
    databases: [FINANCE_DB, ANALYTICS_DB]
    
  - type: databricks
    connection: ${DATABRICKS_CONN}
    catalogs: [ml_features, raw_data]
    
  - type: bigquery
    project: analytics-prod
    datasets: [reporting, dashboards]
    
  - type: s3
    bucket: data-lake-prod
    prefixes: [/raw/, /processed/, /ml/]
    
  - type: kafka
    bootstrap_servers: ${KAFKA_SERVERS}
    topics: [events.*, metrics.*]

# Single command to catalog everything
lumos sync --all

# Result: One catalog with 2,050 datasets across all platforms
```

**Result:** Every team sees every dataset, regardless of where it lives.

---

## Problem 6: Service-Specific Catalogs Create Silos

### The Scenario

Each platform vendor offers a catalog:
- Snowflake -> Snowflake Catalog
- Databricks -> Unity Catalog
- Google -> Data Catalog
- AWS -> Glue Catalog

**The trap:**
- Each catalog only sees its own platform
- No cross-platform lineage
- No unified search
- Vendor lock-in multiplied

### Why This Happens

Vendors want you locked in. Their catalog is a feature, not a solution.

**Their incentive:** Keep you on their platform
**Your need:** See data across all platforms

### How Lumos Solves It

```
+-------------------------------------------------------------+
|                    LUMOS UNIFIED VIEW                        |
|                                                              |
|  +---------+  +---------+  +---------+  +---------+        |
|  |Snowflake|  |Databricks|  | BigQuery|  |   S3   |        |
|  | Catalog |  |  Unity  |  | Catalog |  |  Files |        |
|  +----+----+  +----+----+  +----+----+  +----+----+        |
|       |            |            |            |              |
|       +------------+------------+------------+              |
|                         |                                    |
|                         v                                    |
|              +---------------------+                        |
|              |   Lumos Metadata    |                        |
|              |   (Portable, Open)  |                        |
|              +----------+----------+                        |
|                         |                                    |
|                         v                                    |
|              +---------------------+                        |
|              |  Your Chosen Catalog |                        |
|              |  (DataHub/Amundsen/  |                        |
|              |   Atlas/Custom)      |                        |
|              +---------------------+                        |
+-------------------------------------------------------------+
```

**Result:** Unified metadata regardless of platform-specific catalogs.

---

## Problem 7: Vendor Lock-In is Severe

### The Scenario

You've used DataHub for 2 years. You have:
- 5,000 datasets cataloged
- 10,000 lineage relationships
- 500 ownership assignments
- 2,000 descriptions and tags

**DataHub announces pricing changes or you want to switch to OpenMetadata.**

**The problem:** Your metadata is trapped.
- No standard export format
- No migration path
- Years of work locked in

### Why Current Tools Fail

Every catalog uses proprietary formats:
- DataHub: Avro-based MCE/MCP
- Amundsen: Neo4j graph
- Atlas: Custom REST API
- Atlan: Proprietary cloud

**There is no "CSV export of my metadata."**

### How Lumos Solves It

```yaml
# Lumos Portable Metadata Format (PMF)

# Export your entire catalog
lumos export --format pmf --output metadata_backup.json

# The file is human-readable and portable
{
  "lumos_version": "1.0.0",
  "export_timestamp": "2024-01-15T10:30:00Z",
  "datasets": [
    {
      "urn": "lumos://snowflake/db/schema/table",
      "name": "customer_events",
      "schema": {
        "fields": [...]
      },
      "lineage": {
        "upstream": [...],
        "downstream": [...]
      },
      "ownership": [...],
      "tags": [...],
      "description": "...",
      "cost": {...},
      "quality": {...}
    }
  ]
}

# Import to any catalog
lumos import --source metadata_backup.json --target openmetadata
lumos import --source metadata_backup.json --target amundsen
```

**Result:** Your metadata is always portable. Switch catalogs anytime.

---

## Problem 8: Data Quality is a Black Box

### The Scenario

Executive opens dashboard. Numbers look wrong.

**Questions with no answers:**
- Is the source data fresh?
- Are there quality checks on this data?
- When did quality last pass?
- What's the data completeness?
- Can I trust this number?

### Why Current Tools Fail

| Tool | Limitation |
|------|------------|
| Great Expectations | Quality tool, no catalog integration |
| dbt tests | dbt only, results not in catalog |
| DataHub | Quality tab exists, but disconnected from DQ tools |
| Monte Carlo | Expensive, separate product |

**The gap:** Quality information exists, but not in the catalog where decisions are made.

### How Lumos Solves It

```yaml
# Lumos Data Quality Integration

dataset: customer_360
quality:
  last_check: 2024-01-15T08:00:00Z
  status: PASSED
  
  checks:
    - name: completeness
      threshold: 99%
      actual: 99.7%
      status: PASSED
      
    - name: freshness
      threshold: 1_hour
      actual: 45_minutes
      status: PASSED
      
    - name: uniqueness(customer_id)
      threshold: 100%
      actual: 100%
      status: PASSED
      
    - name: referential_integrity
      threshold: 100%
      actual: 98.5%
      status: WARNING
      details: "1.5% of orders reference non-existent customers"

  trend:
    - date: 2024-01-14
      score: 98.2%
    - date: 2024-01-15
      score: 99.1%

# Visible directly in catalog search results
# Consumers know before they use the data
```

**Result:** Quality information is embedded in the catalog, visible at decision time.

---

## Summary: The Root Cause

All eight problems share a root cause:

**Metadata is treated as an afterthought, not infrastructure.**

Organizations invest millions in compute, storage, and orchestration. They treat metadata as optional documentation.

**Lumos treats metadata as critical infrastructure:**
- Captured automatically
- Versioned like code
- Portable across platforms
- Integrated with quality and cost

---

## Next: How Lumos Solves Each Problem

See [03_solution_overview.md](03_solution_overview.md) for detailed solutions.

## Why Now

- Data estates grow faster than human-maintained governance.
- Storage-first and polyglot stacks (warehouse + lake + streaming) are the norm.
- Metadata must behave like code: versioned, diffed, and enforced in CI.
