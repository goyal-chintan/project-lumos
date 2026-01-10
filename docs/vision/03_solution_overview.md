# How Lumos Solves Each Problem

> For every industry problem, Lumos provides a concrete, implementable solution.

---

## The Core Philosophy

Lumos is built on three principles:

1. **Automatic First**: Minimize human effort; capture metadata from sources directly
2. **Portable Always**: Never lock data into proprietary formats
3. **CI/CD Native**: Treat metadata like code-versioned, reviewed, deployed

---

## The Middle Layer Architecture

Lumos sits between your data sources and your chosen catalog:

```
+---------------------------------------------------------------------+
|                         DATA SOURCES                                 |
|  +--------+ +--------+ +--------+ +--------+ +--------+ +--------+ |
|  |   S3   | |  GCS   | |Snowflake| |  Kafka | |Postgres| |  HDFS  | |
|  +--------+ +--------+ +--------+ +--------+ +--------+ +--------+ |
+-------------------------------+-------------------------------------+
                                |
                                v
+---------------------------------------------------------------------+
|                                                                      |
|                      LUMOS (THE MIDDLE LAYER)                        |
|                                                                      |
|  +-------------+  +-------------+  +-------------+  +-------------+ |
|  | Auto-Detect |  |   Schema    |  |   Lineage   |  |   Version   | |
|  |   Engine    |  |   Tracker   |  |   Engine    |  |   Manager   | |
|  +-------------+  +-------------+  +-------------+  +-------------+ |
|                                                                      |
|  +-------------+  +-------------+  +-------------+  +-------------+ |
|  |    Cost     |  |     DQ      |  |  Ownership  |  |  Snapshot   | |
|  | Calculator  |  | Integration |  |   Manager   |  |   Manager   | |
|  +-------------+  +-------------+  +-------------+  +-------------+ |
|                                                                      |
+-------------------------------+-------------------------------------+
                                |
                                v
+---------------------------------------------------------------------+
|                   PLATFORM ABSTRACTION LAYER                         |
|  +-------------+  +-------------+  +-------------+  +-------------+ |
|  |   DataHub   |  |  Amundsen   |  |    Atlas    |  |OpenMetadata | |
|  |   Handler   |  |   Handler   |  |   Handler   |  |   Handler   | |
|  |     [ok]      |  |   Planned   |  |   Planned   |  |   Planned   | |
|  +-------------+  +-------------+  +-------------+  +-------------+ |
+-------------------------------+-------------------------------------+
                                |
                                v
+---------------------------------------------------------------------+
|                      YOUR CHOSEN CATALOG                             |
|                                                                      |
|          DataHub  |  Amundsen  |  Atlas  |  OpenMetadata             |
|                                                                      |
+---------------------------------------------------------------------+
```

---

## Solution 1: Auto-Detection Engine

**Problem Solved:** Raw storage data is invisible

### How It Works

```python
# Lumos scans any storage location
lumos ingest --config sources.yaml

# sources.yaml
sources:
  - type: s3
    bucket: data-lake-prod
    prefixes:
      - /raw/events/
      - /processed/features/
    file_patterns:
      - "*.avro"
      - "*.parquet"
      - "*.csv"
```

### The Detection Process

1. **Scan**: Recursively discover files in storage
2. **Identify**: Detect file format from extension and magic bytes
3. **Parse**: Extract schema from file headers/samples
4. **Map**: Convert to Lumos portable schema format
5. **Register**: Push to your chosen catalog

### Supported Formats

| Format | Schema Source | Nested Types | Partitions |
|--------|--------------|--------------|------------|
| Avro | File header | [ok] Full support | [ok] Detected |
| Parquet | File footer | [ok] Full support | [ok] Detected |
| CSV | Header row + inference | [no] Flat only | [ok] Detected |
| JSON | Sample-based inference | [ok] Full support | [ok] Detected |
| ORC | File footer | [ok] Full support | [ok] Detected |

### Example Output

```yaml
# Discovered dataset
dataset:
  urn: lumos://s3/data-lake-prod/raw/events/user_clicks
  name: user_clicks
  platform: s3
  format: parquet
  
  location:
    bucket: data-lake-prod
    prefix: /raw/events/user_clicks/
    partitioning: year={yyyy}/month={mm}/day={dd}
    
  schema:
    fields:
      - name: event_id
        type: STRING
        nullable: false
        description: "Unique event identifier"
        
      - name: user_id
        type: STRING
        nullable: false
        
      - name: timestamp
        type: TIMESTAMP
        nullable: false
        
      - name: properties
        type: STRUCT
        fields:
          - name: page_url
            type: STRING
          - name: referrer
            type: STRING
            
  statistics:
    file_count: 1,247
    total_size_bytes: 45_000_000_000
    row_count_estimate: 500_000_000
    last_modified: 2024-01-15T08:00:00Z
```

---

## Solution 2: Schema Change Tracker

**Problem Solved:** Schema changes are untracked

### How It Works

Lumos maintains a versioned history of every schema:

```yaml
# Schema version history
dataset: user_events
schema_history:
  - version: 1.0.0
    timestamp: 2023-06-01T10:00:00Z
    fields: [event_id, user_id, timestamp, event_type]
    
  - version: 1.1.0
    timestamp: 2023-09-15T14:30:00Z
    change_type: COLUMN_ADDED
    change_details:
      column: session_id
      type: STRING
    backward_compatible: true
    
  - version: 2.0.0
    timestamp: 2024-01-10T09:00:00Z
    change_type: COLUMN_TYPE_CHANGED
    change_details:
      column: user_id
      old_type: STRING
      new_type: INT64
    backward_compatible: false
    breaking_change_alert: SENT
```

### Change Detection Modes

1. **Scheduled Scans**: Re-scan sources on schedule, detect differences
2. **Event-Driven**: Listen to storage events (S3 notifications, etc.)
3. **CI/CD Integration**: Compare schemas in pull requests

### Breaking Change Analysis

```yaml
# Automatic impact analysis
schema_change:
  dataset: user_events
  change: user_id type STRING -> INT64
  
impact_analysis:
  downstream_datasets:
    - name: user_sessions
      compatibility: BREAKING
      reason: "Expects STRING user_id in JOIN"
      owner: analytics-team
      
    - name: feature_store.user_features
      compatibility: BREAKING
      reason: "Type mismatch in pipeline"
      owner: ml-platform
      
    - name: reporting.daily_users
      compatibility: COMPATIBLE
      reason: "Casts user_id to STRING anyway"
      owner: bi-team
      
  recommendations:
    - "Notify analytics-team and ml-platform before deploying"
    - "Consider backward-compatible approach: add new column first"
    - "Update 2 downstream pipelines before schema change"
```

### CI/CD Integration

```yaml
# .github/workflows/schema-check.yml
name: Schema Change Review

on:
  pull_request:
    paths:
      - 'schemas/**'
      - 'dbt/**'

jobs:
  schema-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Detect Schema Changes
        run: lumos schema diff --base main --head ${{ github.sha }}
        
      - name: Analyze Impact
        run: lumos schema impact --changes schema_diff.json
        
      - name: Comment on PR
        run: lumos schema comment --pr ${{ github.event.number }}
        # Posts: "This PR changes 3 schemas, affecting 12 downstream datasets"
```

---

## Solution 3: Automatic Documentation

**Problem Solved:** Developers never document

### Strategy 1: Infer from Schema

```yaml
# Column name -> Business term mapping
glossary_mappings:
  cust_id: Customer Identifier
  txn_amt: Transaction Amount
  ts: Timestamp
  dt: Date
  flg: Flag (Boolean)
  
# Pattern-based inference
path_patterns:
  /raw/events/*: "Raw event data from streaming pipeline"
  /processed/features/*: "Feature store datasets for ML"
  /reporting/*: "Business reporting tables"
```

### Strategy 2: Extract from Code

```python
# Lumos parses SQL and extracts documentation
# From: dbt model
# {{ config(materialized='table') }}
# -- Customer lifetime value calculation
# -- Owner: analytics-team
# -- SLA: Updated daily by 6am UTC

SELECT
  customer_id,
  SUM(order_total) as lifetime_value
FROM orders
GROUP BY customer_id

# Lumos extracts:
# - description: "Customer lifetime value calculation"
# - owner: analytics-team
# - sla: "Updated daily by 6am UTC"
```

### Strategy 3: Enforce via CI/CD

```yaml
# lumos.yaml - Documentation requirements
documentation:
  required_fields:
    - description
    - owner
    - business_domain
    
  enforcement:
    mode: block_on_missing  # or warn_only
    
  exceptions:
    - pattern: /tmp/*
      reason: "Temporary datasets don't need docs"
```

### Strategy 4: Nudge Stakeholders

```yaml
# Weekly nudge for missing documentation
notifications:
  missing_docs_reminder:
    schedule: "0 9 * * 1"  # Monday 9am
    recipients:
      - dataset_owner
    template: |
      Hi {owner_name},
      
      Your dataset "{dataset_name}" is missing documentation:
      - [ ] Description
      - [ ] Business context
      
      Add documentation: {catalog_url}
      
      Datasets without documentation will be flagged for deprecation
      after 30 days.
```

---

## Solution 4: Cost Attribution Engine

**Problem Solved:** Data costs are unknown

### How It Works

Lumos traces lineage and attributes costs:

```yaml
# Cost model configuration
cost_model:
  storage:
    s3:
      cost_per_gb_month: 0.023
    snowflake:
      cost_per_tb_month: 40.00
      
  compute:
    spark:
      cost_per_dbu: 0.15
    snowflake:
      cost_per_credit: 2.00
      
  attribution:
    method: lineage_weighted  # or equal_split, last_touch
```

### Cost Calculation

```
                        +-----------------+
                        |   raw_events    |
                        | Storage: $500/mo|
                        +--------+--------+
                                 |
                    +------------+------------+
                    |                         |
           +--------v--------+      +--------v--------+
           | cleaned_events  |      |  event_metrics  |
           |Compute: $3000/mo|      |Compute: $1000/mo|
           | (60% allocation)|      | (40% allocation)|
           +--------+--------+      +-----------------+
                    |
           +--------v--------+
           |  feature_store  |
           |Compute: $2000/mo|
           |Storage: $200/mo |
           +--------+--------+
                    |
           +--------v--------+
           |  ml_predictions |
           |Compute: $1500/mo|
           +-----------------+

# Total cost of ml_predictions:
# Direct: $1,500 (compute)
# Attributed from feature_store: $2,200 x 100% = $2,200
# Attributed from cleaned_events: $3,000 x 60% x 100% = $1,800
# Attributed from raw_events: $500 x 60% = $300
# ---------------------------------------------
# Total: $5,800/month
```

### Cost Dashboard

```yaml
# Cost report output
cost_report:
  dataset: ml_predictions
  period: 2024-01
  
  summary:
    total_cost: $5,800
    direct_cost: $1,500
    attributed_cost: $4,300
    
  breakdown:
    by_type:
      compute: $4,700 (81%)
      storage: $1,100 (19%)
      
    by_team:
      ml-platform: $3,500
      data-engineering: $1,800
      analytics: $500
      
  trends:
    vs_last_month: +12%
    vs_last_quarter: -5%
    
  optimization:
    - "cleaned_events job runs 30x/month but 20 runs have no consumers"
    - "Potential savings: $2,000/month"
```

---

## Solution 5: Unified Catalog View

**Problem Solved:** No single pane of glass

### Configuration

```yaml
# Connect all sources in one config
lumos_config:
  sources:
    # Cloud storage
    - type: s3
      name: aws-data-lake
      buckets: [raw-data, processed-data, ml-features]
      
    - type: gcs
      name: gcp-analytics
      buckets: [analytics-prod, reporting]
      
    # Databases
    - type: snowflake
      name: snowflake-prod
      account: ${SNOWFLAKE_ACCOUNT}
      databases: [FINANCE, MARKETING, PRODUCT]
      
    - type: bigquery
      name: bigquery-analytics
      project: analytics-prod
      
    - type: databricks
      name: databricks-ml
      workspace: ${DATABRICKS_WORKSPACE}
      
    # Streaming
    - type: kafka
      name: kafka-prod
      bootstrap_servers: ${KAFKA_SERVERS}
      topic_patterns: [events.*, metrics.*]

# Sync all sources
lumos sync --all --output datahub
```

### Unified Search

```bash
# Search across all platforms
$ lumos search "customer"

Results (47 datasets):

1. snowflake.FINANCE.customers
   Platform: Snowflake | Owner: finance-team | Quality: 99.2%
   
2. s3://data-lake/processed/customer_360/
   Platform: S3 (Parquet) | Owner: data-platform | Quality: 98.5%
   
3. databricks.ml_features.customer_features
   Platform: Databricks | Owner: ml-platform | Quality: 97.8%
   
4. kafka.events.customer_events
   Platform: Kafka | Owner: streaming-team | Schema: Avro v2.1
   
5. bigquery.analytics.customer_segments
   Platform: BigQuery | Owner: analytics | Last updated: 2h ago
```

---

## Solution 6: Platform Abstraction

**Problem Solved:** Vendor lock-in

### The Interface

```python
# core/platform/interface.py

class MetadataPlatformInterface(ABC):
    """
    Abstract interface for any metadata catalog.
    Implement this to support a new platform.
    """
    
    @abstractmethod
    def emit_dataset(self, dataset: LumosDataset) -> bool:
        """Register or update a dataset"""
        pass
    
    @abstractmethod
    def emit_lineage(self, upstream: str, downstream: str) -> bool:
        """Add lineage relationship"""
        pass
    
    @abstractmethod
    def get_dataset(self, urn: str) -> Optional[LumosDataset]:
        """Retrieve dataset metadata"""
        pass
    
    @abstractmethod
    def search(self, query: str) -> List[LumosDataset]:
        """Search datasets"""
        pass
```

### Implementations

```python
# Current: DataHub
class DataHubHandler(MetadataPlatformInterface):
    def emit_dataset(self, dataset):
        mce = self._convert_to_mce(dataset)
        self.emitter.emit(mce)

# Planned: Amundsen
class AmundsenHandler(MetadataPlatformInterface):
    def emit_dataset(self, dataset):
        neo4j_node = self._convert_to_neo4j(dataset)
        self.driver.write(neo4j_node)

# Planned: OpenMetadata
class OpenMetadataHandler(MetadataPlatformInterface):
    def emit_dataset(self, dataset):
        entity = self._convert_to_om_entity(dataset)
        self.client.create_or_update(entity)
```

### Switching Platforms

```yaml
# Switch from DataHub to OpenMetadata in one line
lumos_config:
  platform: openmetadata  # was: datahub
  
  platform_config:
    openmetadata:
      server: https://openmetadata.company.com
      auth_token: ${OM_TOKEN}
```

---

## Solution 7: Portable Snapshots

**Problem Solved:** Metadata trapped in vendors

### Export Format

```json
{
  "lumos_version": "1.0.0",
  "export_timestamp": "2024-01-15T10:30:00Z",
  "source_platform": "datahub",
  
  "statistics": {
    "datasets": 5247,
    "lineage_edges": 12834,
    "owners": 156,
    "tags": 89
  },
  
  "datasets": [
    {
      "urn": "lumos://snowflake/FINANCE/customers",
      "name": "customers",
      "platform": "snowflake",
      "database": "FINANCE",
      
      "schema": {
        "fields": [
          {"name": "customer_id", "type": "STRING", "nullable": false},
          {"name": "email", "type": "STRING", "nullable": true, "pii": true},
          {"name": "created_at", "type": "TIMESTAMP", "nullable": false}
        ]
      },
      
      "lineage": {
        "upstream": [
          "lumos://kafka/events.customer_signup",
          "lumos://s3/raw/crm_export"
        ],
        "downstream": [
          "lumos://snowflake/ANALYTICS/customer_360",
          "lumos://databricks/ml_features/customer_features"
        ]
      },
      
      "ownership": {
        "owners": [
          {"user": "jane.smith", "type": "TECHNICAL_OWNER"},
          {"group": "finance-data-team", "type": "DATA_STEWARD"}
        ]
      },
      
      "tags": ["pii", "customer-domain", "tier-1"],
      
      "quality": {
        "score": 98.5,
        "freshness": "2024-01-15T08:00:00Z",
        "completeness": 99.2
      },
      
      "documentation": {
        "description": "Master customer table with current state",
        "business_context": "Source of truth for customer identity"
      }
    }
  ],
  
  "glossary": [...],
  "users": [...],
  "groups": [...]
}
```

### Migration Workflow

```bash
# Export from DataHub
lumos export --platform datahub --output backup.json

# Validate export
lumos validate --input backup.json
# [ok] 5,247 datasets
# [ok] 12,834 lineage edges
# [ok] All references resolved

# Import to OpenMetadata
lumos import --input backup.json --platform openmetadata

# Verify migration
lumos diff --source datahub --target openmetadata
# [ok] 5,247/5,247 datasets migrated
# [ok] 12,834/12,834 lineage edges migrated
# [warn] 3 tags require manual mapping
```

---

## Solution 8: Data Quality Integration

**Problem Solved:** Quality is a black box

### Integration Points

```yaml
# Connect your DQ tools
quality_integrations:
  - type: great_expectations
    checkpoint_store: s3://dq-results/checkpoints/
    
  - type: dbt_tests
    manifest_path: target/manifest.json
    run_results_path: target/run_results.json
    
  - type: custom_api
    endpoint: https://dq.internal.company.com/api/v1/results
```

### Quality in Catalog

```yaml
# Dataset with embedded quality
dataset: customer_360
quality:
  overall_score: 97.8
  last_validated: 2024-01-15T08:00:00Z
  validator: great_expectations
  
  dimensions:
    completeness: 99.2%
    accuracy: 98.5%
    freshness: 45_minutes
    uniqueness: 100%
    
  checks:
    - name: "customer_id is unique"
      status: PASSED
      
    - name: "email format valid"
      status: PASSED
      
    - name: "created_at not in future"
      status: PASSED
      
    - name: "referential integrity with orders"
      status: WARNING
      details: "1.2% orphaned records"
      
  trend:
    - {date: "2024-01-13", score: 97.2}
    - {date: "2024-01-14", score: 97.5}
    - {date: "2024-01-15", score: 97.8}
    
  sla:
    target: 95%
    current: 97.8%
    status: MEETING
```

### Quality-Aware Search

```bash
$ lumos search "customer" --min-quality 95

Results (23 of 47 datasets meet quality threshold):

1. snowflake.FINANCE.customers
   Quality: 99.2% [ok] | Freshness: 30min
   
2. databricks.ml_features.customer_features  
   Quality: 97.8% [ok] | Freshness: 2h
   
# Excluded (below 95% threshold):
# - legacy.customer_backup (Quality: 72%)
# - staging.customer_raw (Quality: 85%)
```

---

## Summary: The Lumos Approach

| Problem | Traditional Approach | Lumos Approach |
|---------|---------------------|----------------|
| Raw storage | Manual registration | Auto-detection |
| Schema changes | Hope for the best | Automatic tracking + alerts |
| Documentation | Rely on humans | Automate + enforce |
| Cost | Unknown | Lineage-based attribution |
| Silos | Accept fragmentation | Unified abstraction |
| Lock-in | Accept vendor control | Portable snapshots |
| Quality | Separate tools | Embedded in catalog |

---

## Next: Technical Architecture

See [04_architecture_vision.md](04_architecture_vision.md) for detailed technical design.
