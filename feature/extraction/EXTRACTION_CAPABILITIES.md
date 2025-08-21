# 🔍 DataHub Comprehensive Extraction Capabilities

## Overview

The extraction directory provides comprehensive metadata extraction from DataHub with **EVERY MINOR DETAIL** available through the DataHub API. This includes schema information, lineage relationships, governance metadata, operational statistics, and much more.

## 📊 **Complete Dataset Metadata Extraction**

### 🎯 **What Can Be Extracted (Every Detail)**

#### 1. **Basic Dataset Information**
- **URN (Unique Resource Name)**: Full dataset identifier
- **Name**: Dataset display name
- **Platform**: Source platform (CSV, Avro, Hive, S3, Kafka, etc.)
- **Environment**: DEV, STAGING, PROD
- **Description**: Dataset documentation
- **Qualified Name**: Full path/name in source system
- **External URL**: Links to external documentation

#### 2. **Schema Information (Complete Field Details)**
```json
{
  "fields": [
    {
      "name": "customer_id",
      "type": "string",
      "native_type": "VARCHAR(50)",
      "description": "Unique customer identifier",
      "nullable": false,
      "tags": ["PII", "identifier"],
      "glossary_terms": ["Customer", "Identity"],
      "json_path": "$.customer.id",
      "field_path": "customer_id"
    }
  ],
  "schema_version": "1.2.0",
  "schema_hash": "abc123def456",
  "platform": "urn:li:dataPlatform:hive",
  "created": 1674291843000,
  "last_modified": 1674291843000
}
```

#### 3. **Properties & Custom Metadata**
```json
{
  "custom_properties": {
    "cloud_version": "{\"S-321\":\"11.0.0\"}",
    "versioning_system": "framework_versioning",
    "data_classification": "sensitive",
    "retention_policy": "7_years",
    "cost_center": "engineering",
    "data_owner_email": "team@company.com"
  },
  "system_properties": {
    "name": "Customer Master Data",
    "description": "Complete customer information",
    "uri": "s3://bucket/customers/"
  }
}
```

#### 4. **Ownership Information**
```json
{
  "owners": [
    {
      "type": "DATAOWNER",
      "urn": "urn:li:corpuser:jdoe", 
      "username": "jdoe"
    },
    {
      "type": "TECHNICAL_OWNER",
      "urn": "urn:li:corpGroup:data-eng",
      "username": "Data Engineering Team"
    }
  ],
  "last_modified": 1674291843000
}
```

#### 5. **Governance & Compliance**
```json
{
  "tags": ["PII", "financial", "gdpr-applicable"],
  "glossary_terms": ["Customer Data", "Revenue", "Personally Identifiable Information"],
  "domains": ["Customer Experience", "Finance"],
  "deprecation_info": {
    "deprecated": false,
    "note": "",
    "decommission_time": null
  },
  "institutional_memory": [
    {
      "url": "https://wiki.company.com/dataset/customers",
      "description": "Dataset documentation and usage guide",
      "author": "jdoe",
      "created": 1674291843000
    }
  ]
}
```

#### 6. **Lineage Relationships**
```json
{
  "upstream_datasets": [
    "urn:li:dataset:(urn:li:dataPlatform:kafka,raw_customer_events,PROD)",
    "urn:li:dataset:(urn:li:dataPlatform:s3,external_customer_data,PROD)"
  ],
  "downstream_datasets": [
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,customer_analytics,PROD)",
    "urn:li:dataset:(urn:li:dataPlatform:redshift,customer_reports,PROD)"
  ],
  "upstream_jobs": [
    "urn:li:dataJob:(urn:li:dataFlow:(airflow,customer_etl,prod),ingest_customers)"
  ],
  "downstream_jobs": [
    "urn:li:dataJob:(urn:li:dataFlow:(spark,analytics,prod),generate_reports)"
  ]
}
```

#### 7. **Operational Metadata**
```json
{
  "last_modified": 1674291843000,
  "created": 1674281843000,
  "size_bytes": 1073741824,
  "row_count": 1000000,
  "column_count": 25,
  "profile_run_id": "profile_20240121_1300",
  "profile_timestamp": 1674291843000
}
```

## 🛠️ **Extraction Services Available**

### 1. **Comprehensive Extractor** (`comprehensive_dataset_extractor.py`)
**Extracts EVERYTHING** - All metadata types in one operation
```python
# Usage
extractor = ComprehensiveDatasetExtractor(config_manager)
datasets = extractor.extract_all_datasets_comprehensive()

# What it extracts:
# ✅ Basic info (name, platform, environment, description)
# ✅ Complete schema (fields, types, descriptions, tags, glossary terms)
# ✅ Properties (custom + system properties)
# ✅ Ownership (owners, types, last modified)
# ✅ Governance (tags, glossary terms, domains, deprecation, documentation)
# ✅ Lineage (upstream/downstream datasets and jobs)
# ✅ Operations (size, row count, profiling info)
```

### 2. **Schema Extractor** (`schema_extractor_service.py`)
**Specialized for schema analysis**
```python
# Focus areas:
# ✅ Field definitions and data types
# ✅ Type distribution analysis
# ✅ Field name frequency analysis
# ✅ Platform-specific type mappings
# ✅ Nested structure analysis (JSON paths)
# ✅ Schema evolution tracking
```

### 3. **Lineage Extractor** (`lineage_extractor_service.py`)
**Specialized for data flow analysis**
```python
# Focus areas:
# ✅ Upstream/downstream relationships
# ✅ Data job dependencies
# ✅ Impact analysis (high-impact datasets)
# ✅ Lineage graph generation (nodes/edges)
# ✅ Platform distribution analysis
# ✅ Isolated dataset identification
```

### 4. **Governance Extractor** (`governance_extractor_service.py`)
**Specialized for compliance and governance**
```python
# Focus areas:
# ✅ Tag analysis and distribution
# ✅ Glossary term usage
# ✅ Ownership patterns
# ✅ Compliance status (deprecation, documentation)
# ✅ Domain organization
# ✅ Field-level governance
```

## 📋 **Sample Extraction Results**

### **Real DataHub Instance Results**
Based on your DataHub instance with 51 datasets:

```
Platform Breakdown:
  AVRO: 20 datasets    (IoT sensor data, device metrics)
  CSV: 24 datasets     (Business data, location info)
  HIVE: 4 datasets     (Analytics tables, fact tables)
  HDFS: 1 dataset      (Raw data storage)
  KAFKA: 1 dataset     (Streaming data)
  S3: 1 dataset        (Cloud storage)

Extraction Capabilities Verified:
✅ Version tracking (cloud_version custom properties)
✅ Lineage relationships detected
✅ Ownership information captured
✅ Platform diversity supported
✅ Custom properties preserved
```

### **Field-Level Details Example**
```json
{
  "dataset_name": "location_master_dimension",
  "platform": "csv",
  "fields": [
    {
      "name": "location_id",
      "type": "string",
      "native_type": "VARCHAR",
      "description": "Unique location identifier",
      "nullable": false,
      "tags": ["identifier", "location"],
      "glossary_terms": ["Location", "Primary Key"]
    },
    {
      "name": "customer_id", 
      "type": "string",
      "description": "Associated customer ID",
      "tags": ["foreign_key", "customer"]
    }
  ]
}
```

## 🚀 **Integration with Framework**

### **Configuration Format**
```json
{
  "extraction_type": "comprehensive",
  "output_path": "extraction_results.json", 
  "datasets": "all",
  "include_field_lineage": true,
  "include_ownership_details": true,
  "include_compliance_info": true,
  "max_depth": 3,
  "direction": "both"
}
```

### **CLI Integration** (Recommended)
```bash
# Extract everything
python framework_cli.py extract:configs/extraction_config.json

# Schema analysis only
python framework_cli.py extract-schema:configs/schema_config.json

# Lineage analysis only  
python framework_cli.py extract-lineage:configs/lineage_config.json

# Governance analysis only
python framework_cli.py extract-governance:configs/governance_config.json
```

## 🎯 **What Should Be in the Extraction Directory**

### **Current Structure** ✅
```
feature/extraction/
├── __init__.py
├── base_extraction_service.py           # Interface for all extractors
├── comprehensive_dataset_extractor.py   # Complete metadata extraction  
├── schema_extractor_service.py          # Schema-focused extraction
├── lineage_extractor_service.py         # Lineage-focused extraction
├── governance_extractor_service.py      # Governance-focused extraction
└── extraction_factory.py                # Factory for creating extractors
```

### **Recommended Additions** 🚀
```
feature/extraction/
├── properties_extractor_service.py      # Custom properties analysis
├── usage_extractor_service.py           # Usage patterns and statistics
├── quality_extractor_service.py         # Data quality metrics
├── assertions_extractor_service.py      # Data quality assertions
├── profiling_extractor_service.py       # Data profiling results
├── impact_extractor_service.py          # Change impact analysis
├── metadata_diff_service.py             # Schema evolution tracking
└── export/
    ├── excel_exporter.py                # Excel export capability
    ├── csv_exporter.py                  # CSV export capability
    └── visualization_exporter.py        # Graph/chart generation
```

## 🔥 **Key Capabilities Summary**

| **Metadata Type** | **Details Extracted** | **Use Cases** |
|-------------------|----------------------|---------------|
| **Schema** | Fields, types, descriptions, tags, glossary terms, nested structures | Schema analysis, type mapping, data modeling |
| **Lineage** | Upstream/downstream datasets, jobs, impact analysis, data flow | Impact analysis, debugging, compliance |
| **Governance** | Tags, glossary terms, ownership, domains, compliance status | Data governance, compliance, security |
| **Properties** | Custom properties, version info, system metadata | Asset management, lifecycle tracking |
| **Operations** | Size, row count, profiling, last modified | Performance monitoring, optimization |
| **Relationships** | Dataset dependencies, job connections, platform mapping | Architecture understanding, migration planning |

## 💡 **Best Practices**

1. **Start with Comprehensive**: Use `comprehensive_dataset_extractor.py` to get everything
2. **Specialize as Needed**: Use focused extractors (`schema`, `lineage`, `governance`) for specific analysis
3. **Regular Extraction**: Set up scheduled extraction for metadata drift detection
4. **Version Tracking**: Use custom properties to track metadata evolution
5. **Export Options**: Convert JSON results to Excel/CSV for business users
6. **Automation**: Integrate extraction into CI/CD pipelines for data governance

This extraction framework provides **complete visibility** into your DataHub metadata with every minor detail captured and organized for analysis! 🎯
