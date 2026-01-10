# Cost Attribution Enablement Guide

## Overview

Lumos enables FinOps teams to track and attribute data costs from source to target datasets. Rather than calculating costs directly, Lumos provides the metadata and lineage information needed for businesses to calculate costs themselves.

## How Cost Attribution Works

### Storage Cost Metadata

Lumos collects storage-related metadata for each dataset:

- **Dataset Size**: Bytes and row count
- **Storage Location**: S3 bucket, GCS bucket, HDFS path
- **Storage Class**: Standard, Glacier, Infrequent Access, etc.
- **Cost Rate**: Configurable cost per GB/month

### Compute Cost Attribution

Lumos attributes compute costs via data job lineage:

- **Job Execution Costs**: Track Spark/EMR/Flink job costs
- **Lineage Mapping**: Link jobs to datasets via lineage
- **Cost Propagation**: Attribute costs from source to target datasets
- **Pipeline Cost**: Total cost of producing an output dataset

### Cost Attribution Chain

```
Source Dataset (S3) 
  -> Transformation Job ($50 compute)
    -> Intermediate Dataset
      -> Final Job ($30 compute)
        -> Output Dataset
          
Total Cost: $50 + $30 + storage costs
```

## Implementation Details

### Files to Create

1. **`feature/cost/cost_metadata_service.py`**
   - Collect cost-related metadata
   - Store cost rates configuration
   - Calculate storage costs

2. **`feature/cost/storage_cost_calculator.py`**
   - Calculate storage costs based on size and storage class
   - Support multiple cloud providers (AWS, GCP, Azure)
   - Configurable cost rates

3. **`feature/cost/compute_cost_attributor.py`**
   - Attribute compute costs via lineage
   - Track job execution costs
   - Propagate costs from source to target

4. **`feature/extraction/cost_extractor.py`**
   - Extract cost metadata for reporting
   - Generate cost reports
   - Export cost data

### Cost Metadata Schema

```json
{
  "dataset_urn": "urn:li:dataset:(s3,my_dataset,PROD)",
  "storage_cost": {
    "size_bytes": 1073741824,
    "size_gb": 1.0,
    "storage_class": "STANDARD",
    "cost_per_gb_month": 0.023,
    "monthly_cost": 0.023
  },
  "compute_cost": {
    "attributed_from_jobs": [
      {
        "job_urn": "urn:li:dataJob:(spark,my_pipeline,PROD)",
        "cost": 50.0,
        "attribution_percentage": 100.0
      }
    ],
    "total_compute_cost": 50.0
  },
  "total_cost": 50.023,
  "cost_attribution_chain": [
    {
      "source_dataset": "urn:li:dataset:(s3,source_data,PROD)",
      "job": "urn:li:dataJob:(spark,my_pipeline,PROD)",
      "cost": 50.0
    }
  ]
}
```

## Usage Examples

### Extract Cost Metadata

```bash
# Extract cost metadata for all datasets
python framework_cli.py extract:cost

# Extract cost for specific dataset
python framework_cli.py extract:cost --dataset urn:li:dataset:(s3,my_dataset,PROD)

# Export cost report to Excel
python framework_cli.py extract:cost --format excel --output cost_report.xlsx
```

### Cost Analysis

```bash
# Generate cost analysis report
python framework_cli.py extract:cost-analysis

# "What if" analysis: cost savings if feature turned off
python framework_cli.py extract:cost-analysis --what-if --feature feature_name
```

## Configuration

### Cost Rates Configuration

```yaml
cost_attribution:
  storage:
    aws_s3:
      standard: 0.023  # $ per GB/month
      glacier: 0.004
      infrequent_access: 0.0125
    gcp_gcs:
      standard: 0.020
      nearline: 0.010
      coldline: 0.004
  compute:
    spark:
      cost_per_hour: 5.0
    emr:
      cost_per_hour: 10.0
    flink:
      cost_per_hour: 8.0
```

## Export Formats

### CSV Export

```csv
dataset_urn,dataset_name,storage_cost,compute_cost,total_cost
urn:li:dataset:(s3,my_dataset,PROD),my_dataset,0.023,50.0,50.023
```

### Excel Export

Multi-sheet Excel workbook:
- **Summary**: Total costs by dataset
- **Storage Costs**: Detailed storage cost breakdown
- **Compute Costs**: Detailed compute cost attribution
- **Cost Chain**: Full cost attribution chain

## Integration with FinOps Tools

Lumos exports cost metadata in formats compatible with:

- **CloudHealth**: CSV import
- **CloudCheckr**: Excel import
- **Custom FinOps Tools**: JSON/CSV export

## Best Practices

1. **Configure Accurate Cost Rates**: Update cost rates regularly to match actual cloud costs
2. **Track Job Costs**: Integrate with job execution systems to capture actual compute costs
3. **Regular Cost Reviews**: Run cost analysis reports monthly
4. **Cost Optimization**: Use cost reports to identify expensive datasets and optimize

## Future Enhancements

- Real-time cost tracking
- Cost forecasting
- Cost anomaly detection
- Integration with cloud billing APIs

