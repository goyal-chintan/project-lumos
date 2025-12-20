# Cost Management Guide

This guide focuses on operational commands and workflows. For the conceptual model, metadata schema, and configuration, see `docs/vision/13_cost_attribution.md`.

## Cost Calculation

### Storage Costs

Calculate storage costs for datasets:

```bash
# Calculate storage costs for all datasets
python framework_cli.py cost:calculate --type storage

# Calculate for specific dataset
python framework_cli.py cost:calculate --type storage --dataset customer_data

# Include historical data
python framework_cli.py cost:calculate --type storage --include-history
```

**Example Output:**
```json
{
  "dataset": "customer_data",
  "storage_cost": {
    "monthly_cost": 125.50,
    "storage_size_gb": 1255,
    "storage_class": "Standard",
    "region": "us-east-1",
    "breakdown": {
      "base_storage": 100.40,
      "requests": 15.10,
      "data_transfer": 10.00
    }
  }
}
```

### Compute Costs

Calculate compute costs for pipelines:

```bash
# Calculate compute costs for all pipelines
python framework_cli.py cost:calculate --type compute

# Calculate for specific pipeline
python framework_cli.py cost:calculate --type compute --pipeline customer_analytics_pipeline

# Include resource utilization
python framework_cli.py cost:calculate --type compute --include-utilization
```

**Example Output:**
```json
{
  "pipeline": "customer_analytics_pipeline",
  "compute_cost": {
    "monthly_cost": 450.00,
    "executions_per_month": 30,
    "avg_execution_time_minutes": 45,
    "resource_usage": {
      "cpu_hours": 22.5,
      "memory_gb_hours": 180.0
    },
    "breakdown": {
      "compute_instances": 350.00,
      "data_processing": 75.00,
      "network": 25.00
    }
  }
}
```

### Total Cost of Ownership (TCO)

Calculate complete TCO for a dataset:

```bash
# Calculate TCO for dataset
python framework_cli.py cost:tco --dataset customer_data

# Include downstream costs
python framework_cli.py cost:tco --dataset customer_data --include-downstream

# Include quality/maintenance costs
python framework_cli.py cost:tco --dataset customer_data --include-maintenance
```

## Cost Attribution

### Attribute Costs to Pipelines

Link dataset costs to the pipelines that create them:

```bash
# Attribute costs to pipelines
python framework_cli.py cost:attribute --dataset customer_data

# Show cost breakdown by pipeline
python framework_cli.py cost:attribute --dataset customer_data --breakdown pipeline
```

### Attribute Costs to Features

Attribute costs to business features:

```bash
# Attribute costs to feature
python framework_cli.py cost:attribute --feature customer_analytics

# Show cost breakdown
python framework_cli.py cost:attribute --feature customer_analytics --breakdown dataset
```

### Cost Allocation

Allocate costs to teams/departments:

```bash
# Allocate costs by ownership
python framework_cli.py cost:allocate --by ownership

# Allocate costs by team
python framework_cli.py cost:allocate --by team

# Generate allocation report
python framework_cli.py cost:allocate --by ownership --output allocation_report.xlsx
```

## Cost Savings Analysis

### Identify Cost Savings Opportunities

Find opportunities to reduce costs:

```bash
# Find cost savings opportunities
python framework_cli.py cost:savings --analyze

# Focus on high-cost datasets
python framework_cli.py cost:savings --analyze --min-cost 1000

# Include recommendations
python framework_cli.py cost:savings --analyze --recommendations
```

**Savings Opportunities:**
- Unused/orphaned datasets
- Over-provisioned resources
- Inefficient storage classes
- Duplicate datasets
- Underutilized pipelines

### Feature Deprecation Analysis

Calculate savings from deprecating a feature:

```bash
# Calculate savings from deprecating feature
python framework_cli.py cost:savings --deprecate-feature customer_analytics_v1

# Show impact analysis
python framework_cli.py cost:savings --deprecate-feature customer_analytics_v1 --impact-analysis
```

**Deprecation Analysis:**
- Direct costs (storage, compute)
- Indirect costs (downstream dependencies)
- Migration costs
- Net savings

**Example Output:**
```json
{
  "feature": "customer_analytics_v1",
  "deprecation_analysis": {
    "direct_monthly_cost": 1250.00,
    "indirect_monthly_cost": 350.00,
    "migration_cost_one_time": 500.00,
    "net_monthly_savings": 1600.00,
    "payback_period_months": 0.31,
    "datasets_affected": 5,
    "pipelines_affected": 3,
    "recommendations": [
      "Migrate to customer_analytics_v2",
      "Archive historical data to Glacier",
      "Reduce compute resources by 30%"
    ]
  }
}
```

### Storage Optimization

Optimize storage costs:

```bash
# Analyze storage optimization
python framework_cli.py cost:optimize --type storage

# Recommend storage class changes
python framework_cli.py cost:optimize --type storage --recommend-storage-class

# Identify cold data
python framework_cli.py cost:optimize --type storage --identify-cold-data
```

**Storage Optimization:**
- Move cold data to cheaper storage (Glacier)
- Delete unused datasets
- Compress data
- Optimize partitioning

### Compute Optimization

Optimize compute costs:

```bash
# Analyze compute optimization
python framework_cli.py cost:optimize --type compute

# Recommend resource changes
python framework_cli.py cost:optimize --type compute --recommend-resources

# Identify inefficient pipelines
python framework_cli.py cost:optimize --type compute --identify-inefficient
```

**Compute Optimization:**
- Right-size compute resources
- Optimize pipeline execution
- Reduce execution frequency
- Use spot instances

## Cost Reporting

### Generate Cost Reports

Create comprehensive cost reports:

```bash
# Generate monthly cost report
python framework_cli.py cost:report --period monthly --output cost_report_2024_01.xlsx

# Generate cost report by team
python framework_cli.py cost:report --period monthly --group-by team --output team_costs.xlsx

# Generate cost trend report
python framework_cli.py cost:report --period monthly --trend 6 --output cost_trends.xlsx
```

**Report Types:**
- Monthly/quarterly/annual reports
- Team/department breakdowns
- Dataset-level reports
- Pipeline-level reports
- Cost trend analysis

### Cost Dashboards

Visualize costs:

```bash
# Generate cost dashboard
python framework_cli.py cost:dashboard --output cost_dashboard.html

# Include savings opportunities
python framework_cli.py cost:dashboard --include-savings --output dashboard_with_savings.html
```

**Dashboard Features:**
- Cost breakdown charts
- Trend visualizations
- Top cost drivers
- Savings opportunities
- Cost allocation pie charts

## Cost Tracking

### Track Cost Trends

Monitor cost trends over time:

```bash
# Track cost trends
python framework_cli.py cost:track --dataset customer_data --period 6

# Compare periods
python framework_cli.py cost:track --dataset customer_data --compare-periods 2024-01,2024-02
```

### Set Cost Alerts

Set up cost alerts:

```bash
# Set cost threshold alert
python framework_cli.py cost:alert --dataset customer_data --threshold 1000 --action notify

# Set cost increase alert
python framework_cli.py cost:alert --dataset customer_data --increase-threshold 20 --action notify
```

**Alert Types:**
- Cost threshold exceeded
- Cost increase percentage
- Unusual cost spikes
- Budget exceeded

## Examples

### Example 1: Calculate Dataset Costs

```bash
# Calculate total cost for customer_data dataset
python framework_cli.py cost:tco --dataset customer_data

# Output:
# Storage: $125.50/month
# Compute: $450.00/month
# Network: $25.00/month
# Total: $600.50/month
```

### Example 2: Find Savings Opportunities

```bash
# Analyze cost savings
python framework_cli.py cost:savings --analyze --min-cost 500

# Output:
# Found 15 opportunities:
# - customer_analytics_v1: $1,250/month (deprecate)
# - unused_datasets: $800/month (delete)
# - cold_data: $600/month (move to Glacier)
# Total potential savings: $2,650/month
```

### Example 3: Feature Deprecation

```bash
# Calculate savings from deprecating feature
python framework_cli.py cost:savings --deprecate-feature customer_analytics_v1

# Output:
# Monthly savings: $1,600
# Migration cost: $500 (one-time)
# Payback period: 0.31 months
# Recommendation: Deprecate immediately
```

## Support

For cost management questions:
- Check [documentation](../README.md)
- Review [vision index](README.md)
- Open an issue on GitHub
