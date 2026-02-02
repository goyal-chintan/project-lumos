# Lumos Ingestion API

This document describes the REST API endpoints for metadata ingestion in the Lumos Framework. The API provides a programmatic alternative to the CLI for ingesting metadata into DataHub.

## Quick Start

### Start the API Server

```bash
# Development mode with auto-reload
uvicorn api.main:app --reload --port 8000

# Production mode
uvicorn api.main:app --host 0.0.0.0 --port 8000
```

### API Documentation

Once the server is running, interactive API documentation is available at:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

---

## Endpoints

### Health Check

Check the health status of the API and DataHub connection.

```
GET /health
```

**Response:**

```json
{
    "status": "healthy",
    "version": "1.0.0",
    "datahub_connected": true,
    "timestamp": "2026-02-02T04:00:00.000000"
}
```

---

### List Supported Sources

Get a list of all supported data source types and their configuration requirements.

```
GET /api/v1/ingest/sources
```

**Response:**

```json
{
    "sources": [
        {
            "type": "csv",
            "description": "CSV files or directories",
            "required_fields": ["source_path"],
            "optional_fields": ["delimiter", "dataset_name", "partitioning_format"]
        },
        {
            "type": "avro",
            "description": "Avro files or directories",
            "required_fields": ["source_path"],
            "optional_fields": ["dataset_name", "partitioning_format"]
        }
    ],
    "timestamp": "2026-02-02T04:00:00.000000"
}
```

---

### Ingest Single Source

Ingest metadata from a single data source.

```
POST /api/v1/ingest
Content-Type: application/json
```

**Request Body:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source_type` | string | Yes | Type: `csv`, `avro`, `parquet`, `mongodb`, `s3`, `postgresql` |
| `source_path` | string | Depends | Path to file/directory (required for file-based sources) |
| `dataset_name` | string | No | Custom name for the dataset |
| `delimiter` | string | No | Delimiter for CSV files (default: `,`) |
| `fully_qualified_source_name` | string | Depends | For database sources (MongoDB, PostgreSQL) |
| `data_type` | string | Depends | For S3 sources: `csv`, `avro`, `parquet` |
| `partitioning_format` | string | No | Partition format (e.g., `year=YYYY/month=MM`) |
| `ingestion_timestamp` | string | No | ISO-8601 timestamp for partitioned ingestion |

**Example - Single CSV File:**

```bash
curl -X POST http://localhost:8000/api/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "source_type": "csv",
    "source_path": "sample-data-csv/test_api/employees.csv"
  }'
```

**Example - Directory of CSV Files:**

```bash
curl -X POST http://localhost:8000/api/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "source_type": "csv",
    "source_path": "sample-data-csv/test_api/",
    "delimiter": ","
  }'
```

**Response:**

```json
{
    "status": "success",
    "message": "Ingestion completed successfully",
    "timestamp": "2026-02-02T04:00:00.000000",
    "datasets_ingested": 1,
    "details": {
        "success": true,
        "datasets_ingested": 1,
        "source_type": "csv",
        "source_path": "sample-data-csv/test_api/employees.csv"
    }
}
```

---

### Batch Ingestion

Ingest metadata from multiple sources in a single request.

```
POST /api/v1/ingest/batch
Content-Type: application/json
```

**Request Body:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `sources` | array | Yes | List of source configurations (same format as single ingest) |
| `continue_on_error` | boolean | No | Continue processing if one source fails (default: `true`) |

**Example:**

```bash
curl -X POST http://localhost:8000/api/v1/ingest/batch \
  -H "Content-Type: application/json" \
  -d '{
    "sources": [
      {"source_type": "csv", "source_path": "sample-data-csv/test_api/products.csv"},
      {"source_type": "csv", "source_path": "sample-data-csv/test_api/orders.csv"}
    ],
    "continue_on_error": true
  }'
```

**Response:**

```json
{
    "status": "success",
    "message": "Batch ingestion completed: 2 succeeded, 0 failed",
    "timestamp": "2026-02-02T04:00:00.000000",
    "total_sources": 2,
    "successful": 2,
    "failed": 0,
    "results": [
        {
            "index": 0,
            "status": "success",
            "source_type": "csv",
            "details": { ... }
        },
        {
            "index": 1,
            "status": "success",
            "source_type": "csv",
            "details": { ... }
        }
    ]
}
```

---

### Ingest from Config File

Upload a JSON configuration file for ingestion.

```
POST /api/v1/ingest/file
Content-Type: multipart/form-data
```

**Request:**

```bash
curl -X POST http://localhost:8000/api/v1/ingest/file \
  -F "file=@path/to/config.json"
```

**Config File Format (JSON):**

```json
[
  {
    "source_type": "csv",
    "source_path": "sample-data-csv/test_api/employees.csv",
    "delimiter": ","
  }
]
```

**Response:**

```json
{
    "status": "success",
    "message": "Ingestion from file completed successfully",
    "timestamp": "2026-02-02T04:00:00.000000",
    "datasets_ingested": 1,
    "details": { ... }
}
```

---

## Source Type Examples

### CSV Files

```bash
# Single file
curl -X POST http://localhost:8000/api/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "source_type": "csv",
    "source_path": "data/customers.csv",
    "delimiter": ","
  }'

# Directory
curl -X POST http://localhost:8000/api/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "source_type": "csv",
    "source_path": "data/csv_files/"
  }'
```

### Avro Files

```bash
curl -X POST http://localhost:8000/api/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "source_type": "avro",
    "source_path": "data/events.avro"
  }'
```

### Parquet Files

```bash
curl -X POST http://localhost:8000/api/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "source_type": "parquet",
    "source_path": "data/transactions.parquet"
  }'
```

### S3 Sources

```bash
curl -X POST http://localhost:8000/api/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "source_type": "s3",
    "source_path": "s3://my-bucket/data/",
    "data_type": "parquet"
  }'
```

### MongoDB

```bash
curl -X POST http://localhost:8000/api/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "source_type": "mongodb",
    "fully_qualified_source_name": "mongodb://localhost:27017/mydb.collection"
  }'
```

---

## Error Handling

### HTTP Status Codes

| Code | Description |
|------|-------------|
| 200 | Success |
| 400 | Bad Request - Invalid configuration or missing required fields |
| 404 | Not Found - Source file or path not found |
| 500 | Internal Server Error - Ingestion failed |

### Error Response Format

```json
{
    "detail": "Error message describing what went wrong"
}
```

---

## Configuration

The API uses the global configuration from `configs/global_settings.yaml`:

```yaml
datahub:
  gms_server: http://localhost:8080
  test_mode: false

default_env: DEV
default_platform: datahub
```

---

## Integration with CI/CD

Example GitHub Actions workflow for automated ingestion:

```yaml
name: Ingest Metadata

on:
  push:
    paths:
      - 'data/**'

jobs:
  ingest:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Start API Server
        run: |
          pip install -r requirements.txt
          uvicorn api.main:app --port 8000 &
          sleep 5
          
      - name: Ingest Data
        run: |
          curl -X POST http://localhost:8000/api/v1/ingest \
            -H "Content-Type: application/json" \
            -d '{"source_type": "csv", "source_path": "data/"}'
```

---

## Python Client Example

```python
import requests

API_BASE = "http://localhost:8000"

# Health check
response = requests.get(f"{API_BASE}/health")
print(response.json())

# Single ingestion
response = requests.post(
    f"{API_BASE}/api/v1/ingest",
    json={
        "source_type": "csv",
        "source_path": "data/customers.csv"
    }
)
print(response.json())

# Batch ingestion
response = requests.post(
    f"{API_BASE}/api/v1/ingest/batch",
    json={
        "sources": [
            {"source_type": "csv", "source_path": "data/orders.csv"},
            {"source_type": "csv", "source_path": "data/products.csv"}
        ]
    }
)
print(response.json())
```

---

## Related Documentation

- [Architecture](ARCHITECTURE.md)
- [Contributing](../CONTRIBUTING.md)
- [CLI Operations](../README.md#cli-operations-examples)
