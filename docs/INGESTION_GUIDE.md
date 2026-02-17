# How to Ingest Data

This guide provides step-by-step instructions for ingesting new files into DataHub using the Lumos Framework CLI (`framework_cli.py`).

## Prerequisites

1.  **Ensure DataHub is running**:
    Verify at `http://localhost:9002`.

2.  **Navigate to the project root**:
    All commands below assume you are in the project root directory.

---

## Ingestion Template

The framework provides a ready-to-use ingestion template at
[`sample_configs_and_templates/ingestion/ingestion_template.json`](../sample_configs_and_templates/ingestion/ingestion_template.json)
covering all supported source types:

```json
[
  {
    "source_type": "s3",
    "data_type": "avro",
    "source_path": "s3://my-bucket/data/table",
    "partitioning_format": "year=%Y/month=%m/day=%d",
    "infer_schema": false,
    "schema": { "id": "int", "name": "string", "age": "int" }
  },
  {
    "source_type": "postgres",
    "fully_qualified_source_name": "sample_prod.red_schema.wifi_table",
    "infer_schema": false,
    "schema": { "id": "int", "name": "string", "age": "int" }
  },
  {
    "source_type": "mongodb",
    "fully_qualified_source_name": "sample_prod.NodeState",
    "infer_schema": false,
    "schema": { "id": "int", "name": "string", "age": "int" }
  },
  {
    "source_type": "csv",
    "source_path": "/path/to/your/csv_files/",
    "delimiter": ",",
    "infer_schema": true,
    "schema": {}
  },
  {
    "source_type": "avro",
    "source_path": "/path/to/your/avro_files/",
    "infer_schema": true,
    "schema": {}
  },
  {
    "source_type": "parquet",
    "source_path": "/path/to/your/parquet_files/",
    "infer_schema": true,
    "schema": {}
  }
]
```

Copy the relevant block(s) from this template, update the paths/names for your data, and save as a new config file.

---

## Scenario 1: Ingesting a Single New File

**Use case**: You have a new CSV file `data/sales_Q1.csv` that you want to add.

**Step 1: Create a config file**

Create a JSON config (e.g., `configs/sales_ingestion.json`):

```json
[
  {
    "source_type": "csv",
    "source_path": "./data/sales_Q1.csv",
    "delimiter": ",",
    "infer_schema": true,
    "schema": {}
  }
]
```

**Step 2: Run the CLI**

```bash
python framework_cli.py ingest:configs/sales_ingestion.json
```

**Step 3: Verification**
*   Check the CLI output for a success message.
*   Go to DataHub (`http://localhost:9002`) and search for `sales_Q1`.

---

## Scenario 2: Ingesting an Entire Folder

**Use case**: You have a folder `data/daily_logs/` containing multiple CSV files.

**Step 1: Create a config file**

Create `configs/daily_logs_ingestion.json`:

```json
[
  {
    "source_type": "csv",
    "source_path": "./data/daily_logs/",
    "infer_schema": true,
    "schema": {}
  }
]
```

**Step 2: Run the CLI**

```bash
python framework_cli.py ingest:configs/daily_logs_ingestion.json
```

**Step 3: Verification**
The CLI output will indicate how many datasets were ingested from the folder.

---

## Scenario 3: Batch Ingest (Multiple Sources)

**Use case**: You want to ingest `data/users.csv` and `data/products.parquet` in one go.

**Step 1: Create a config file**

Create `configs/batch_ingestion.json` with multiple entries:

```json
[
  {
    "source_type": "csv",
    "source_path": "./data/users.csv",
    "infer_schema": true,
    "schema": {}
  },
  {
    "source_type": "parquet",
    "source_path": "./data/products.parquet",
    "infer_schema": true,
    "schema": {}
  }
]
```

**Step 2: Run the CLI**

```bash
python framework_cli.py ingest:configs/batch_ingestion.json
```

---

## Alternative: API Endpoint

For programmatic access, the API server also supports ingestion. The full API contract template is available at
[`sample_configs_and_templates/ingestion/api_ingestion_template.json`](../sample_configs_and_templates/ingestion/api_ingestion_template.json).

1.  **Start the API server**:
    ```bash
    uvicorn api.main:app --reload --port 8000
    ```

2.  **Single source** — `POST /api/v1/ingest`:
    ```bash
    curl -X POST http://localhost:8000/api/v1/ingest \
      -H "Content-Type: application/json" \
      -d '{
        "source_type": "csv",
        "source_path": "./data/sales_Q1.csv",
        "dataset_name": "sales_q1_2026"
      }'
    ```

3.  **Batch ingest** — `POST /api/v1/ingest/batch`:
    ```bash
    curl -X POST http://localhost:8000/api/v1/ingest/batch \
      -H "Content-Type: application/json" \
      -d '{
        "sources": [
          { "source_type": "csv", "source_path": "./data/users.csv" },
          { "source_type": "parquet", "source_path": "./data/products.parquet" }
        ],
        "continue_on_error": true
      }'
    ```

4.  **Upload config file** — `POST /api/v1/ingest/file`:
    ```bash
    curl -X POST http://localhost:8000/api/v1/ingest/file \
      -F "file=@configs/sales_ingestion.json"
    ```

5.  **View supported sources**: `GET /api/v1/ingest/sources`

6.  **API docs**: Swagger UI at `http://localhost:8000/docs`

---

## Troubleshooting

*   **Error: "File not found"**: Double-check the `source_path` in your config file. It must be a valid path relative to where you run the CLI (or an absolute path).
*   **Error: "Connection refused"**: Make sure DataHub is running and accessible at the URL configured in `configs/global_settings.yaml`.
*   **Dataset not showing in DataHub**: Check the DataHub container logs or verify you are looking in the correct Environment (DEV/PROD).
