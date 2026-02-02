# How to Ingest Data via API

This guide provides step-by-step instructions for ingesting new files into DataHub using the Lumos API.

## Prerequisites

1.  **Start the API Server**:
    ```bash
    uvicorn api.main:app --reload --port 8000
    ```
2.  **Ensure DataHub is running**:
    Verify at `http://localhost:9002`.

---

## Scenario 1: Ingesting a Single New File

**Use case**: You have a new CSV file `data/sales_Q1.csv` that you want to add.

**Step 1: Place the file**
Ensure the file is accessible to the server running the API.
Example path: `/Users/skalamani/Documents/project-lumos/data/sales_Q1.csv`

**Step 2: Send the Request**
Run the following command in your terminal:

```bash
curl -X POST http://localhost:8000/api/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "source_type": "csv",
    "source_path": "/Users/skalamani/Documents/project-lumos/data/sales_Q1.csv",
    "dataset_name": "sales_q1_2026"
  }'
```

**Step 3: Verification**
*   Check the API response for `"status": "success"`.
*   Go to DataHub (http://localhost:9002) and search for "sales_q1_2026".

---

## Scenario 2: Ingesting an Entire Folder

**Use case**: You have a folder `data/daily_logs/` containing 50 CSV files.

**Step 1: Check the folder**
Ensure all files in the folder are of the same type (e.g., all CSVs).

**Step 2: Send the Request**
Point `source_path` to the directory.

```bash
curl -X POST http://localhost:8000/api/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "source_type": "csv",
    "source_path": "/Users/skalamani/Documents/project-lumos/data/daily_logs/"
  }'
```

**Step 3: Verification**
The API response will show `"datasets_ingested": 50` (or however many files were found).

---

## Scenario 3: Batch Ingest (Multiple Specific Files)

**Use case**: You want to ingest `users.csv` and `products.parquet` in one go.

**Step 1: Prepare the Request**

```bash
curl -X POST http://localhost:8000/api/v1/ingest/batch \
  -H "Content-Type: application/json" \
  -d '{
    "sources": [
      {
        "source_type": "csv",
        "source_path": "data/users.csv"
      },
      {
        "source_type": "parquet",
        "source_path": "data/products.parquet"
      }
    ],
    "continue_on_error": true
  }'
```

---

## Troubleshooting

*   **Error: "File not found"**: Double-check the `source_path`. It must be an **absolute path** or relative to where you started the `uvicorn` server.
*   **Error: "Connection refused"**: Make sure the API server is running (`ps aux | grep uvicorn`).
*   **Dataset not showing**: Check the DataHub container logs or verify you are looking in the correct Environment (DEV/PROD).
