"""Lumos Framework API - Main Application Entry Point.

This module provides REST API endpoints for metadata ingestion and management.
It wraps the existing CLI-based controllers to enable programmatic access.

Usage:
    uvicorn api.main:app --reload --port 8000

API Documentation:
    - Swagger UI: http://localhost:8000/docs
    - ReDoc: http://localhost:8000/redoc
"""

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes import health, ingestion

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Create FastAPI application
app = FastAPI(
    title="Lumos Framework API",
    description="""
## Lumos Framework API

REST API for metadata ingestion and management in DataHub.

### Features

- **Ingestion**: Ingest metadata from various sources (CSV, Avro, Parquet, MongoDB, S3, PostgreSQL)
- **Health Check**: Monitor API and DataHub connection status
- **Batch Processing**: Ingest multiple sources in a single request

### Quick Start

1. Start the API server:
   ```bash
   uvicorn api.main:app --reload --port 8000
   ```

2. Test the health endpoint:
   ```bash
   curl http://localhost:8000/health
   ```

3. Ingest a CSV file:
   ```bash
   curl -X POST http://localhost:8000/api/v1/ingest \\
     -H "Content-Type: application/json" \\
     -d '{"source_type": "csv", "source_path": "sample-data-csv/"}'
   ```

### Documentation

- [Swagger UI](/docs)
- [ReDoc](/redoc)
    """,
    version="1.0.0",
    license_info={
        "name": "Apache-2.0",
        "url": "https://www.apache.org/licenses/LICENSE-2.0.html",
    },
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router, tags=["Health"])
app.include_router(ingestion.router, prefix="/api/v1", tags=["Ingestion"])


@app.on_event("startup")
async def startup_event() -> None:
    """Log startup message."""
    logger.info("🚀 Lumos Framework API starting up...")
    logger.info("📚 API documentation available at /docs")


@app.on_event("shutdown")
async def shutdown_event() -> None:
    """Log shutdown message."""
    logger.info("👋 Lumos Framework API shutting down...")


# Root endpoint redirect to docs
@app.get("/", include_in_schema=False)
async def root() -> dict:
    """Root endpoint - returns API information."""
    return {
        "name": "Lumos Framework API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
    }
