#!/usr/bin/env bash
set -euo pipefail

# Run the sales demo end-to-end: ingestion -> lineage -> enrichment
# Usage:
#   bash scripts/sales_demo.sh
#
# Notes:
# - Run from anywhere; the script will cd to the repo root.
# - Ensure configs/global_settings.yaml has your desired test_mode and gms_server.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

PYTHON_BIN="${PYTHON_BIN:-python3}"

${PYTHON_BIN} framework_cli.py \
  ingest:sample_configs_and_templates/ingestion/sales_ingestion.json \
  add-lineage:sample_configs_and_templates/lineage/sales_customers_to_orders.json \
  add-lineage:sample_configs_and_templates/lineage/sales_orders_to_order_details.json \
  add-lineage:sample_configs_and_templates/lineage/sales_products_to_order_details.json \
  enrich:sample_configs_and_templates/enrichment/sales_enrichments.json

echo "✅ Sales demo completed."


