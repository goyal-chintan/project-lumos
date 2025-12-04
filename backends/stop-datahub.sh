#!/bin/bash
# Script to stop DataHub

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Stopping DataHub..."
docker compose down

echo ""
echo "DataHub stopped."
echo ""
echo "To remove all data: docker compose down -v"

