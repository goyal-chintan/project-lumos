#!/bin/bash
# Script to start DataHub using docker-compose

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Starting DataHub..."
docker compose up -d

echo ""
echo "Waiting for services to be healthy..."
sleep 10

echo ""
echo "Checking service status..."
docker compose ps

echo ""
echo "DataHub should be available at:"
echo "  - UI: http://localhost:9002 (username: datahub, password: datahub)"
echo "  - GMS API: http://localhost:8080"
echo ""
echo "To view logs: docker compose logs -f"
echo "To stop: docker compose down"

