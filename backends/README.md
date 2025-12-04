# Backends

This folder contains infrastructure setup files for running DataHub and other platforms locally.

## DataHub Setup

### Quick Start

1. **Start DataHub:**
   ```bash
   cd backends
   docker compose up -d
   ```
   
   Or use the helper script:
   ```bash
   ./backends/start-datahub.sh
   ```

2. **Verify DataHub is running:**
   ```bash
   docker compose ps
   ```
   All services should show as "healthy" or "Up".

3. **Access DataHub UI:**
   - Open your browser and navigate to: http://localhost:9002
   - Default credentials:
     - Username: `datahub`
     - Password: `datahub`

4. **Verify GMS endpoint:**
   ```bash
   curl http://localhost:8080/config
   ```

### Stop DataHub

```bash
cd backends
docker compose down
```

Or use the helper script:
```bash
./backends/stop-datahub.sh
```

### Stop and Remove All Data

```bash
cd backends
docker compose down -v
```

This will remove all containers and volumes (including data).

### Configuration

The `docker-compose.yml` file is the official DataHub quickstart configuration downloaded from the DataHub project. It includes:
- DataHub GMS (GraphQL Management Service) on port 8080
- DataHub Frontend on port 9002
- MySQL database on port 3306
- Elasticsearch on port 9200
- Kafka broker on port 9092
- Neo4j on ports 7474 and 7687
- Schema Registry on port 8081
- Zookeeper on port 2181

### Integration Tests

Integration tests read the DataHub GMS URL from environment variable `DATAHUB_GMS`:
```bash
export DATAHUB_GMS=http://localhost:8080
pytest -m integration
```

### Troubleshooting

- **Port conflicts:** If you have services running on the default ports, you can modify the port mappings in `docker-compose.yml` or stop conflicting services.
- **Check logs:** `docker compose logs [service-name]` to see logs for a specific service.
- **Reset everything:** `docker compose down -v` to remove all data and start fresh.


