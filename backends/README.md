# Backends

This folder will house optional infrastructure helpers. For now we rely on your existing local DataHub.

- Integration tests read the DataHub GMS URL from environment variable `DATAHUB_GMS`.
- Example: `export DATAHUB_GMS=http://localhost:8080 && pytest -m integration`

Future: we can add a docker-compose to bring up DataHub/other platforms on demand.


