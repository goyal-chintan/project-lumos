from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path

import pytest
import requests

from core.controllers import ingestion_controller


def _enabled() -> bool:
    return os.getenv("LUMOS_RUN_INTEGRATION") == "1"


def _gms_url() -> str:
    return os.getenv("DATAHUB_GMS", "http://localhost:8080").rstrip("/")


def _graphql(query: str, variables: dict | None = None) -> dict:
    resp = requests.post(
        _gms_url() + "/api/graphql",
        json={"query": query, "variables": variables or {}},
        timeout=10,
    )
    resp.raise_for_status()
    payload = resp.json()
    if "errors" in payload:
        raise AssertionError(f"GraphQL errors: {payload['errors']}")
    return payload["data"]


def _wait_for(predicate, timeout_s: float = 60.0, interval_s: float = 3.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        val = predicate()
        if val:
            return val
        time.sleep(interval_s)
    return None


_ISO_UTC_MS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


@pytest.mark.integration
def test_ingestion_creates_dataset_with_schema_and_custom_properties(tmp_path) -> None:
    if not _enabled():
        pytest.skip("Set LUMOS_RUN_INTEGRATION=1 to run integration tests")

    # Preflight GraphQL.
    try:
        _graphql("query { __typename }")
    except Exception as e:
        pytest.skip(f"DataHub GMS not reachable: {e}")

    repo_root = Path(__file__).resolve().parents[2]
    csv_path = repo_root / "sample-data-csv" / "categories.csv"
    assert csv_path.exists(), f"Expected sample file at {csv_path}"

    cfg_file = tmp_path / "ingestion.json"
    cfg_file.write_text(json.dumps([{"source_type": "csv", "source_path": str(csv_path), "infer_schema": True, "schema": {}}]), encoding="utf-8")
    ingestion_controller.run_ingestion(str(cfg_file))

    def _find_categories_urn() -> str | None:
        q = """
        {
          search(input: {type: DATASET, query: "categories", start: 0, count: 10}) {
            searchResults {
              entity {
                urn
                ... on Dataset { name }
              }
            }
          }
        }
        """
        data = _graphql(q)
        for r in data["search"]["searchResults"]:
            ent = r["entity"]
            if ent.get("name") == "categories":
                return ent["urn"]
        return None

    urn = _wait_for(_find_categories_urn, timeout_s=60.0, interval_s=3.0)
    assert urn, "Expected dataset 'categories' to appear in DataHub after ingestion"

    q = """
    query($urn: String!) {
      dataset(urn: $urn) {
        urn
        schemaMetadata { fields { fieldPath } }
        properties { customProperties { key value } }
      }
    }
    """
    ds = (_graphql(q, {"urn": urn}).get("dataset") or {})
    fields = {f["fieldPath"] for f in (ds.get("schemaMetadata") or {}).get("fields") or []}
    assert {"id", "name", "description"}.issubset(fields)

    props_entries = (ds.get("properties") or {}).get("customProperties") or []
    props = {e.get("key"): e.get("value") for e in props_entries if isinstance(e, dict)}

    assert props.get("source_type") == "csv"
    assert "ingestion_timestamp" in props
    assert _ISO_UTC_MS.match(props["ingestion_timestamp"] or "")


