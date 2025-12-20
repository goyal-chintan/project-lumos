from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Any, Dict

import pytest


def _enabled() -> bool:
    return os.getenv("LUMOS_RUN_INTEGRATION") == "1"


def _gms_url() -> str:
    return (os.getenv("METADATA_GMS") or os.getenv("DATAHUB_GMS") or "http://localhost:8080").rstrip("/")


def _graphql(query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    import requests

    resp = requests.post(
        _gms_url() + "/api/graphql",
        json={"query": query, "variables": variables or {}},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise AssertionError(f"GraphQL errors: {data['errors']}")
    return data["data"]


def _wait_for(predicate, timeout_s: float = 30.0, interval_s: float = 2.0):
    deadline = time.monotonic() + timeout_s
    last_exc: Optional[Exception] = None
    while time.monotonic() < deadline:
        try:
            val = predicate()
            if val:
                return val
        except Exception as e:  # pragma: no cover (best-effort polling)
            last_exc = e
        time.sleep(interval_s)
    if last_exc:
        raise last_exc
    return None


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@pytest.mark.integration
def test_ingestion_then_enrichment_is_visible(tmp_path) -> None:
    if not _enabled():
        pytest.skip("Set LUMOS_RUN_INTEGRATION=1 to run integration tests")

    try:
        from core.controllers import enrichment_controller, ingestion_controller
    except Exception as e:
        pytest.skip(f"Controllers not importable in this environment: {e}")

    # Preflight: if the platform isn't reachable, skip instead of failing noisily.
    try:
        _graphql("query { __typename }")
    except Exception as e:
        pytest.skip(f"Metadata platform not reachable: {e}")

    # Ingest a single small CSV so the test is quick & deterministic.
    repo_root = Path(__file__).resolve().parents[2]
    csv_path = repo_root / "sample-data-csv" / "categories.csv"
    assert csv_path.exists(), f"Expected sample file at {csv_path}"

    ingestion_cfg = [
        {
            "source_type": "csv",
            "source_path": str(csv_path),
            "infer_schema": True,
            "schema": {},
        }
    ]
    ingestion_cfg_file = tmp_path / "ingestion.json"
    ingestion_cfg_file.write_text(__import__("json").dumps(ingestion_cfg), encoding="utf-8")

    ingestion_controller.run_ingestion(str(ingestion_cfg_file))

    def _find_categories_urn() -> Optional[str]:
        q = """
        {
          search(input: {type: DATASET, query: "categories", start: 0, count: 10}) {
            searchResults {
              entity {
                urn
                ... on Dataset {
                  name
                  platform { name }
                }
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
    assert urn, "Expected dataset 'categories' to appear after ingestion"

    # Enrich description for the ingested dataset.
    new_desc = f"Integration test description ({_utc_stamp()})"
    enrich_cfg = {
        "data_type": "csv",
        "dataset_name": "categories",
        "enrichments": [{"enrichment_type": "description", "config": {"description": new_desc}}],
    }
    enrich_cfg_file = tmp_path / "enrich.json"
    enrich_cfg_file.write_text(__import__("json").dumps(enrich_cfg), encoding="utf-8")

    enrichment_controller.run_enrichment(str(enrich_cfg_file))

    def _description_matches() -> bool:
        q = """
        query($urn: String!) {
          dataset(urn: $urn) {
            urn
            properties { description }
          }
        }
        """
        data = _graphql(q, {"urn": urn})
        desc = (data.get("dataset") or {}).get("properties", {}).get("description") or ""
        return new_desc in desc

    assert _wait_for(_description_matches, timeout_s=60.0, interval_s=3.0) is True


