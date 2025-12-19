from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest
import requests

from core.common.utils import sanitize_entity_id
from core.controllers import enrichment_controller, ingestion_controller


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


def _utc_stamp() -> str:
    # Proper ISO-8601 UTC with milliseconds (NOT epoch): 2025-12-01T20:33:00.103Z
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@pytest.mark.integration
def test_enrichment_updates_show_up_in_datahub(tmp_path) -> None:
    if not _enabled():
        pytest.skip("Set LUMOS_RUN_INTEGRATION=1 to run integration tests")

    # Preflight GraphQL.
    try:
        _graphql("query { __typename }")
    except Exception as e:
        pytest.skip(f"DataHub GMS not reachable: {e}")

    # Ensure the sample dataset exists by ingesting it (idempotent-ish).
    repo_root = Path(__file__).resolve().parents[2]
    csv_path = repo_root / "sample-data-csv" / "categories.csv"
    assert csv_path.exists(), f"Expected sample file at {csv_path}"

    ingestion_cfg_file = tmp_path / "ingestion.json"
    ingestion_cfg_file.write_text(
        json.dumps([{"source_type": "csv", "source_path": str(csv_path), "infer_schema": True, "schema": {}}]),
        encoding="utf-8",
    )
    ingestion_controller.run_ingestion(str(ingestion_cfg_file))

    # Find the dataset urn.
    def _find_dataset_urn() -> str | None:
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

    dataset_urn = _wait_for(_find_dataset_urn, timeout_s=60.0, interval_s=3.0)
    if not dataset_urn:
        pytest.skip("Could not find dataset 'categories' after ingestion.")

    # Apply all enrichments for the dataset.
    stamp = _utc_stamp()
    desc = f"Integration test description {stamp}"
    doc_url = f"http://example.com/{stamp}"
    doc_desc = f"Integration test docs {stamp}"
    props = {"integration_test": stamp, "owner": "lumos"}
    tags = [f"integration_{stamp}", "gold tag"]

    enrich_cfg = {
        "data_type": "csv",
        "dataset_name": "categories",
        "enrichments": [
            {"enrichment_type": "description", "config": {"description": desc}},
            {"enrichment_type": "documentation", "config": {"doc_url": doc_url, "description": doc_desc}},
            {"enrichment_type": "properties", "config": {"custom_properties": props}},
            {"enrichment_type": "tags", "config": {"tags": tags}},
        ],
    }
    enrich_cfg_file = tmp_path / "enrich.json"
    enrich_cfg_file.write_text(json.dumps(enrich_cfg), encoding="utf-8")
    enrichment_controller.run_enrichment(str(enrich_cfg_file))

    # Verify via GraphQL.
    def _assert_enrichment_visible() -> bool:
        q = """
        query($urn: String!) {
          dataset(urn: $urn) {
            urn
            properties { description customProperties { key value } }
            institutionalMemory { elements { url description } }
            tags { tags { tag { urn } } }
          }
        }
        """
        data = _graphql(q, {"urn": dataset_urn})
        ds = data.get("dataset") or {}

        description = (ds.get("properties") or {}).get("description") or ""
        if desc not in description:
            return False

        custom_props_entries = (ds.get("properties") or {}).get("customProperties") or []
        custom_props = {e.get("key"): e.get("value") for e in custom_props_entries if isinstance(e, dict)}
        if custom_props.get("integration_test") != stamp:
            return False

        elements = ((ds.get("institutionalMemory") or {}).get("elements") or [])
        if not any((e.get("url") == doc_url and doc_desc in (e.get("description") or "")) for e in elements):
            return False

        tag_urns = [
            (t.get("tag") or {}).get("urn")
            for t in ((ds.get("tags") or {}).get("tags") or [])
            if isinstance(t, dict)
        ]
        # TagService sanitizes tag names (including timestamps).
        if f"urn:li:tag:{sanitize_entity_id('gold tag')}" not in tag_urns:
            return False
        if f"urn:li:tag:{sanitize_entity_id(f'integration_{stamp}')}" not in tag_urns:
            return False

        return True

    assert _wait_for(_assert_enrichment_visible, timeout_s=60.0, interval_s=3.0) is True


