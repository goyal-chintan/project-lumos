from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path

import pytest
import requests

from feature.extraction.schema_extractor_service import SchemaExtractorService
from feature.extraction.properties_extractor_service import PropertiesExtractorService


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


class _DummyCM:
    def get_global_config(self):
        return {"datahub": {"gms_server": _gms_url()}}


@pytest.mark.integration
def test_schema_and_properties_extraction_match_graphql(tmp_path) -> None:
    if not _enabled():
        pytest.skip("Set LUMOS_RUN_INTEGRATION=1 to run integration tests")

    # Preflight
    try:
        _graphql("query { __typename }")
    except Exception as e:
        pytest.skip(f"DataHub GMS not reachable: {e}")

    # Find a known dataset (created by ingestion demos/tests).
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

    dataset_urn = _wait_for(_find_categories_urn, timeout_s=60.0, interval_s=3.0)
    if not dataset_urn:
        pytest.skip("Could not find dataset 'categories' in DataHub. Run ingestion integration first.")

    # Pull the UI-visible truth from GraphQL (same backing store as UI).
    q = """
    query($urn: String!) {
      dataset(urn: $urn) {
        urn
        name
        properties { customProperties { key value } }
        schemaMetadata { fields { fieldPath } }
      }
    }
    """
    ds = (_graphql(q, {"urn": dataset_urn}).get("dataset") or {})
    gql_fields = {f["fieldPath"] for f in (ds.get("schemaMetadata") or {}).get("fields") or []}
    gql_custom_props = {
        e.get("key"): e.get("value")
        for e in ((ds.get("properties") or {}).get("customProperties") or [])
        if isinstance(e, dict)
    }

    # Schema extraction
    schema_out = tmp_path / "schema.json"
    schema_svc = SchemaExtractorService(_DummyCM())
    r = schema_svc.extract({"extraction_type": "schema", "output_path": str(schema_out), "datasets": [dataset_urn]})
    assert r.success is True
    data = json.loads(schema_out.read_text(encoding="utf-8"))
    assert _ISO_UTC_MS.match(data["extraction_metadata"]["extracted_at"])

    schemas = data.get("schemas") or []
    assert schemas, "Expected at least 1 schema entry"
    entry = next((s for s in schemas if s.get("dataset_urn") == dataset_urn), None)
    assert entry is not None
    extracted_fields = {f["name"] for f in (entry.get("fields") or [])}

    # Basic sanity: should include the CSV sample columns.
    assert {"id", "name", "description"}.issubset(extracted_fields)
    # And those should exist in GraphQL schema.
    assert {"id", "name", "description"}.issubset(gql_fields)

    # Properties extraction
    props_out = tmp_path / "props.json"
    props_svc = PropertiesExtractorService(_DummyCM())
    r2 = props_svc.extract({"extraction_type": "properties", "output_path": str(props_out), "datasets": [dataset_urn]})
    assert r2.success is True
    pdata = json.loads(props_out.read_text(encoding="utf-8"))
    assert _ISO_UTC_MS.match(pdata["extraction_metadata"]["extracted_at"])

    dps = pdata.get("dataset_properties") or []
    dp = next((d for d in dps if d.get("dataset_urn") == dataset_urn), None)
    assert dp is not None
    extracted_custom_props = dp.get("custom_properties") or {}

    # Ensure we don't invent properties: every GraphQL custom property should be present in extraction output.
    for k, v in gql_custom_props.items():
        assert extracted_custom_props.get(k) == v


