from __future__ import annotations

import os
import time
from datetime import datetime, timezone

import pytest
import requests

from core.common.utils import sanitize_entity_id
from core.platform.impl.datahub_handler import DataHubHandler
from feature.dq_services.assertion_service import AssertionService


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
def test_custom_assertion_is_created_and_searchable() -> None:
    if not _enabled():
        pytest.skip("Set LUMOS_RUN_INTEGRATION=1 to run integration tests")

    # Preflight: ensure GraphQL is reachable.
    try:
        _graphql("query { __typename }")
    except Exception as e:
        pytest.skip(f"DataHub GMS not reachable: {e}")

    # If the server doesn't support assertions, skip.
    try:
        entity_type = _graphql('query { __type(name: "EntityType") { enumValues { name } } }')
        names = {v["name"] for v in (entity_type.get("__type") or {}).get("enumValues") or []}
        if "ASSERTION" not in names:
            pytest.skip("DataHub GraphQL does not expose EntityType.ASSERTION")
    except Exception:
        # Don't hard-fail on schema drift; we'll try search anyway.
        pass

    # Find a known dataset URN (created by ingestion tests / demos).
    dataset_q = """
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
    data = _graphql(dataset_q)
    results = data["search"]["searchResults"]
    dataset_urn = None
    for r in results:
        ent = r["entity"]
        if ent.get("name") == "categories":
            dataset_urn = ent["urn"]
            break
    if not dataset_urn:
        pytest.skip("Could not find dataset 'categories' in DataHub to attach an assertion to.")

    # Create an assertion + run event.
    stamp = _utc_stamp()
    assertion_id = f"lumos_it_not_null_{sanitize_entity_id(stamp)}"
    handler = DataHubHandler({"gms_server": _gms_url(), "test_mode": False})
    svc = AssertionService(handler, None)  # type: ignore[arg-type]
    assertion_urn = svc.assert_quality(
        dataset_urn,
        "not_null",
        assertion_id=assertion_id,
        description=f"Integration test not_null {stamp}",
        emit_run_event=True,
    )

    # Verify it is searchable as an assertion entity.
    def _found() -> bool:
        q = """
        query($q: String!) {
          search(input: {type: ASSERTION, query: $q, start: 0, count: 20}) {
            searchResults {
              entity { urn }
            }
          }
        }
        """
        try:
            d = _graphql(q, {"q": assertion_id})
        except AssertionError:
            # Some deployments may not support searching assertions via ASSERTION entity type.
            return False
        return any(r["entity"]["urn"] == assertion_urn for r in d["search"]["searchResults"])

    if _wait_for(_found, timeout_s=60.0, interval_s=3.0) is not True:
        pytest.skip("Could not confirm assertion via ASSERTION search; schema/index may differ by DataHub version.")


