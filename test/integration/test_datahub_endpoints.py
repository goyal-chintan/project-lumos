from __future__ import annotations

import os

import pytest
import requests


def _enabled() -> bool:
    # Keep unit tests offline/fast by default.
    return os.getenv("LUMOS_RUN_INTEGRATION") == "1"


@pytest.mark.integration
def test_datahub_ui_is_reachable() -> None:
    if not _enabled():
        pytest.skip("Set LUMOS_RUN_INTEGRATION=1 to run integration tests")

    ui = os.getenv("DATAHUB_UI", "http://localhost:9002").rstrip("/")
    resp = requests.get(ui + "/", timeout=5, allow_redirects=True)

    assert resp.status_code == 200
    assert "datahub" in resp.text.lower()


@pytest.mark.integration
def test_datahub_gms_graphql_is_reachable() -> None:
    if not _enabled():
        pytest.skip("Set LUMOS_RUN_INTEGRATION=1 to run integration tests")

    gms = os.getenv("DATAHUB_GMS", "http://localhost:8080").rstrip("/")
    resp = requests.post(
        gms + "/api/graphql",
        json={"query": "query { __typename }"},
        timeout=5,
    )

    assert resp.status_code == 200
    data = resp.json()
    assert "data" in data


