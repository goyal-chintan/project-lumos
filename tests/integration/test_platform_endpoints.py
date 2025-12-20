from __future__ import annotations

import os

import pytest


def _enabled() -> bool:
    # Keep integration tests opt-in by default.
    return os.getenv("LUMOS_RUN_INTEGRATION") == "1"


def _ui_url() -> str:
    return (os.getenv("METADATA_UI") or os.getenv("DATAHUB_UI") or "http://localhost:9002").rstrip("/")


def _gms_url() -> str:
    return (os.getenv("METADATA_GMS") or os.getenv("DATAHUB_GMS") or "http://localhost:8080").rstrip("/")


@pytest.mark.integration
def test_ui_is_reachable() -> None:
    if not _enabled():
        pytest.skip("Set LUMOS_RUN_INTEGRATION=1 to run integration tests")

    try:
        import requests
    except Exception as e:
        pytest.skip(f"HTTP client not available in this environment: {e}")

    resp = requests.get(_ui_url() + "/", timeout=5, allow_redirects=True)
    assert resp.status_code == 200


@pytest.mark.integration
def test_gms_graphql_is_reachable() -> None:
    if not _enabled():
        pytest.skip("Set LUMOS_RUN_INTEGRATION=1 to run integration tests")

    try:
        import requests
    except Exception as e:
        pytest.skip(f"HTTP client not available in this environment: {e}")

    resp = requests.post(
        _gms_url() + "/api/graphql",
        json={"query": "query { __typename }"},
        timeout=5,
    )

    assert resp.status_code == 200
    data = resp.json()
    assert "data" in data


