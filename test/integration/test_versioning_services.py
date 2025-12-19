from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone

import pytest
import requests
from datahub.emitter.mce_builder import make_dataset_urn

from core.common.config_manager import ConfigManager
from core.common.utils import sanitize_entity_id
from core.controllers import ingestion_controller
from feature.versioning.dataset_scanner import DatasetScanner
from feature.versioning.version_service import VersionManager


_ISO_UTC_MS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


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
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _default_env() -> str:
    return ConfigManager().get_global_config().get("default_env", "DEV")


def _dataset_exists(urn: str) -> bool:
    q = """
    query($urn: String!) {
      dataset(urn: $urn) { urn }
    }
    """
    data = _graphql(q, {"urn": urn})
    return bool(data.get("dataset"))


def _get_dataset_properties(urn: str) -> tuple[str | None, dict[str, str]]:
    q = """
    query($urn: String!) {
      dataset(urn: $urn) {
        properties {
          description
          customProperties { key value }
        }
      }
    }
    """
    data = _graphql(q, {"urn": urn})
    props = ((data.get("dataset") or {}).get("properties") or {})
    desc = props.get("description")
    entries = props.get("customProperties") or []
    custom: dict[str, str] = {}
    for e in entries:
        if isinstance(e, dict) and e.get("key") is not None:
            custom[str(e["key"])] = str(e.get("value") or "")
    return desc, custom


@pytest.mark.integration
def test_versioning_updates_custom_properties_without_wiping_description(tmp_path) -> None:
    if not _enabled():
        pytest.skip("Set LUMOS_RUN_INTEGRATION=1 to run integration tests")

    # Preflight connectivity.
    try:
        _graphql("query { __typename }")
    except Exception as e:
        pytest.skip(f"DataHub GMS not reachable: {e}")

    env = _default_env()
    stamp = sanitize_entity_id(_utc_stamp())

    dataset_name = f"versioning_ds_{stamp}"
    csv_path = tmp_path / f"{dataset_name}.csv"
    csv_path.write_text("id,val\n1,10\n", encoding="utf-8")

    ingest_cfg = tmp_path / "ingest.json"
    ingest_cfg.write_text(
        json.dumps(
            [
                {
                    "source_type": "csv",
                    "source_path": str(csv_path),
                    "infer_schema": True,
                    "schema": {},
                }
            ]
        ),
        encoding="utf-8",
    )
    ingestion_controller.run_ingestion(str(ingest_cfg))

    dataset_urn = make_dataset_urn("csv", dataset_name, env)
    assert _wait_for(lambda: _dataset_exists(dataset_urn), timeout_s=60.0, interval_s=3.0) is True

    # Sanity: dataset scanner should be able to discover it (part of @versioning).
    scanner = DatasetScanner(ConfigManager())
    assert _wait_for(
        lambda: any(ds.urn == dataset_urn for ds in scanner.scan_all_datasets()),
        timeout_s=60.0,
        interval_s=3.0,
    ) is True

    before_desc, before_custom = _get_dataset_properties(dataset_urn)
    assert before_desc  # ingestion should set a description

    vm = VersionManager(ConfigManager())
    result = vm.update_dataset_version(dataset_urn, dataset_name)
    assert result.success is True

    def _updated_visible() -> bool:
        after_desc, after_custom = _get_dataset_properties(dataset_urn)
        if after_desc != before_desc:
            return False
        cloud_version = after_custom.get("cloud_version")
        last_updated = after_custom.get("last_updated")
        if not cloud_version or not last_updated:
            return False
        if not _ISO_UTC_MS.match(last_updated):
            return False
        mapping = json.loads(cloud_version)
        if not isinstance(mapping, dict):
            return False
        # Should include the version just written by VersionManager.
        return set(result.new_mapping.keys()).issubset(set(mapping.keys()))

    assert _wait_for(_updated_visible, timeout_s=60.0, interval_s=3.0) is True


