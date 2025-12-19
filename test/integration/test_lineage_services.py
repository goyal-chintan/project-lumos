from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest
import requests
from datahub.emitter.mce_builder import make_dataset_urn

from core.common.config_manager import ConfigManager
from core.common.utils import sanitize_entity_id
from core.controllers import data_job_lineage_controller, ingestion_controller, lineage_controller


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


def _has_lineage(source_urn: str, *, direction: str, related_urn: str) -> bool:
    q = """
    query($urn: String!) {
      dataset(urn: $urn) {
        urn
        lineage(input: {direction: %s, start: 0, count: 50}) {
          relationships {
            type
            entity { urn }
          }
        }
      }
    }
    """ % (
        direction,
    )
    data = _graphql(q, {"urn": source_urn})
    rels = (((data.get("dataset") or {}).get("lineage") or {}).get("relationships") or [])
    return any((r.get("entity") or {}).get("urn") == related_urn for r in rels if isinstance(r, dict))


@pytest.mark.integration
def test_dataset_lineage_is_visible_in_graphql_ui(tmp_path) -> None:
    if not _enabled():
        pytest.skip("Set LUMOS_RUN_INTEGRATION=1 to run integration tests")

    # Preflight.
    try:
        _graphql("query { __typename }")
    except Exception as e:
        pytest.skip(f"DataHub GMS not reachable: {e}")

    env = _default_env()
    stamp = sanitize_entity_id(_utc_stamp())

    upstream_name = f"lineage_up_{stamp}"
    downstream_name = f"lineage_down_{stamp}"

    upstream_csv = tmp_path / f"{upstream_name}.csv"
    downstream_csv = tmp_path / f"{downstream_name}.csv"
    upstream_csv.write_text("id,val\n1,10\n", encoding="utf-8")
    downstream_csv.write_text("id,val\n2,20\n", encoding="utf-8")

    up_cfg = tmp_path / "up_ingest.json"
    up_cfg.write_text(json.dumps([{"source_type": "csv", "source_path": str(upstream_csv), "infer_schema": True, "schema": {}}]), encoding="utf-8")
    ingestion_controller.run_ingestion(str(up_cfg))

    down_cfg = tmp_path / "down_ingest.json"
    down_cfg.write_text(json.dumps([{"source_type": "csv", "source_path": str(downstream_csv), "infer_schema": True, "schema": {}}]), encoding="utf-8")
    ingestion_controller.run_ingestion(str(down_cfg))

    upstream_urn = make_dataset_urn("csv", upstream_name, env)
    downstream_urn = make_dataset_urn("csv", downstream_name, env)

    assert _wait_for(lambda: _dataset_exists(upstream_urn), timeout_s=60.0, interval_s=3.0) is True
    assert _wait_for(lambda: _dataset_exists(downstream_urn), timeout_s=60.0, interval_s=3.0) is True

    lineage_cfg = tmp_path / "lineage.json"
    lineage_cfg.write_text(
        json.dumps(
            [
                {
                    "lineage": {
                        "downstream": {"data_type": "csv", "dataset": downstream_name},
                        "upstreams": [{"data_type": "csv", "dataset": upstream_name}],
                    }
                }
            ]
        ),
        encoding="utf-8",
    )
    lineage_controller.run_add_lineage(str(lineage_cfg))

    assert _wait_for(lambda: _has_lineage(downstream_urn, direction="UPSTREAM", related_urn=upstream_urn), timeout_s=60.0, interval_s=3.0) is True
    assert _wait_for(lambda: _has_lineage(upstream_urn, direction="DOWNSTREAM", related_urn=downstream_urn), timeout_s=60.0, interval_s=3.0) is True


@pytest.mark.integration
def test_data_job_lineage_makes_output_depend_on_inputs(tmp_path) -> None:
    if not _enabled():
        pytest.skip("Set LUMOS_RUN_INTEGRATION=1 to run integration tests")

    # Preflight.
    try:
        _graphql("query { __typename }")
    except Exception as e:
        pytest.skip(f"DataHub GMS not reachable: {e}")

    env = _default_env()
    stamp = sanitize_entity_id(_utc_stamp())

    input_name = f"job_in_{stamp}"
    output_name = f"job_out_{stamp}"

    input_csv = tmp_path / f"{input_name}.csv"
    output_csv = tmp_path / f"{output_name}.csv"
    input_csv.write_text("id,val\n1,10\n", encoding="utf-8")
    output_csv.write_text("id,val\n2,20\n", encoding="utf-8")

    in_cfg = tmp_path / "in_ingest.json"
    in_cfg.write_text(json.dumps([{"source_type": "csv", "source_path": str(input_csv), "infer_schema": True, "schema": {}}]), encoding="utf-8")
    ingestion_controller.run_ingestion(str(in_cfg))

    out_cfg = tmp_path / "out_ingest.json"
    out_cfg.write_text(json.dumps([{"source_type": "csv", "source_path": str(output_csv), "infer_schema": True, "schema": {}}]), encoding="utf-8")
    ingestion_controller.run_ingestion(str(out_cfg))

    input_urn = make_dataset_urn("csv", input_name, env)
    output_urn = make_dataset_urn("csv", output_name, env)

    assert _wait_for(lambda: _dataset_exists(input_urn), timeout_s=60.0, interval_s=3.0) is True
    assert _wait_for(lambda: _dataset_exists(output_urn), timeout_s=60.0, interval_s=3.0) is True

    job_cfg = tmp_path / "job_lineage.json"
    job_cfg.write_text(
        json.dumps(
            [
                {
                    "data_job": {
                        "flow_id": f"flow_{stamp}",
                        "job_id": f"job_{stamp}",
                        "orchestrator": "airflow",
                        "inputs": [{"data_type": "csv", "dataset": input_name}],
                        "outputs": [{"data_type": "csv", "dataset": output_name}],
                    }
                }
            ]
        ),
        encoding="utf-8",
    )
    data_job_lineage_controller.run_add_data_job_lineage(str(job_cfg))

    assert _wait_for(lambda: _has_lineage(output_urn, direction="UPSTREAM", related_urn=input_urn), timeout_s=60.0, interval_s=3.0) is True


