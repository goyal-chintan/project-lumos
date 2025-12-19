from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

import pytest
import requests
from datahub.emitter.mce_builder import make_dataset_urn

from core.common.config_manager import ConfigManager
from core.common.utils import sanitize_entity_id
from core.controllers import ingestion_controller, ownership_controller


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


def _corp_user_exists(urn: str, expected_username: str) -> bool:
    q = """
    query($urn: String!) {
      corpUser(urn: $urn) { urn username }
    }
    """
    data = _graphql(q, {"urn": urn})
    cu = data.get("corpUser") or {}
    return cu.get("urn") == urn and cu.get("username") == expected_username


def _corp_group_exists(urn: str, expected_name: str) -> bool:
    q = """
    query($urn: String!) {
      corpGroup(urn: $urn) { urn name }
    }
    """
    data = _graphql(q, {"urn": urn})
    cg = data.get("corpGroup") or {}
    return cg.get("urn") == urn and cg.get("name") == expected_name


def _dataset_has_owner(dataset_urn: str, owner_urn: str) -> bool:
    q = """
    query($urn: String!) {
      dataset(urn: $urn) {
        ownership {
          owners {
            owner {
              __typename
              ... on CorpUser { urn }
              ... on CorpGroup { urn }
            }
          }
        }
      }
    }
    """
    data = _graphql(q, {"urn": dataset_urn})
    owners = (((data.get("dataset") or {}).get("ownership") or {}).get("owners") or [])
    for o in owners:
        owner = (o or {}).get("owner") or {}
        if owner.get("urn") == owner_urn:
            return True
    return False


@pytest.mark.integration
def test_create_user_group_and_assign_dataset_ownership(tmp_path) -> None:
    if not _enabled():
        pytest.skip("Set LUMOS_RUN_INTEGRATION=1 to run integration tests")

    # Preflight connectivity.
    try:
        _graphql("query { __typename }")
    except Exception as e:
        pytest.skip(f"DataHub GMS not reachable: {e}")

    env = _default_env()
    stamp = sanitize_entity_id(_utc_stamp())

    # 1) Create a dataset (so we can assign ownership).
    dataset_name = f"ownership_ds_{stamp}"
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

    # 2) Create a user.
    username = f"lumos_user_{stamp}"
    user_urn = f"urn:li:corpuser:{username}"

    users_yaml = tmp_path / "users.yaml"
    users_yaml.write_text(
        "\n".join(
            [
                "operation: create_users",
                "users:",
                f"  - username: {username}",
                f"    display_name: Lumos User {stamp}",
                f"    email: {username}@example.com",
                "",
            ]
        ),
        encoding="utf-8",
    )
    assert ownership_controller.run_create_users(str(users_yaml)) == 0
    assert _wait_for(lambda: _corp_user_exists(user_urn, username), timeout_s=60.0, interval_s=3.0) is True

    # 3) Create a group with that user as a member.
    group_name = f"lumos_group_{stamp}"
    group_urn = f"urn:li:corpGroup:{group_name}"

    groups_yaml = tmp_path / "groups.yaml"
    groups_yaml.write_text(
        "\n".join(
            [
                "operation: create_groups",
                "groups:",
                f"  - name: {group_name}",
                f"    display_name: Lumos Group {stamp}",
                "    members:",
                f"      - {username}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    assert ownership_controller.run_create_groups(str(groups_yaml)) == 0
    assert _wait_for(lambda: _corp_group_exists(group_urn, group_name), timeout_s=60.0, interval_s=3.0) is True

    # 4) Assign ownership of the dataset to the user and verify via GraphQL (UI-backed).
    assignments_yaml = tmp_path / "assignments.yaml"
    assignments_yaml.write_text(
        "\n".join(
            [
                "operation: assign_ownership",
                "assignments:",
                f"  - owner_name: {username}",
                "    owner_category: user",
                "    ownership_type: TECHNICAL_OWNER",
                "    entity:",
                "      datatype: csv",
                f"      dataset_name: {dataset_name}",
                f"      env: {env}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    assert ownership_controller.run_assign_ownership(str(assignments_yaml)) == 0
    assert _wait_for(lambda: _dataset_has_owner(dataset_urn, user_urn), timeout_s=60.0, interval_s=3.0) is True


