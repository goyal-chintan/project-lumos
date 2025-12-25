"""Unit tests for feature.ownership.ownership_service."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest


def _make_service(monkeypatch):
    from feature.ownership.ownership_service import OwnershipService

    # Make sure we never create a real REST emitter
    dummy_emitter = MagicMock()

    monkeypatch.setattr(OwnershipService, "_initialize_emitter", lambda self: dummy_emitter)

    platform_handler = MagicMock()
    config_manager = MagicMock()
    config_manager.get_global_config.return_value = {
        "default_env": "DEV",
        "datahub": {"gms_host": "http://localhost:8080"},
    }

    service = OwnershipService(platform_handler, config_manager)
    return service, dummy_emitter


def test_generate_user_urn_valid_and_invalid(monkeypatch) -> None:
    service, _ = _make_service(monkeypatch)

    assert service._generate_user_urn("john") == "urn:li:corpuser:john"

    with pytest.raises(ValueError):
        service._generate_user_urn(" ")


def test_generate_group_urn_valid_and_invalid(monkeypatch) -> None:
    service, _ = _make_service(monkeypatch)

    assert service._generate_group_urn("data") == "urn:li:corpGroup:data"

    with pytest.raises(ValueError):
        service._generate_group_urn("")


def test_validate_ownership_type_allows_builtins_and_custom(monkeypatch) -> None:
    service, _ = _make_service(monkeypatch)

    assert service._validate_ownership_type("TECHNICAL_OWNER") is True
    assert service._validate_ownership_type("LUMOS_OWNER") is True
    assert service._validate_ownership_type("SOME_CUSTOM_TYPE") is True
    assert service._validate_ownership_type("invalid-type") is False


def test_create_user_emits_user_info(monkeypatch) -> None:
    service, emitter = _make_service(monkeypatch)

    user_data = {"username": "alice", "display_name": "Alice"}

    assert service.create_user(user_data) is True
    assert emitter.emit.call_count >= 1


def test_create_user_returns_false_on_validation_error(monkeypatch) -> None:
    service, emitter = _make_service(monkeypatch)

    assert service.create_user({}) is False
    emitter.emit.assert_not_called()


def test_create_group_emits_group_info(monkeypatch) -> None:
    service, emitter = _make_service(monkeypatch)

    group_data = {"name": "team-data", "description": "Team"}

    assert service.create_group(group_data) is True
    assert emitter.emit.call_count >= 1


def test_assign_ownership_invalid_type_returns_false(monkeypatch) -> None:
    service, emitter = _make_service(monkeypatch)

    assignment = {
        "owner_name": "alice",
        "entity": {"datatype": "csv", "dataset_name": "ds1", "env": "DEV"},
        "ownership_type": "invalid-type",
    }

    assert service.assign_ownership(assignment) is False
    emitter.emit.assert_not_called()


def test_assign_ownership_success_emits(monkeypatch) -> None:
    service, emitter = _make_service(monkeypatch)

    assignment = {
        "owner_name": "alice",
        "entity": {"datatype": "csv", "dataset_name": "ds1", "env": "DEV"},
        "ownership_type": "TECHNICAL_OWNER",
    }

    assert service.assign_ownership(assignment) is True
    emitter.emit.assert_called_once()


def test_process_batch_operations_counts_results(monkeypatch) -> None:
    service, _ = _make_service(monkeypatch)

    # Fake data returned by load_json_file
    import feature.ownership.ownership_service as mod

    monkeypatch.setattr(mod, "load_json_file", lambda _path, kind: (
        [{"username": "u1"}] if kind == "users" else
        [{"name": "g1"}] if kind == "groups" else
        [{"owner_name": "u1", "entity": {"datatype": "csv", "dataset_name": "d1"}}]
    ))

    monkeypatch.setattr(service, "create_user", lambda _d: True)
    monkeypatch.setattr(service, "create_group", lambda _d: False)
    monkeypatch.setattr(service, "assign_ownership", lambda _d: True)

    results = service.process_batch_operations({
        "users_file": "/tmp/users.json",
        "groups_file": "/tmp/groups.json",
        "assignments_file": "/tmp/assignments.json",
    })

    assert results["users"]["total"] == 1
    assert results["users"]["successful"] == 1
    assert results["groups"]["total"] == 1
    assert results["groups"]["failed"] == 1
    assert results["assignments"]["total"] == 1
    assert results["assignments"]["successful"] == 1
