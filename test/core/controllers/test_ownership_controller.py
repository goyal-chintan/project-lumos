from __future__ import annotations

import pytest

from core.controllers import ownership_controller as oc


def test_validate_users_config_happy_path() -> None:
    oc._validate_users_config({"operation": "create_users", "users": [{"username": "a"}]})


def test_validate_users_config_rejects_bad_shape() -> None:
    with pytest.raises(ValueError):
        oc._validate_users_config(["not-dict"])  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        oc._validate_users_config({"operation": "x", "users": []})
    with pytest.raises(ValueError):
        oc._validate_users_config({"operation": "create_users"})


def test_validate_groups_config_happy_path() -> None:
    oc._validate_groups_config({"operation": "create_groups", "groups": [{"name": "g"}]})


def test_validate_assignments_config_happy_path() -> None:
    oc._validate_assignments_config(
        {"operation": "assign_ownership", "assignments": [{"owner_name": "a", "entity": {}}]}
    )


def test_run_create_users_returns_0_on_all_success(monkeypatch) -> None:
    users_cfg = {"operation": "create_users", "users": [{"username": "u1"}, {"username": "u2"}]}

    class _DummyCM:
        def load_config(self, _path):
            return users_cfg

        def get_global_config(self):
            return {"datahub": {"gms_server": "http://localhost:8080"}}

    monkeypatch.setattr(oc, "ConfigManager", _DummyCM)
    monkeypatch.setattr(oc.PlatformFactory, "get_instance", lambda *_a, **_k: object())

    calls = []

    class _DummyOwnershipService:
        def __init__(self, _ph, _cm):
            pass

        def create_user(self, user_data):
            calls.append(user_data["username"])
            return True

    monkeypatch.setattr(oc, "OwnershipService", _DummyOwnershipService)

    rc = oc.run_create_users("ignored.yaml")
    assert rc == 0
    assert calls == ["u1", "u2"]


def test_run_create_users_returns_1_if_any_fail(monkeypatch) -> None:
    users_cfg = {"operation": "create_users", "users": [{"username": "u1"}, {"username": "u2"}]}

    class _DummyCM:
        def load_config(self, _path):
            return users_cfg

        def get_global_config(self):
            return {"datahub": {"gms_server": "http://localhost:8080"}}

    monkeypatch.setattr(oc, "ConfigManager", _DummyCM)
    monkeypatch.setattr(oc.PlatformFactory, "get_instance", lambda *_a, **_k: object())

    class _DummyOwnershipService:
        def __init__(self, _ph, _cm):
            pass

        def create_user(self, user_data):
            return user_data["username"] != "u2"

    monkeypatch.setattr(oc, "OwnershipService", _DummyOwnershipService)

    rc = oc.run_create_users("ignored.yaml")
    assert rc == 1


def test_run_create_groups_returns_0_on_all_success(monkeypatch) -> None:
    groups_cfg = {"operation": "create_groups", "groups": [{"name": "g1"}, {"name": "g2"}]}

    class _DummyCM:
        def load_config(self, _path):
            return groups_cfg

        def get_global_config(self):
            return {"datahub": {"gms_server": "http://localhost:8080"}}

    monkeypatch.setattr(oc, "ConfigManager", _DummyCM)
    monkeypatch.setattr(oc.PlatformFactory, "get_instance", lambda *_a, **_k: object())

    calls = []

    class _DummyOwnershipService:
        def __init__(self, _ph, _cm):
            pass

        def create_group(self, group_data):
            calls.append(group_data["name"])
            return True

    monkeypatch.setattr(oc, "OwnershipService", _DummyOwnershipService)

    rc = oc.run_create_groups("ignored.yaml")
    assert rc == 0
    assert calls == ["g1", "g2"]


def test_run_create_groups_returns_1_if_any_fail(monkeypatch) -> None:
    groups_cfg = {"operation": "create_groups", "groups": [{"name": "g1"}, {"name": "g2"}]}

    class _DummyCM:
        def load_config(self, _path):
            return groups_cfg

        def get_global_config(self):
            return {"datahub": {"gms_server": "http://localhost:8080"}}

    monkeypatch.setattr(oc, "ConfigManager", _DummyCM)
    monkeypatch.setattr(oc.PlatformFactory, "get_instance", lambda *_a, **_k: object())

    class _DummyOwnershipService:
        def __init__(self, _ph, _cm):
            pass

        def create_group(self, group_data):
            return group_data["name"] != "g2"

    monkeypatch.setattr(oc, "OwnershipService", _DummyOwnershipService)
    assert oc.run_create_groups("ignored.yaml") == 1


def test_run_assign_ownership_returns_0_on_all_success(monkeypatch) -> None:
    assignments_cfg = {
        "operation": "assign_ownership",
        "assignments": [
            {"owner_name": "o1", "entity": {"dataset_name": "d1"}},
            {"owner_name": "o2", "entity": {"dataset_name": "d2"}},
        ],
    }

    class _DummyCM:
        def load_config(self, _path):
            return assignments_cfg

        def get_global_config(self):
            return {"datahub": {"gms_server": "http://localhost:8080"}}

    monkeypatch.setattr(oc, "ConfigManager", _DummyCM)
    monkeypatch.setattr(oc.PlatformFactory, "get_instance", lambda *_a, **_k: object())

    calls = []

    class _DummyOwnershipService:
        def __init__(self, _ph, _cm):
            pass

        def assign_ownership(self, assignment_data):
            calls.append(assignment_data["owner_name"])
            return True

    monkeypatch.setattr(oc, "OwnershipService", _DummyOwnershipService)

    rc = oc.run_assign_ownership("ignored.yaml")
    assert rc == 0
    assert calls == ["o1", "o2"]


def test_run_assign_ownership_returns_1_if_any_fail(monkeypatch) -> None:
    assignments_cfg = {
        "operation": "assign_ownership",
        "assignments": [
            {"owner_name": "o1", "entity": {"dataset_name": "d1"}},
            {"owner_name": "o2", "entity": {"dataset_name": "d2"}},
        ],
    }

    class _DummyCM:
        def load_config(self, _path):
            return assignments_cfg

        def get_global_config(self):
            return {"datahub": {"gms_server": "http://localhost:8080"}}

    monkeypatch.setattr(oc, "ConfigManager", _DummyCM)
    monkeypatch.setattr(oc.PlatformFactory, "get_instance", lambda *_a, **_k: object())

    class _DummyOwnershipService:
        def __init__(self, _ph, _cm):
            pass

        def assign_ownership(self, assignment_data):
            return assignment_data["owner_name"] != "o2"

    monkeypatch.setattr(oc, "OwnershipService", _DummyOwnershipService)
    assert oc.run_assign_ownership("ignored.yaml") == 1


def test_run_create_users_returns_1_when_config_load_fails(monkeypatch) -> None:
    class _DummyCM:
        def load_config(self, _path):
            return {}

        def get_global_config(self):
            return {"datahub": {"gms_server": "http://localhost:8080"}}

    monkeypatch.setattr(oc, "ConfigManager", _DummyCM)
    assert oc.run_create_users("ignored.yaml") == 1


def test_run_create_users_returns_1_when_platform_config_missing(monkeypatch) -> None:
    users_cfg = {"operation": "create_users", "users": [{"username": "u1"}]}

    class _DummyCM:
        def load_config(self, _path):
            return users_cfg

        def get_global_config(self):
            return {}  # missing datahub config

    monkeypatch.setattr(oc, "ConfigManager", _DummyCM)
    assert oc.run_create_users("ignored.yaml") == 1


def test_run_create_users_returns_1_when_service_raises(monkeypatch) -> None:
    users_cfg = {"operation": "create_users", "users": [{"username": "u1"}]}

    class _DummyCM:
        def load_config(self, _path):
            return users_cfg

        def get_global_config(self):
            return {"datahub": {"gms_server": "http://localhost:8080"}}

    monkeypatch.setattr(oc, "ConfigManager", _DummyCM)
    monkeypatch.setattr(oc.PlatformFactory, "get_instance", lambda *_a, **_k: object())

    class _DummyOwnershipService:
        def __init__(self, _ph, _cm):
            pass

        def create_user(self, _user_data):
            raise RuntimeError("boom")

    monkeypatch.setattr(oc, "OwnershipService", _DummyOwnershipService)
    assert oc.run_create_users("ignored.yaml") == 1


