import pytest

from core.controllers import ownership_controller


@pytest.mark.unit
class TestValidationHelpers:
    def test_validate_users_config_rejects_non_dict(self) -> None:
        with pytest.raises(ValueError, match="must be a dictionary"):
            ownership_controller._validate_users_config([])  # type: ignore[arg-type]

    def test_validate_users_config_requires_operation_and_users(self) -> None:
        with pytest.raises(ValueError, match="operation 'create_users'"):
            ownership_controller._validate_users_config({"operation": "x", "users": []})

        with pytest.raises(ValueError, match="'users' list"):
            ownership_controller._validate_users_config({"operation": "create_users"})

    def test_validate_groups_config_requires_operation_and_groups(self) -> None:
        with pytest.raises(ValueError, match="operation 'create_groups'"):
            ownership_controller._validate_groups_config({"operation": "x", "groups": []})

        with pytest.raises(ValueError, match="'groups' list"):
            ownership_controller._validate_groups_config({"operation": "create_groups"})

    def test_validate_assignments_config_requires_operation_and_assignments(self) -> None:
        with pytest.raises(ValueError, match="operation 'assign_ownership'"):
            ownership_controller._validate_assignments_config({"operation": "x", "assignments": []})

        with pytest.raises(ValueError, match="'assignments' list"):
            ownership_controller._validate_assignments_config({"operation": "assign_ownership"})


@pytest.mark.unit
class TestRunOwnershipOperations:
    def test_run_create_users_returns_0_when_all_succeed(self, monkeypatch) -> None:
        class _FakeConfigManager:
            def load_config(self, path: str):
                assert path == "users.yaml"
                return {"operation": "create_users", "users": [{"username": "a"}, {"username": "b"}]}

            def get_global_config(self):
                return {"datahub": {"gms_server": "http://localhost:8080"}}

        class _FakeOwnershipService:
            def __init__(self, platform_handler, config_manager):
                assert platform_handler == {"p": "datahub"}

            def create_user(self, user_data: dict) -> bool:
                return True

        monkeypatch.setattr(ownership_controller, "ConfigManager", _FakeConfigManager)
        monkeypatch.setattr(
            ownership_controller.PlatformFactory,
            "get_instance",
            lambda platform_name, config_manager: {"p": platform_name},
        )
        monkeypatch.setattr(ownership_controller, "OwnershipService", _FakeOwnershipService)

        assert ownership_controller.run_create_users("users.yaml") == 0

    def test_run_create_users_returns_1_when_any_fail(self, monkeypatch) -> None:
        class _FakeConfigManager:
            def load_config(self, path: str):
                return {"operation": "create_users", "users": [{"username": "a"}, {"username": "b"}]}

            def get_global_config(self):
                return {"datahub": {"gms_server": "http://localhost:8080"}}

        class _FakeOwnershipService:
            def __init__(self, platform_handler, config_manager):
                pass

            def create_user(self, user_data: dict) -> bool:
                return user_data.get("username") != "b"

        monkeypatch.setattr(ownership_controller, "ConfigManager", _FakeConfigManager)
        monkeypatch.setattr(
            ownership_controller.PlatformFactory,
            "get_instance",
            lambda platform_name, config_manager: {"p": platform_name},
        )
        monkeypatch.setattr(ownership_controller, "OwnershipService", _FakeOwnershipService)

        assert ownership_controller.run_create_users("users.yaml") == 1

    def test_run_create_groups_returns_0_when_all_succeed(self, monkeypatch) -> None:
        class _FakeConfigManager:
            def load_config(self, path: str):
                return {"operation": "create_groups", "groups": [{"name": "g"}]}

            def get_global_config(self):
                return {"datahub": {"gms_server": "http://localhost:8080"}}

        class _FakeOwnershipService:
            def __init__(self, platform_handler, config_manager):
                pass

            def create_group(self, group_data: dict) -> bool:
                return True

        monkeypatch.setattr(ownership_controller, "ConfigManager", _FakeConfigManager)
        monkeypatch.setattr(
            ownership_controller.PlatformFactory,
            "get_instance",
            lambda platform_name, config_manager: {"p": platform_name},
        )
        monkeypatch.setattr(ownership_controller, "OwnershipService", _FakeOwnershipService)

        assert ownership_controller.run_create_groups("groups.yaml") == 0

    def test_run_assign_ownership_returns_0_when_all_succeed(self, monkeypatch) -> None:
        class _FakeConfigManager:
            def load_config(self, path: str):
                return {
                    "operation": "assign_ownership",
                    "assignments": [{"owner_name": "o", "entity": {"dataset_name": "d"}}],
                }

            def get_global_config(self):
                return {"datahub": {"gms_server": "http://localhost:8080"}}

        class _FakeOwnershipService:
            def __init__(self, platform_handler, config_manager):
                pass

            def assign_ownership(self, assignment_data: dict) -> bool:
                return True

        monkeypatch.setattr(ownership_controller, "ConfigManager", _FakeConfigManager)
        monkeypatch.setattr(
            ownership_controller.PlatformFactory,
            "get_instance",
            lambda platform_name, config_manager: {"p": platform_name},
        )
        monkeypatch.setattr(ownership_controller, "OwnershipService", _FakeOwnershipService)

        assert ownership_controller.run_assign_ownership("assign.yaml") == 0

    def test_missing_platform_config_returns_1(self, monkeypatch) -> None:
        class _FakeConfigManager:
            def load_config(self, path: str):
                return {"operation": "create_users", "users": [{"username": "a"}]}

            def get_global_config(self):
                return {}

        monkeypatch.setattr(ownership_controller, "ConfigManager", _FakeConfigManager)

        assert ownership_controller.run_create_users("users.yaml") == 1
