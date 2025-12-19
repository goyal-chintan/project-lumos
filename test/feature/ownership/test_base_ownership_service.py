from __future__ import annotations

import pytest

from feature.ownership.base_ownership_service import BaseOwnershipService


class _DummyCM:
    def get_global_config(self):
        return {"default_env": "DEV"}


class _DummyPH:
    pass


class _Svc(BaseOwnershipService):
    def create_user(self, user_data):
        return True

    def create_group(self, group_data):
        return True

    def assign_ownership(self, assignment_data):
        return True

    def process_batch_operations(self, config):
        return {}


def test_validate_user_data_requires_username() -> None:
    svc = _Svc(_DummyPH(), _DummyCM())
    assert svc.validate_user_data({}) == ["Missing required field: username"]


def test_validate_group_data_requires_name() -> None:
    svc = _Svc(_DummyPH(), _DummyCM())
    assert svc.validate_group_data({}) == ["Missing required field: name"]


def test_validate_assignment_data_requires_owner_and_entity() -> None:
    svc = _Svc(_DummyPH(), _DummyCM())
    errs = svc.validate_assignment_data({})
    assert "Missing required field: owner_name" in errs
    assert "Missing required field: entity" in errs


def test_validate_assignment_data_requires_entity_fields() -> None:
    svc = _Svc(_DummyPH(), _DummyCM())
    # Empty entity dict is treated as missing `entity` (falsy), so entity field checks don't run.
    errs = svc.validate_assignment_data({"owner_name": "a", "entity": {}})
    assert "Missing required field: entity" in errs

    errs2 = svc.validate_assignment_data({"owner_name": "a", "entity": {"datatype": "csv"}})
    assert "Missing required entity field: dataset_name" in errs2

    errs3 = svc.validate_assignment_data({"owner_name": "a", "entity": {"dataset_name": "d"}})
    assert "Missing required entity field: datatype" in errs3


