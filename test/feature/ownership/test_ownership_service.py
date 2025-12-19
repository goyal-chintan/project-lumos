from __future__ import annotations

import types

import pytest

from feature.ownership.ownership_service import OwnershipService


class _DummyCM:
    def get_global_config(self):
        return {"default_env": "DEV", "datahub": {"gms_host": "http://example:8080"}}


class _DummyPH:
    pass


def test_generate_user_and_group_urns_strip_and_validate(monkeypatch) -> None:
    # Avoid real emitter init
    monkeypatch.setattr(OwnershipService, "_initialize_emitter", lambda self: types.SimpleNamespace(emit=lambda *_a, **_k: None))
    svc = OwnershipService(_DummyPH(), _DummyCM())

    assert svc._generate_user_urn(" john ") == "urn:li:corpuser:john"
    assert svc._generate_group_urn(" team ") == "urn:li:corpGroup:team"
    with pytest.raises(ValueError):
        svc._generate_user_urn(" ")


def test_generate_owner_urn_switches_on_category(monkeypatch) -> None:
    monkeypatch.setattr(OwnershipService, "_initialize_emitter", lambda self: types.SimpleNamespace(emit=lambda *_a, **_k: None))
    svc = OwnershipService(_DummyPH(), _DummyCM())
    assert svc._generate_owner_urn("x", "group") == "urn:li:corpGroup:x"
    assert svc._generate_owner_urn("x", "user") == "urn:li:corpuser:x"


def test_generate_entity_urn_validates(monkeypatch) -> None:
    monkeypatch.setattr(OwnershipService, "_initialize_emitter", lambda self: types.SimpleNamespace(emit=lambda *_a, **_k: None))
    svc = OwnershipService(_DummyPH(), _DummyCM())

    with pytest.raises(ValueError):
        svc._generate_entity_urn({})
    with pytest.raises(ValueError):
        svc._generate_entity_urn({"datatype": "nope", "dataset_name": "d"})

    urn = svc._generate_entity_urn({"datatype": "csv", "dataset_name": "d", "env": "DEV"})
    assert urn.startswith("urn:li:dataset:(")
    assert "d" in urn


def test_validate_ownership_type_allows_builtin_and_custom(monkeypatch) -> None:
    monkeypatch.setattr(OwnershipService, "_initialize_emitter", lambda self: types.SimpleNamespace(emit=lambda *_a, **_k: None))
    svc = OwnershipService(_DummyPH(), _DummyCM())
    assert svc._validate_ownership_type("TECHNICAL_OWNER") is True
    assert svc._validate_ownership_type("LUMOS_OWNER") is True
    assert svc._validate_ownership_type("BADTYPE") is False


def test_create_user_emits_one_or_two_mcps(monkeypatch) -> None:
    emits = []

    class _DummyEmitter:
        def emit(self, mcp):
            emits.append(mcp)

    monkeypatch.setattr(OwnershipService, "_initialize_emitter", lambda self: _DummyEmitter())
    # Simplify DataHub classes + MCP wrapper
    monkeypatch.setattr("feature.ownership.ownership_service.CorpUserInfoClass", lambda **kw: types.SimpleNamespace(**kw))
    monkeypatch.setattr("feature.ownership.ownership_service.CorpUserEditableInfoClass", lambda **kw: types.SimpleNamespace(**kw))
    monkeypatch.setattr("feature.ownership.ownership_service.MetadataChangeProposalWrapper", lambda entityUrn, aspect: types.SimpleNamespace(entityUrn=entityUrn, aspect=aspect))

    svc = OwnershipService(_DummyPH(), _DummyCM())
    assert svc.create_user({"username": "u"}) is True
    assert len(emits) == 1

    emits.clear()
    assert svc.create_user({"username": "u", "about_me": "x"}) is True
    assert len(emits) == 2


def test_create_group_emits_one_or_two_mcps(monkeypatch) -> None:
    emits = []

    class _DummyEmitter:
        def emit(self, mcp):
            emits.append(mcp)

    monkeypatch.setattr(OwnershipService, "_initialize_emitter", lambda self: _DummyEmitter())
    monkeypatch.setattr("feature.ownership.ownership_service.CorpGroupInfoClass", lambda **kw: types.SimpleNamespace(**kw))
    monkeypatch.setattr("feature.ownership.ownership_service.CorpGroupEditableInfoClass", lambda **kw: types.SimpleNamespace(**kw))
    monkeypatch.setattr("feature.ownership.ownership_service.MetadataChangeProposalWrapper", lambda entityUrn, aspect: types.SimpleNamespace(entityUrn=entityUrn, aspect=aspect))

    svc = OwnershipService(_DummyPH(), _DummyCM())
    assert svc.create_group({"name": "g"}) is True
    assert len(emits) == 1

    emits.clear()
    assert svc.create_group({"name": "g", "description": "x"}) is True
    assert len(emits) == 2


def test_assign_ownership_handles_custom_type(monkeypatch) -> None:
    emits = []

    class _DummyEmitter:
        def emit(self, mcp):
            emits.append(mcp)

    monkeypatch.setattr(OwnershipService, "_initialize_emitter", lambda self: _DummyEmitter())
    monkeypatch.setattr("feature.ownership.ownership_service.MetadataChangeProposalWrapper", lambda entityUrn, aspect: types.SimpleNamespace(entityUrn=entityUrn, aspect=aspect))
    monkeypatch.setattr("feature.ownership.ownership_service.OwnerClass", lambda **kw: types.SimpleNamespace(**kw))
    monkeypatch.setattr("feature.ownership.ownership_service.OwnershipClass", lambda **kw: types.SimpleNamespace(**kw))
    # Provide minimal OwnershipTypeClass without the custom attribute
    monkeypatch.setattr("feature.ownership.ownership_service.OwnershipTypeClass", types.SimpleNamespace(TECHNICAL_OWNER="TECH"))

    svc = OwnershipService(_DummyPH(), _DummyCM())
    ok = svc.assign_ownership(
        {
            "owner_name": "u",
            "owner_category": "user",
            "ownership_type": "LUMOS_OWNER",
            "entity": {"datatype": "csv", "dataset_name": "d", "env": "DEV"},
        }
    )
    assert ok is True
    assert len(emits) == 1


def test_process_batch_operations_counts_results(monkeypatch) -> None:
    monkeypatch.setattr(OwnershipService, "_initialize_emitter", lambda self: types.SimpleNamespace(emit=lambda *_a, **_k: None))
    svc = OwnershipService(_DummyPH(), _DummyCM())

    monkeypatch.setattr("feature.ownership.ownership_service.load_json_file", lambda path, _t: [{"x": 1}, {"x": 2}])
    monkeypatch.setattr(svc, "create_user", lambda _d: True)
    monkeypatch.setattr(svc, "create_group", lambda _d: False)
    monkeypatch.setattr(svc, "assign_ownership", lambda _d: True)

    res = svc.process_batch_operations({"users_file": "u.json", "groups_file": "g.json", "assignments_file": "a.json"})
    assert res["users"]["total"] == 2
    assert res["users"]["successful"] == 2
    assert res["groups"]["failed"] == 2
    assert res["assignments"]["successful"] == 2



