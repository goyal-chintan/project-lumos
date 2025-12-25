"""Unit tests for feature.rbac.policy_service."""
from __future__ import annotations


def test_apply_policy_prints_message(capsys) -> None:
    from feature.rbac.policy_service import PolicyService

    service = PolicyService()
    service.apply_policy({"name": "test"})

    out = capsys.readouterr().out
    assert "Applying policy" in out
