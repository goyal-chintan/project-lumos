from __future__ import annotations

from feature.rbac.policy_service import PolicyService


def test_apply_policy_prints(capsys) -> None:
    svc = PolicyService()
    svc.apply_policy({"k": "v"})
    out = capsys.readouterr().out
    assert "Applying policy" in out



