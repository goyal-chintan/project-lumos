from __future__ import annotations

from feature.profiling.dataset_stats_service import DatasetStatsService


def test_profile_prints(capsys) -> None:
    svc = DatasetStatsService()
    svc.profile("urn:li:dataset:(x)")
    out = capsys.readouterr().out
    assert "Profiling stats for" in out



