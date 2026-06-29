import asyncio

from backend.analytics import build_dashboard_data, parse_sources, parse_summary, parse_timeseries
from backend.metrika_client import MockMetrikaClient


def test_parse_summary() -> None:
    summary = parse_summary({"totals": [10, 8, 24, 12.345, 2.4, 75.123]})

    assert summary.visits == 10
    assert summary.users == 8
    assert summary.pageviews == 24
    assert summary.bounce_rate == 12.35
    assert summary.page_depth == 2.4
    assert summary.avg_visit_duration_seconds == 75.12


def test_parse_timeseries() -> None:
    points = parse_timeseries(
        {
            "time_intervals": [["2026-06-28", "2026-06-28"], ["2026-06-29", "2026-06-29"]],
            "data": [{"metrics": [[10, 12], [8, 9], [30, 33]]}],
        }
    )

    assert len(points) == 2
    assert points[1].date == "2026-06-29"
    assert points[1].visits == 12
    assert points[1].users == 9
    assert points[1].pageviews == 33


def test_parse_sources() -> None:
    sources = parse_sources(
        {
            "data": [
                {"dimensions": [{"name": "organic"}], "metrics": [20, 15]},
                {"dimensions": [{"name": "direct"}], "metrics": [10, 8]},
            ]
        }
    )

    assert [item.source for item in sources] == ["organic", "direct"]
    assert sources[0].visits == 20


def test_build_dashboard_data_with_mock() -> None:
    dashboard = asyncio.run(build_dashboard_data(MockMetrikaClient(), "2026-06-16", "2026-06-29"))

    assert dashboard.summary.visits == 1420
    assert dashboard.timeseries
    assert dashboard.sources
