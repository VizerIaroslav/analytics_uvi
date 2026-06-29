from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Protocol

from pydantic import BaseModel


class MetrikaClientProtocol(Protocol):
    async def fetch_summary(self, date1: str, date2: str) -> dict[str, Any]:
        ...

    async def fetch_timeseries(self, date1: str, date2: str) -> dict[str, Any]:
        ...

    async def fetch_sources(self, date1: str, date2: str, limit: int = 10) -> dict[str, Any]:
        ...


class SummaryMetrics(BaseModel):
    visits: int
    users: int
    pageviews: int
    bounce_rate: float
    page_depth: float
    avg_visit_duration_seconds: float


class TimeseriesPoint(BaseModel):
    date: str
    visits: int
    users: int
    pageviews: int


class SourceMetrics(BaseModel):
    source: str
    visits: int
    users: int


class DashboardData(BaseModel):
    date1: str
    date2: str
    summary: SummaryMetrics
    timeseries: list[TimeseriesPoint]
    sources: list[SourceMetrics]


def default_date_range(days: int = 14) -> tuple[str, str]:
    end = date.today()
    start = end - timedelta(days=days - 1)
    return start.isoformat(), end.isoformat()


async def build_dashboard_data(client: MetrikaClientProtocol, date1: str, date2: str) -> DashboardData:
    summary_raw = await client.fetch_summary(date1, date2)
    timeseries_raw = await client.fetch_timeseries(date1, date2)
    sources_raw = await client.fetch_sources(date1, date2)

    return DashboardData(
        date1=date1,
        date2=date2,
        summary=parse_summary(summary_raw),
        timeseries=parse_timeseries(timeseries_raw),
        sources=parse_sources(sources_raw),
    )


def parse_summary(payload: dict[str, Any]) -> SummaryMetrics:
    totals = payload.get("totals") or [0, 0, 0, 0, 0, 0]
    return SummaryMetrics(
        visits=int(totals[0] or 0),
        users=int(totals[1] or 0),
        pageviews=int(totals[2] or 0),
        bounce_rate=round(float(totals[3] or 0), 2),
        page_depth=round(float(totals[4] or 0), 2),
        avg_visit_duration_seconds=round(float(totals[5] or 0), 2),
    )


def parse_timeseries(payload: dict[str, Any]) -> list[TimeseriesPoint]:
    intervals = payload.get("time_intervals") or []
    data = payload.get("data") or []
    if not data:
        return []

    metrics = data[0].get("metrics") or [[], [], []]
    points: list[TimeseriesPoint] = []
    for index, interval in enumerate(intervals):
        points.append(
            TimeseriesPoint(
                date=interval[0],
                visits=_metric_at(metrics, 0, index),
                users=_metric_at(metrics, 1, index),
                pageviews=_metric_at(metrics, 2, index),
            )
        )
    return points


def parse_sources(payload: dict[str, Any]) -> list[SourceMetrics]:
    rows = payload.get("data") or []
    result: list[SourceMetrics] = []
    for row in rows:
        dimensions = row.get("dimensions") or [{"name": "Неизвестно"}]
        metrics = row.get("metrics") or [0, 0]
        result.append(
            SourceMetrics(
                source=dimensions[0].get("name") or "Неизвестно",
                visits=int(metrics[0] or 0),
                users=int(metrics[1] or 0),
            )
        )
    return result


def _metric_at(metrics: list[list[float]], metric_index: int, item_index: int) -> int:
    try:
        return int(metrics[metric_index][item_index] or 0)
    except IndexError:
        return 0
