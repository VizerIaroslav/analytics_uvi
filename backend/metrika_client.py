from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import httpx


class MetrikaClientError(RuntimeError):
    pass


class YandexMetrikaClient:
    def __init__(self, token: str, counter_id: str, base_url: str) -> None:
        self.token = token
        self.counter_id = counter_id
        self.base_url = base_url.rstrip("/")

    async def fetch_summary(self, date1: str, date2: str) -> dict[str, Any]:
        return await self._get(
            "/stat/v1/data",
            {
                "ids": self.counter_id,
                "metrics": "ym:s:visits,ym:s:users,ym:s:pageviews,ym:s:bounceRate,ym:s:pageDepth,ym:s:avgVisitDurationSeconds",
                "date1": date1,
                "date2": date2,
                "accuracy": "full",
            },
        )

    async def fetch_timeseries(self, date1: str, date2: str) -> dict[str, Any]:
        return await self._get(
            "/stat/v1/data/bytime",
            {
                "ids": self.counter_id,
                "metrics": "ym:s:visits,ym:s:users,ym:s:pageviews",
                "date1": date1,
                "date2": date2,
                "group": "day",
                "accuracy": "full",
            },
        )

    async def fetch_sources(self, date1: str, date2: str, limit: int = 10) -> dict[str, Any]:
        return await self._get(
            "/stat/v1/data",
            {
                "ids": self.counter_id,
                "dimensions": "ym:s:lastTrafficSource",
                "metrics": "ym:s:visits,ym:s:users",
                "date1": date1,
                "date2": date2,
                "sort": "-ym:s:visits",
                "limit": limit,
                "lang": "ru",
                "accuracy": "full",
            },
        )

    async def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        if not self.token:
            raise MetrikaClientError("YANDEX_METRIKA_TOKEN is empty. Set it in .env or enable mock mode.")

        headers = {"Authorization": f"OAuth {self.token}"}
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(f"{self.base_url}{path}", params=params, headers=headers)

        if response.status_code >= 400:
            raise MetrikaClientError(f"Metrika API error {response.status_code}: {response.text}")

        return response.json()


class MockMetrikaClient:
    async def fetch_summary(self, date1: str, date2: str) -> dict[str, Any]:
        return {
            "totals": [1420, 1038, 2680, 13.8, 1.89, 92.4],
            "query": {"date1": date1, "date2": date2},
        }

    async def fetch_timeseries(self, date1: str, date2: str) -> dict[str, Any]:
        start = date.fromisoformat(date1)
        end = date.fromisoformat(date2)
        days = (end - start).days + 1
        dates = [(start + timedelta(days=offset)).isoformat() for offset in range(days)]
        metrics = [
            [120 + offset * 7 + (offset % 3) * 15 for offset in range(days)],
            [88 + offset * 5 + (offset % 4) * 9 for offset in range(days)],
            [210 + offset * 10 + (offset % 2) * 25 for offset in range(days)],
        ]
        return {"time_intervals": [[value, value] for value in dates], "data": [{"metrics": metrics}]}

    async def fetch_sources(self, date1: str, date2: str, limit: int = 10) -> dict[str, Any]:
        rows = [
            ("Переходы из поисковых систем", 530, 410),
            ("Прямые заходы", 360, 280),
            ("Переходы по ссылкам", 225, 190),
            ("Переходы из социальных сетей", 180, 130),
            ("Переходы из рекламных систем", 125, 95),
        ]
        return {
            "data": [
                {"dimensions": [{"name": name}], "metrics": [visits, users]}
                for name, visits, users in rows[:limit]
            ],
            "query": {"date1": date1, "date2": date2},
        }
