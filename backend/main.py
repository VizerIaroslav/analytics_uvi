from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from backend.analytics import DashboardData, build_dashboard_data, default_date_range
from backend.config import Settings, get_settings
from backend.metrika_client import MetrikaClientError, MockMetrikaClient, YandexMetrikaClient

app = FastAPI(title="Yandex Metrika Analytics API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8501", "http://127.0.0.1:8501"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_metrika_client(settings: Settings = Depends(get_settings)):
    if settings.yandex_metrika_use_mock:
        return MockMetrikaClient()
    return YandexMetrikaClient(
        token=settings.yandex_metrika_token,
        counter_id=settings.yandex_metrika_counter_id,
        base_url=settings.yandex_metrika_base_url,
    )


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/dashboard", response_model=DashboardData)
async def dashboard(
    date1: str | None = Query(default=None, description="Start date in YYYY-MM-DD format"),
    date2: str | None = Query(default=None, description="End date in YYYY-MM-DD format"),
    client=Depends(get_metrika_client),
) -> DashboardData:
    if not date1 or not date2:
        date1, date2 = default_date_range()

    try:
        return await build_dashboard_data(client, date1, date2)
    except MetrikaClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
