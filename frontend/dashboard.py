from __future__ import annotations

import os
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import requests
import streamlit as st


API_URL = os.getenv("BACKEND_API_URL", "http://localhost:8000").rstrip("/")


st.set_page_config(page_title="Yandex Metrika Analytics", layout="wide")
st.title("Аналитика Яндекс Метрики")

today = date.today()
default_start = today - timedelta(days=13)

with st.sidebar:
    st.header("Период")
    date1 = st.date_input("Дата начала", default_start)
    date2 = st.date_input("Дата окончания", today)
    refresh = st.button("Обновить", type="primary")


@st.cache_data(ttl=300)
def load_dashboard(start: str, end: str) -> dict:
    response = requests.get(
        f"{API_URL}/api/dashboard",
        params={"date1": start, "date2": end},
        timeout=20,
    )
    response.raise_for_status()
    return response.json()


try:
    if refresh:
        load_dashboard.clear()
    payload = load_dashboard(date1.isoformat(), date2.isoformat())
except requests.RequestException as exc:
    st.error(f"Не удалось получить данные из backend: {exc}")
    st.stop()

summary = payload["summary"]
top = st.columns(6)
top[0].metric("Визиты", f"{summary['visits']:,}".replace(",", " "))
top[1].metric("Пользователи", f"{summary['users']:,}".replace(",", " "))
top[2].metric("Просмотры", f"{summary['pageviews']:,}".replace(",", " "))
top[3].metric("Отказы", f"{summary['bounce_rate']}%")
top[4].metric("Глубина", summary["page_depth"])
top[5].metric("Среднее время", f"{summary['avg_visit_duration_seconds']} с")

timeseries = pd.DataFrame(payload["timeseries"])
sources = pd.DataFrame(payload["sources"])

left, right = st.columns([2, 1])

with left:
    st.subheader("Динамика")
    if timeseries.empty:
        st.info("Нет данных за выбранный период.")
    else:
        fig = px.line(
            timeseries,
            x="date",
            y=["visits", "users", "pageviews"],
            markers=True,
            labels={"date": "Дата", "value": "Значение", "variable": "Метрика"},
        )
        st.plotly_chart(fig, use_container_width=True)

with right:
    st.subheader("Источники трафика")
    if sources.empty:
        st.info("Нет данных по источникам.")
    else:
        fig = px.bar(
            sources.sort_values("visits", ascending=True),
            x="visits",
            y="source",
            orientation="h",
            labels={"visits": "Визиты", "source": "Источник"},
        )
        st.plotly_chart(fig, use_container_width=True)

st.subheader("Таблица по дням")
st.dataframe(timeseries, use_container_width=True, hide_index=True)
