from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


DAG_ID = "metrika_to_kafka"
DEFAULT_YANDEX_METRIKA_TOKEN = "A7qN4xZ9Lm2BvT6pR8cD3yW5sH1kJ0fG"


def _setting(name: str, default: str = "") -> str:
    return Variable.get(name, default_var=os.getenv(name, default))


def _date_range(**context) -> tuple[str, str]:
    logical_date = context["logical_date"].date()
    return logical_date.isoformat(), logical_date.isoformat()


def _fetch_metrika_payload(**context) -> dict[str, Any]:
    import requests

    date1, date2 = _date_range(**context)
    use_mock = _setting("YANDEX_METRIKA_USE_MOCK", "false").lower() == "true"
    counter_id = _setting("YANDEX_METRIKA_COUNTER_ID", "44147844")

    if use_mock:
        return {
            "counter_id": counter_id,
            "date1": date1,
            "date2": date2,
            "summary": {
                "visits": 120,
                "users": 88,
                "pageviews": 210,
                "bounce_rate": 13.8,
                "page_depth": 1.89,
                "avg_visit_duration_seconds": 92.4,
            },
            "loaded_at": datetime.utcnow().isoformat(timespec="seconds"),
        }

    token = _setting("YANDEX_METRIKA_TOKEN", DEFAULT_YANDEX_METRIKA_TOKEN)
    if not token:
        raise RuntimeError("YANDEX_METRIKA_TOKEN is empty")

    response = requests.get(
        "https://api-metrika.yandex.net/stat/v1/data",
        params={
            "ids": counter_id,
            "date1": date1,
            "date2": date2,
            "metrics": "ym:s:visits,ym:s:users,ym:s:pageviews,ym:s:bounceRate,ym:s:pageDepth,ym:s:avgVisitDurationSeconds",
            "accuracy": "full",
        },
        headers={"Authorization": f"OAuth {token}"},
        timeout=30,
    )
    response.raise_for_status()

    totals = response.json().get("totals") or [0, 0, 0, 0, 0, 0]
    return {
        "counter_id": counter_id,
        "date1": date1,
        "date2": date2,
        "summary": {
            "visits": int(totals[0] or 0),
            "users": int(totals[1] or 0),
            "pageviews": int(totals[2] or 0),
            "bounce_rate": float(totals[3] or 0),
            "page_depth": float(totals[4] or 0),
            "avg_visit_duration_seconds": float(totals[5] or 0),
        },
        "loaded_at": datetime.utcnow().isoformat(timespec="seconds"),
    }


def _send_to_kafka(**context) -> None:
    from kafka import KafkaProducer

    payload = context["ti"].xcom_pull(task_ids="fetch_metrika_payload")
    topic = _setting("KAFKA_METRIKA_TOPIC", "metrika.analytics.raw")
    bootstrap_servers = _setting("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    producer = KafkaProducer(
        bootstrap_servers=[item.strip() for item in bootstrap_servers.split(",")],
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
    )
    producer.send(topic, key=f"{payload['counter_id']}:{payload['date1']}", value=payload)
    producer.flush()
    producer.close()


default_args = {
    "owner": "analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Load Yandex Metrika daily summary and publish it to Kafka",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["metrika", "kafka", "analytics"],
) as dag:
    fetch_metrika_payload = PythonOperator(
        task_id="fetch_metrika_payload",
        python_callable=_fetch_metrika_payload,
    )

    send_to_kafka = PythonOperator(
        task_id="send_to_kafka",
        python_callable=_send_to_kafka,
    )

    fetch_metrika_payload >> send_to_kafka
