from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


DAG_ID = "kafka_to_clickhouse"


def _setting(name: str, default: str = "") -> str:
    return Variable.get(name, default_var=os.getenv(name, default))


def _ensure_table(client, table_name: str) -> None:
    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (
            counter_id String,
            date Date,
            visits UInt64,
            users UInt64,
            pageviews UInt64,
            bounce_rate Float64,
            page_depth Float64,
            avg_visit_duration_seconds Float64,
            loaded_at DateTime
        )
        ENGINE = MergeTree
        ORDER BY (counter_id, date)
        """
    )


def _consume_kafka_to_clickhouse(**context) -> None:
    import clickhouse_connect
    from kafka import KafkaConsumer

    topic = _setting("KAFKA_METRIKA_TOPIC", "metrika.analytics.raw")
    bootstrap_servers = _setting("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    batch_size = int(_setting("KAFKA_CLICKHOUSE_BATCH_SIZE", "500"))
    poll_timeout_ms = int(_setting("KAFKA_POLL_TIMEOUT_MS", "10000"))
    table_name = _setting("CLICKHOUSE_METRIKA_TABLE", "metrika_daily")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[item.strip() for item in bootstrap_servers.split(",")],
        group_id=_setting("KAFKA_CLICKHOUSE_GROUP_ID", "metrika-clickhouse-loader"),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        consumer_timeout_ms=poll_timeout_ms,
    )

    client = clickhouse_connect.get_client(
        host=_setting("CLICKHOUSE_HOST", "localhost"),
        port=int(_setting("CLICKHOUSE_PORT", "8123")),
        username=_setting("CLICKHOUSE_USER", "default"),
        password=_setting("CLICKHOUSE_PASSWORD", ""),
        database=_setting("CLICKHOUSE_DATABASE", "default"),
    )
    _ensure_table(client, table_name)

    rows: list[tuple] = []
    for message in consumer:
        payload = message.value
        summary = payload.get("summary") or {}
        rows.append(
            (
                str(payload["counter_id"]),
                payload["date1"],
                int(summary.get("visits") or 0),
                int(summary.get("users") or 0),
                int(summary.get("pageviews") or 0),
                float(summary.get("bounce_rate") or 0),
                float(summary.get("page_depth") or 0),
                float(summary.get("avg_visit_duration_seconds") or 0),
                payload.get("loaded_at") or datetime.utcnow().isoformat(timespec="seconds"),
            )
        )

        if len(rows) >= batch_size:
            _insert_rows(client, table_name, rows)
            consumer.commit()
            rows.clear()

    if rows:
        _insert_rows(client, table_name, rows)
        consumer.commit()

    consumer.close()


def _insert_rows(client, table_name: str, rows: list[tuple]) -> None:
    client.insert(
        table_name,
        rows,
        column_names=[
            "counter_id",
            "date",
            "visits",
            "users",
            "pageviews",
            "bounce_rate",
            "page_depth",
            "avg_visit_duration_seconds",
            "loaded_at",
        ],
    )


default_args = {
    "owner": "analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Consume Yandex Metrika records from Kafka and write them to ClickHouse",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["kafka", "clickhouse", "analytics"],
) as dag:
    consume_kafka_to_clickhouse = PythonOperator(
        task_id="consume_kafka_to_clickhouse",
        python_callable=_consume_kafka_to_clickhouse,
    )
