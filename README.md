# Yandex Metrika Analytics Demo

Тестовый Python-проект для сбора данных из Яндекс Метрики и построения минимального аналитического дашборда.

## Что внутри

- `backend/` - FastAPI API, который ходит в Yandex Metrika Reporting API.
- `frontend/` - Streamlit-дэшборд с KPI, графиком по дням и источниками трафика.
- `dags/` - Airflow DAG-и для загрузки Метрики в Kafka и перекладки из Kafka в ClickHouse.
- `tests/` - тесты парсинга и сборки аналитической модели.
- `MockMetrikaClient` - режим демо-данных, чтобы проект запускался без реального токена.

## Быстрый старт

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
```

По умолчанию включен mock-режим:

```env
YANDEX_METRIKA_USE_MOCK=true
```

Запуск backend:

```bash
uvicorn backend.main:app --reload --port 8000
```

Запуск frontend во втором терминале:

```bash
streamlit run frontend/dashboard.py
```

Дэшборд будет доступен на `http://localhost:8501`, API - на `http://localhost:8000`.

## Подключение реальной Метрики

1. Получите OAuth-токен Яндекса с правами чтения Яндекс Метрики.
2. Найдите ID счетчика в интерфейсе Метрики.
3. Заполните `.env`:

```env
YANDEX_METRIKA_TOKEN=ваш_oauth_token
YANDEX_METRIKA_COUNTER_ID=12345678
YANDEX_METRIKA_USE_MOCK=false
BACKEND_API_URL=http://localhost:8000
```

Backend использует методы Reporting API:

- `/stat/v1/data` для сводных метрик и источников;
- `/stat/v1/data/bytime` для дневной динамики.

## Проверка

```bash
pytest
```

## Airflow DAG-и

Для Airflow-окружения установите дополнительные зависимости:

```bash
pip install -r requirements-airflow.txt
```

Скопируйте папку `dags/` в папку DAG-ов Airflow или укажите ее как `AIRFLOW__CORE__DAGS_FOLDER`.

Доступные DAG-и:

- `metrika_to_kafka` - ежедневно забирает сводку из Яндекс Метрики и пишет JSON-сообщение в Kafka topic `metrika.analytics.raw`.
- `kafka_to_clickhouse` - каждые 15 минут читает topic Kafka и пишет строки в таблицу ClickHouse `metrika_daily`.

Настройки можно передавать через переменные окружения или Airflow Variables:

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_METRIKA_TOPIC=metrika.analytics.raw
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_DATABASE=default
CLICKHOUSE_METRIKA_TABLE=metrika_daily
```

## API

```http
GET /api/dashboard?date1=2026-06-16&date2=2026-06-29
```

Ответ содержит:

- `summary` - визиты, пользователи, просмотры, отказы, глубина, средняя длительность визита;
- `timeseries` - динамика по дням;
- `sources` - топ источников трафика.
