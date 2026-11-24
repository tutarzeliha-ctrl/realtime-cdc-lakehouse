# Orchestration and monitoring (Airflow)

Airflow runs the analytics layer on a schedule and checks that the CDC consumer is still processing changes.

## Why a freshness check is needed

dbt tests compare `fct_orders` with the Parquet snapshot. If the consumer stops, both stay at the same old state and the tests still pass. The freshness check looks at Kafka and the snapshot instead.

## The freshness rule

The data is **stale** when both are true:

1. **Consumer lag > 0:** the topic holds messages the consumer group has not committed.
2. **The Parquet snapshot is older than `FRESHNESS_MAX_AGE_MINUTES` (default 10):** the consumer rewrites the snapshot after every applied message, so its modification time is the time of the last applied change.

A quiet period without new orders has lag 0 and is never stale. A burst that is still being processed has a recent snapshot and is not stale either.

## The connector check

The freshness rule only sees changes that reached Kafka. If Debezium stops, nothing arrives, lag stays 0 and the data looks fresh. A second check reads the Kafka Connect REST API (`/connectors/orders-cdc-connector/status`) and fails when Connect does not respond, or when the connector or one of its tasks is not `RUNNING`.

This gap was found in the live test: the `connect` container had stopped, an inserted order never reached Kafka, and the freshness check stayed green.

## DAGs

| DAG | Schedule | Tasks |
|---|---|---|
| `cdc_freshness_check` | every 5 minutes | `consumer_is_up_to_date`, `debezium_connector_is_running` |
| `daily_analytics` | 06:00 UTC daily | `connector_gate` + `freshness_gate` → `dbt_build` → `reconcile_source_with_snapshot` |

- If either gate fails, `dbt_build` does not run, so no daily history is recorded from stale data.
- `reconcile_source_with_snapshot` compares the number and total amount of orders in PostgreSQL with the non-deleted rows in the snapshot (PostgreSQL removes deleted orders; the snapshot keeps them as `is_deleted = TRUE`). It retries twice, because an order written between the two reads causes a short-lived difference.

## Alerts

Every failure is written to the task log. If `SLACK_WEBHOOK_URL` is set in `.env` (copy `.env.example`), the same message is also sent to Slack. `.env` is not committed.

## Run

The consumer still runs on the host (`python .\lakehouse_consumer.py`). Airflow runs in Docker and reads the same project folder.

```powershell
docker compose up -d --build airflow
```

The first build takes several minutes. Then open http://localhost:8080 (no login: local development setup), and switch both DAGs on.

dbt runs inside the container in its own virtual environment, with the same versions as `analytics/requirements.txt`. It writes to the same `analytics/analytics.duckdb` as a dbt run on the host, so do not run both at the same time.

## Tests

```powershell
pytest orchestration/tests -v
```

The check logic runs without Airflow. The DAG structure tests are skipped when Airflow is not installed.

## Known limitations

- `airflow standalone` with SQLite is a local development setup, not a production deployment.
- Tested with one partition and one consumer.
- The connector check sees whether Debezium is running, not whether PostgreSQL's replication slot is falling behind. Slot lag is not monitored.
- dbt runs in the container with its own target folder and without partial parsing, because the `analytics/` folder is shared with dbt runs on Windows (see docs/VERIFICATION.md, section 8).
