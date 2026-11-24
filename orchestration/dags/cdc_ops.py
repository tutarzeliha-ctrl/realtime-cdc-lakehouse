"""Airflow DAGs that operate the CDC pipeline.

cdc_freshness_check  every 5 minutes: fails when the consumer has stopped.
daily_analytics      once a day: freshness gate -> dbt build -> source reconciliation.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.sdk.exceptions import AirflowFailException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

from alerts import notify_failure
import pipeline_checks as checks

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
CONSUMER_GROUP = os.environ.get("CDC_CONSUMER_GROUP", "lakehouse-cdc-consumer-v1")
CDC_TOPIC = os.environ.get("CDC_TOPIC", "dbserver1.public.orders")
SNAPSHOT_PATH = os.environ.get("SNAPSHOT_PATH", "/opt/project/orders_snapshot.parquet")
MAX_AGE_SECONDS = float(os.environ.get("FRESHNESS_MAX_AGE_MINUTES", "10")) * 60
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/project/analytics")
DBT_BIN = os.environ.get("DBT_BIN", "/opt/dbt_venv/bin/dbt")

PG = dict(
    host=os.environ.get("PG_HOST", "postgres"),
    port=int(os.environ.get("PG_PORT", "5432")),
    dbname=os.environ.get("PG_DB", "app_db"),
    user=os.environ.get("PG_USER", "app_user"),
    password=os.environ.get("PG_PASSWORD", ""),
)


def check_freshness() -> dict:
    lag = checks.get_consumer_lag(KAFKA_BOOTSTRAP, CONSUMER_GROUP, CDC_TOPIC)
    age = checks.get_snapshot_age_seconds(SNAPSHOT_PATH)
    result = checks.evaluate_freshness(lag, age, MAX_AGE_SECONDS)
    print(result.reason)
    if result.stale:
        # Fail without retrying: a stopped consumer will not recover by waiting a minute.
        raise AirflowFailException(f"Stale data: {result.reason}")
    return {"lag": result.lag, "snapshot_age_seconds": result.snapshot_age_seconds}


@dag(
    dag_id="cdc_freshness_check",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"on_failure_callback": notify_failure, "retries": 0},
    tags=["cdc", "monitoring"],
)
def cdc_freshness_check():

    @task
    def consumer_is_up_to_date() -> dict:
        return check_freshness()

    consumer_is_up_to_date()


@dag(
    dag_id="daily_analytics",
    schedule="0 6 * * *",  # 06:00 UTC = 09:00 in Türkiye
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"on_failure_callback": notify_failure, "retries": 0},
    tags=["dbt", "analytics"],
)
def daily_analytics():

    @task
    def freshness_gate() -> dict:
        # If the data is stale, this task fails and dbt build does not run:
        # no daily history is recorded from a stale snapshot.
        return check_freshness()

    dbt_build = BashOperator(
        task_id="dbt_build",
        # The analytics/ folder is shared with dbt runs on the Windows host. The container
        # keeps its own target/log folders and skips partial parsing, so it never reads
        # parse state written by another dbt installation.
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && {DBT_BIN} build --profiles-dir . "
            "--target-path /opt/airflow/dbt/target --log-path /opt/airflow/dbt/logs --no-partial-parse"
        ),
        env={"DBT_SEND_ANONYMOUS_USAGE_STATS": "False"},
        append_env=True,
    )

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def reconcile_source_with_snapshot() -> dict:
        # Retried because an order written between the two reads causes a short-lived difference.
        source = checks.fetch_source_totals(**PG)
        snapshot = checks.fetch_snapshot_totals(SNAPSHOT_PATH)
        problems = checks.evaluate_reconciliation(source, snapshot)
        if problems:
            raise ValueError(" ".join(problems))
        print(f"PostgreSQL and snapshot agree: {source.row_count} orders, total {source.total_amount}.")
        return {"orders": source.row_count, "total_amount": str(source.total_amount)}

    freshness_gate() >> dbt_build >> reconcile_source_with_snapshot()


cdc_freshness_check()
daily_analytics()
