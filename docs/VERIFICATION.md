# Verification Record

Environment: Windows, PowerShell, Docker Desktop, Python 3.12.0, `kafka-python-ng 2.2.3`, `duckdb 0.10.0`.
Services: `postgres:16`, `confluentinc/cp-kafka:7.6.1`, `confluentinc/cp-zookeeper:7.6.1`, `debezium/connect:2.4`.
Date: 2026-11-23.

The outputs below were copied from terminal screenshots taken during the run.

## 1. Unit and failure-scenario tests

```text
pytest -v
collected 13 items
...
13 passed in 0.89s
```

The three regression tests added in this release failed against the previous code and pass now:

- `test_restart_refreshes_stale_parquet_after_export_failure`
- `test_precise_decimal_strings_are_always_base64[1234-expected0]`
- `test_precise_decimal_strings_are_always_base64[NAN0-expected1]`

Kafka is replaced by a mock at the test boundary; DuckDB and Parquet files are real (pytest `tmp_path`).

## 2. Schema mismatch found and recovered

Starting the consumer against an existing `lakehouse.duckdb` from an older consumer version produced a retry loop:

```text
[ERROR] Consumer crashed, retrying in 3s: Binder Error: Referenced column "updated_at" not found in FROM clause!
```

Check:

```text
DESCRIBE orders  ->  order_id, customer_name, amount, status, created_at, source_ts_ms, connector_ts_ms
```

The old table had no `updated_at` or `is_deleted` column. Recovery used the rebuild procedure from the README (old file renamed, consumer group rewound to earliest). After restart:

```text
Committed Kafka offset dbserver1.public.orders[0]->1 (last_applied_offset=0) and refreshed Parquet snapshot.
...
Committed Kafka offset dbserver1.public.orders[0]->7 (last_applied_offset=6) and refreshed Parquet snapshot.
```

All 7 historical events in the topic were applied to the new DuckDB file.

## 3. INSERT → UPDATE → DELETE

```text
INSERT INTO public.orders ... VALUES (201, 'Live Test', 150.250, 'CREATED');   -> INSERT 0 1
UPDATE public.orders SET amount = 210.750, status = 'PAID' WHERE order_id = 201; -> UPDATE 1
DELETE FROM public.orders WHERE order_id = 201;                                -> DELETE 1
```

State after stopping the consumer:

```text
orders : [(1, Decimal('25.500'), 'CREATED', False), (101, Decimal('210.750'), 'DELETED', True), (201, Decimal('210.750'), 'DELETED', True)]
offsets: [('dbserver1.public.orders', 0, 9)]
parquet: [(1, Decimal('25.500'), 'CREATED', False), (101, Decimal('210.750'), 'DELETED', True), (201, Decimal('210.750'), 'DELETED', True)]
```

- Order 201 keeps its last amount as an exact decimal and is soft-deleted.
- Orders 1 (seed row) and 101 (an earlier test) were rebuilt from Kafka history.
- The Parquet snapshot matches DuckDB.

## 4. Restart without new messages

The consumer joined the group and was assigned `dbserver1.public.orders` partition 0. No `Committed Kafka offset` lines appeared: there were no messages after the committed offset.

## 5. Forced replay

With the consumer stopped, the consumer group was rewound to offset 0, so Kafka redelivered all 10 messages:

```text
NEW-OFFSET 0
...
Skipping already applied offset topic=dbserver1.public.orders partition=0 offset=0 (last_applied=9)
...
Skipping already applied offset topic=dbserver1.public.orders partition=0 offset=9 (last_applied=9)
```

State after stopping the consumer was identical to section 3 (same three rows, `last_applied_offset = 9`, Parquet equal to DuckDB).

## 6. Clean install from GitHub

The repository was cloned into a new folder (`cdc-clean-test`), so Docker Compose created new containers and empty volumes. Only the README steps were used:

```text
git clone https://github.com/tutarzeliha-ctrl/realtime-cdc-lakehouse.git cdc-clean-test
python -m venv venv; .\venv\Scripts\Activate.ps1
pip install -r requirements.txt     -> kafka-python-ng 2.2.3, duckdb 0.10.0, pyarrow 15.0.0, pytest 8.0.0
pytest -v                           -> 13 passed in 1.24s
docker compose up -d
(connector registered with curl.exe as in the README)
python .\lakehouse_consumer.py
Successfully joined group lakehouse-cdc-consumer-v1 with generation 1
Committed Kafka offset dbserver1.public.orders[0]->1 (last_applied_offset=0) and refreshed Parquet snapshot.
```

The seed row from `init.sql` reached DuckDB through Debezium's initial snapshot.

## 7. Analytics layer (dbt)

Run on Windows from `analytics/`, in its own virtual environment (`dbt-core 1.12.7`, `dbt-duckdb 1.11.1`, `duckdb 2.0.1`).

```text
dbt build --vars "{orders_snapshot_path: sample_data/orders_snapshot_sample.parquet}"
Done. PASS=13 WARN=0 ERROR=0 SKIP=0     (milestone 1: stg_orders, fct_orders)

dbt build                               (live orders_snapshot.parquet)
Done. PASS=13 WARN=0 ERROR=0 SKIP=0     (milestone 1)
Done. PASS=20 WARN=0 ERROR=0 SKIP=0     (milestone 2: + snapshot and fct_order_status_daily)
```

Current state from `fct_orders` on the live data: 4 cancelled, 1 pending, 1 successful. These are the test orders created during the verification runs above, not customer data. On 23 and 24 November, `fct_order_status_daily` shows the same counts, which is what `assert_daily_status_matches_current_state` checks.

Checked in a separate environment before the Windows run:

- Three simulated daily runs (20, 21, 22 November) give the expected end-of-day status for each order.
- Running `dbt build` twice with unchanged data adds no rows to the snapshot history.
- An unmapped source status (`SHIPPED`) raises a warning in staging and fails the `fct_orders` build.
- Changing a history row by hand makes `assert_daily_status_matches_current_state` fail.

## 8. Orchestration (Airflow) — checked before the Windows run

Checked in a separate Linux environment with Airflow 3.4.0, without Docker, Kafka or PostgreSQL. Kafka lag and PostgreSQL totals were replaced by fixed values; dbt ran for real on the sample data.

```text
pytest orchestration/tests   (Airflow 3.4.0, duckdb 2.0.1)       17 passed
pytest orchestration/tests   (consumer venv, duckdb 0.10.0)      14 passed, 1 skipped (DAG tests need Airflow)
```

`daily_analytics` run with `dag.test()`:

| Scenario | freshness_gate | dbt_build | reconcile_source_with_snapshot |
|---|---|---|---|
| Lag 340, snapshot 2 days old | failed (alert logged) | upstream_failed (did not run) | upstream_failed |
| Lag 0 | success | success (PASS=20) | success: 4 orders, total 276.900 |
| One more order in PostgreSQL | success | success | failed: row count 5 vs 4, amount 296.890 vs 276.900 (alert logged) |

Not yet verified: the Docker image build, the DAGs against the live Kafka and PostgreSQL, and the Slack message.

## Not verified

- A Parquet export failure in the live system (covered only by a unit test).
- Multiple partitions or multiple consumer instances.
- Behaviour after a power loss or a Docker volume reset.
- Daily history across several real days: so far one snapshot run on the live data; the multi-day logic is tested only with simulated data.
