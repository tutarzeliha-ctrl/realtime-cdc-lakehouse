# Runbook

What to do when an Airflow task fails. Each failure message names the DAG, the task and the reason.

## `Stale data: N unprocessed message(s) … The consumer has probably stopped.`

From `cdc_freshness_check` or `daily_analytics.freshness_gate`.

1. Check whether the consumer process is running. If not, start it: `python .\lakehouse_consumer.py`.
2. If it is running, read its log:
   - `Consumer crashed, retrying in 3s: …` repeated → the same message keeps failing (see "Known limitations" in the README: no dead-letter queue). Fix the cause before restarting.
   - `Binder Error … not found` → the DuckDB file has an older schema. Follow "Rebuilding the local state" in the README.
3. Watch the lag go down: the next `cdc_freshness_check` run should succeed.
4. If `daily_analytics` was blocked, trigger it again in Airflow after the consumer has caught up.

## `Debezium is not capturing changes: …`

From `cdc_freshness_check.debezium_connector_is_running` or `daily_analytics.connector_gate`.

1. `Kafka Connect did not respond` → check `docker compose ps`. If `connect` is missing or exited, start it: `docker compose up -d connect`.
2. `Connector … is PAUSED / FAILED` or `Task 0 is FAILED: …` → read the first trace line in the message, then `curl.exe -s http://localhost:8083/connectors/orders-cdc-connector/status` for the full trace. After fixing the cause, restart the task: `curl.exe -s -X POST http://localhost:8083/connectors/orders-cdc-connector/tasks/0/restart`.
3. `NOT REGISTERED` → register the connector (README, "Setup").
4. Changes made while Debezium was down are not lost: PostgreSQL keeps them in the replication slot, and Debezium reads them when it restarts. Watch the consumer apply them, then check that the next `cdc_freshness_check` run passes.

## `Row count differs …` / `Total amount differs …`

From `daily_analytics.reconcile_source_with_snapshot`, after two retries.

1. Run `cdc_freshness_check`. If it fails, handle the stale data first.
2. If the consumer is up to date and the difference stays, compare the two sides by order:
   - PostgreSQL: `SELECT order_id, amount FROM public.orders ORDER BY order_id;`
   - Snapshot: `SELECT order_id, amount FROM read_parquet('orders_snapshot.parquet') WHERE NOT is_deleted ORDER BY order_id;`
3. A missing order points to a lost or skipped change; a different amount points to a decoding problem. Record what you find before rebuilding.

## `dbt_build` failed

Open the task log. A failing `accepted_values` test on `order_status` means the source sent a status that is not mapped yet (see docs/METRICS.md). Add the mapping and its definition before rerunning.

## `Topic 'dbserver1.public.orders' not found`

Debezium has not created the topic yet, or the connector is not registered. Check `curl.exe -s http://localhost:8083/connectors/orders-cdc-connector/status`.
