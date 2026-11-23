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

## Not verified in this run

- Connector registration from a clean environment (the connector was already registered from an earlier session).
- A Parquet export failure in the live system (covered only by a unit test).
- Multiple partitions or multiple consumer instances.
- Behaviour after a power loss or a Docker volume reset.
