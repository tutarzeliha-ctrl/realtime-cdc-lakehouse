# Realtime CDC Lakehouse (PostgreSQL → Debezium → Kafka → DuckDB → Parquet)

A local Change Data Capture (CDC) pipeline on Windows and Docker Compose.
Debezium captures `INSERT`, `UPDATE` and `DELETE` changes from a PostgreSQL `orders` table and publishes them to Kafka.
A Python consumer applies each change to DuckDB in a local transaction, records the applied Kafka offset in the same transaction, and publishes a Parquet snapshot of the table.

This is a local portfolio project. It has been tested on a single machine with one Kafka partition and one consumer.

## Architecture

```text
PostgreSQL 16 (wal_level=logical, REPLICA IDENTITY FULL)
   │  pgoutput logical replication
   ▼
Debezium Connect 2.4  ──►  Kafka (cp-kafka 7.6.1, topic dbserver1.public.orders)
                                │  127.0.0.1:9092 (host listener)
                                ▼
                     lakehouse_consumer.py (kafka-python-ng, manual commits)
                                │
                  ┌─────────────┴─────────────┐
                  ▼                           ▼
        lakehouse.duckdb               orders_snapshot.parquet
   (orders + cdc_offsets tables)     (temp file + os.replace)
```

## Processing guarantees

For each Kafka message, the consumer runs three steps in this order:

1. **DuckDB transaction:** apply the change to `orders` and write the message offset to `cdc_offsets.last_applied_offset`. Both are committed together or rolled back together.
2. **Parquet export:** write the full `orders` table to a temporary file, then replace `orders_snapshot.parquet` with `os.replace`.
3. **Kafka commit:** commit `offset + 1` for the processed topic-partition only.

What this gives:

- **Kafka's committed offset** decides where the consumer resumes after a restart.
- **The local offset** decides whether a message has already been applied. If the process stops after step 1 but before step 3, Kafka redelivers the message and the consumer skips it instead of applying it twice.
- **On every (re)start**, the Parquet snapshot is rebuilt from DuckDB. This covers the case where step 1 succeeded and step 2 failed: the redelivered message is skipped, and the snapshot catches up at restart.
- Delivery from Kafka is at-least-once; duplicate application is prevented by the local offset check. DuckDB, Parquet and Kafka are not updated atomically as one unit.

### Change semantics

| Debezium `op` | Effect in DuckDB / Parquet |
|---|---|
| `r` (snapshot), `c` (insert) | Upsert the row |
| `u` (update) | Upsert the row; the original `created_at` is kept |
| `d` (delete) | Soft delete: `status = 'DELETED'`, `is_deleted = TRUE`, last known values kept |

### Decimal handling

The connector uses `decimal.handling.mode = precise`. Debezium then sends `amount` as base64-encoded big-endian bytes. `decode_precise_decimal` always decodes strings as base64 and returns a `Decimal`, never a float.
The scale is fixed at `3`, matching `NUMERIC(12, 3)` in `init.sql`.

## Project structure

```text
realtime-cdc-lakehouse/
├── docker-compose.yml          # Zookeeper, Kafka, PostgreSQL 16, Debezium Connect 2.4, Airflow
├── init.sql                    # Source table, REPLICA IDENTITY FULL, one seed row
├── postgres-connector.json     # Debezium PostgreSQL connector configuration
├── lakehouse_consumer.py       # Consumer, decimal decoder, DuckDB state, Parquet export
├── test_lakehouse_consumer.py  # Unit and failure-scenario tests (pytest)
├── check_kafka.py              # Debug helper: prints the first 10 topic messages
├── requirements.txt
├── analytics/                  # dbt project on top of the Parquet snapshot (see analytics/README.md)
├── orchestration/              # Airflow DAGs and health checks (see orchestration/README.md)
├── .env.example                # Optional Slack webhook for alerts
├── docs/VERIFICATION.md        # Live verification record
├── docs/METRICS.md             # Business definitions for order status and metrics
├── docs/RUNBOOK.md             # What to do when an Airflow check fails
└── PROJECT_STATUS.md
```

## Analytics layer

The `analytics/` folder contains a dbt project (dbt-duckdb) that reads `orders_snapshot.parquet` and builds:

- `fct_orders`: one row per order with its current business status (pending, successful, cancelled)
- `fct_order_status_daily`: one row per order per day with the status at the end of the day, recorded with a dbt snapshot

 It has its own virtual environment and its own DuckDB file, so it can run while the consumer is running. Setup and tests: [analytics/README.md](analytics/README.md).

## Orchestration and monitoring

The `orchestration/` folder adds Airflow (one container, local development setup):

- `cdc_freshness_check` runs every 5 minutes and fails when the consumer has stopped: unprocessed messages in Kafka and no snapshot published for 10 minutes. A quiet period without orders does not trigger it.
- `daily_analytics` runs once a day: freshness gate → `dbt build` → reconciliation of PostgreSQL with the snapshot. When the data is stale, dbt does not run.
- Failures are logged and, optionally, sent to Slack. [docs/RUNBOOK.md](docs/RUNBOOK.md) lists what to do for each alert.

Setup and design: [orchestration/README.md](orchestration/README.md).

## Requirements

- Windows with PowerShell, Docker Desktop (Docker Compose v2)
- Python 3.12

Tested dependency versions are pinned in `requirements.txt` (`kafka-python-ng 2.2.3`, `duckdb 0.10.0`, `pyarrow 15.0.0`, `pytest 8.0.0`).

## Setup

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
docker compose up -d
docker compose ps        # all services should be running / healthy
```

Register the Debezium connector:

```powershell
curl.exe -s -X POST -H "Content-Type: application/json" --data-binary "@postgres-connector.json" http://localhost:8083/connectors
curl.exe -s http://localhost:8083/connectors/orders-cdc-connector/status
```

The status response should show `"state":"RUNNING"` for the connector and its task.

The database password in `docker-compose.yml` and `postgres-connector.json` is a local-only development value. Do not reuse it elsewhere.

## Run

```powershell
pytest -v
python .\lakehouse_consumer.py
```

Stop the consumer with `Ctrl + C`. A `Fetch to node 1 failed: Cancelled` log line during shutdown is expected: an in-flight fetch is cancelled, followed by `shutting down cleanly`.

## Live verification

With the consumer running, use a second terminal:

```powershell
docker compose exec -T postgres psql -U app_user -d app_db -c "INSERT INTO public.orders (order_id, customer_name, amount, status) VALUES (201, 'Live Test', 150.250, 'CREATED');"
docker compose exec -T postgres psql -U app_user -d app_db -c "UPDATE public.orders SET amount = 210.750, status = 'PAID' WHERE order_id = 201;"
docker compose exec -T postgres psql -U app_user -d app_db -c "DELETE FROM public.orders WHERE order_id = 201;"
```

Stop the consumer (DuckDB allows only one process to open the file while the consumer holds it), then inspect DuckDB and Parquet:

```powershell
@'
import duckdb
con = duckdb.connect("lakehouse.duckdb", read_only=True)
print("orders :", con.execute("SELECT order_id, amount, status, is_deleted FROM orders ORDER BY order_id").fetchall())
print("offsets:", con.execute("SELECT topic, partition_id, last_applied_offset FROM cdc_offsets").fetchall())
print("parquet:", con.execute("SELECT order_id, amount, status, is_deleted FROM read_parquet('orders_snapshot.parquet') ORDER BY order_id").fetchall())
'@ | python -
```

Recorded results, including a forced replay test, are in [docs/VERIFICATION.md](docs/VERIFICATION.md).

## Rebuilding the local state

Deleting `lakehouse.duckdb` alone is **not** enough. The consumer group keeps its committed offset in Kafka, so a fresh DuckDB file would start from that offset and miss earlier changes.

To rebuild DuckDB from the Kafka topic:

```powershell
# 1. Stop the consumer (Ctrl + C)
# 2. Move the old files aside
Rename-Item lakehouse.duckdb lakehouse.old.duckdb
Rename-Item lakehouse.duckdb.wal lakehouse.old.duckdb.wal -ErrorAction SilentlyContinue
# 3. Rewind the consumer group (the consumer must be stopped)
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server 127.0.0.1:9092 --group lakehouse-cdc-consumer-v1 --topic dbserver1.public.orders --reset-offsets --to-earliest --execute
# 4. Start the consumer again
python .\lakehouse_consumer.py
```

This only works while the topic still holds the full history (Kafka's default retention is 7 days).

## Troubleshooting

| Symptom | Cause | Action |
|---|---|---|
| `Binder Error: Referenced column "updated_at" not found`, retried every 3 s | `lakehouse.duckdb` was created by an older consumer version with a different schema | Rebuild the local state (section above) |
| `ModuleNotFoundError: No module named 'kafka.vendor.six.moves'` | `kafka-python 2.0.2` is installed; it does not work on Python 3.12 | `pip install -r requirements.txt` (uses `kafka-python-ng`) |
| Inspection script cannot open `lakehouse.duckdb` | The consumer still holds the file | Stop the consumer first |
| Consumer connects but receives nothing | Connector not registered or not running | Check the connector status endpoint |

## Known limitations

- **Tested scope:** one machine, one Kafka partition, one consumer instance. Rebalancing with several consumers has not been tested.
- **Poison messages:** an event that cannot be decoded or applied stops progress. The consumer retries every 3 seconds without a dead-letter queue or skip policy.
- **No schema migration:** `CREATE TABLE IF NOT EXISTS` does not change an existing table. A schema change requires a rebuild.
- **Fixed decimal scale:** the scale (`3`) is not read from the Debezium message schema.
- **Snapshot cost:** the full table is rewritten to Parquet after every message, so export time grows with table size.
- **Durability:** `os.replace` swaps the file atomically, but the file is not fsynced; a power loss is not covered.
- **Timestamps:** `updated_at` stores Debezium's event `ts_ms` (connector processing time, epoch milliseconds as text), not the source commit time.
- **OneDrive:** the project was run inside a OneDrive-synced folder. Sync locking was not observed but has not been tested deliberately.
