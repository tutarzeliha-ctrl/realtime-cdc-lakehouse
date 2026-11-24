# Order Analytics (dbt + DuckDB)

dbt models on top of the CDC pipeline in this repository.

```text
lakehouse_consumer.py ──► lakehouse.duckdb          (consumer only)
                     └──► orders_snapshot.parquet  ──► dbt ──► analytics.duckdb
```

dbt reads the Parquet snapshot, not `lakehouse.duckdb`. The consumer holds a write lock on `lakehouse.duckdb` while it runs; the Parquet file is replaced atomically, so dbt can read it at any time. dbt writes to its own `analytics.duckdb`.

## Models

| Model | Grain | Purpose |
|---|---|---|
| `stg_orders` (view) | one row per order | Types, UTC timestamps, business status |
| `fct_orders` (table) | one row per order, current state | Successful / pending / cancelled orders and value |
| `orders_status_history` (snapshot) | one row per version of an order | SCD Type 2 history, `timestamp` strategy on the change event time |
| `fct_order_status_daily` (table) | one row per order per day | Status at the end of each day (UTC) |

Status and metric definitions, including the limits of the daily history: [../docs/METRICS.md](../docs/METRICS.md).

## Tests

- `unique` and `not_null` on `order_id` in `stg_orders` and `fct_orders`
- `accepted_values` on the source status (warning) and on the business status (error)
- `assert_fct_orders_reconciles_with_source`: row count and total amount of `fct_orders` equal the Parquet snapshot
- `assert_daily_status_one_row_per_order_per_day`: grain check, no overlapping versions
- `assert_daily_status_matches_current_state`: on the last day, the daily table agrees with `fct_orders`

## How history is recorded

Each `dbt build` (or `dbt snapshot`) compares the current Parquet snapshot with the recorded history and closes and opens versions where an order changed. History therefore only grows when dbt runs: run it at least once a day. Running it more often records more intermediate changes; running it twice with the same data adds nothing.

## Run

dbt has its own virtual environment so that the consumer's tested dependency versions stay unchanged.

```powershell
cd analytics
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt

# With the bundled sample data (no Docker needed)
dbt build --vars "{orders_snapshot_path: sample_data/orders_snapshot_sample.parquet}"

# With the live snapshot written by the consumer (../orders_snapshot.parquet)
dbt build
```

`sample_data/orders_snapshot_sample.parquet` is a small hand-made file with the same schema as the consumer's export (7 orders). It is sample data, not output from a real run.

## Known limitations

- Daily history starts with the first `dbt snapshot` run and depends on how often dbt runs (see METRICS.md). Scheduling is manual for now.
- Deleting `analytics.duckdb` deletes the recorded history; it cannot be rebuilt from the Parquet snapshot.
- `dbt build` creates `fct_orders` before its tests run. If a test fails, the table already contains the new data; downstream models are skipped.
- Tested with Python 3.12, `dbt-core 1.12.7`, `dbt-duckdb 1.11.1`, `duckdb 2.0.1`.
