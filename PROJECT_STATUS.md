# Project Status

## Releases

- **v1.0** — CDC layer: PostgreSQL → Debezium → Kafka → Python consumer → DuckDB + Parquet.
- **v1.1** — Analytics layer (`analytics/`): dbt models on top of the Parquet snapshot.

## Current state

The pipeline runs end to end on Windows: source changes reach DuckDB and Parquet, and dbt builds the current-state and daily-status models from the Parquet snapshot.

## Verified behaviour (evidence: docs/VERIFICATION.md)

CDC layer:
- 13 unit / failure-scenario tests pass on Windows, Python 3.12.0.
- Live INSERT → UPDATE → DELETE reaches DuckDB and Parquet with exact decimals and soft delete.
- Restart does not re-apply messages; forced replay from offset 0 skips all already applied messages.
- Rebuild procedure (rename DuckDB file + rewind consumer group) restores state from Kafka history.
- Clean install from GitHub with only the README steps works.

Analytics layer:
- `dbt build` passes on the bundled sample data and on the live snapshot on Windows (20/20).
- `fct_orders` reconciles with the Parquet snapshot (row count and total amount).
- On the last day, `fct_order_status_daily` agrees with `fct_orders`.
- Daily logic tested with a three-day simulated scenario; reruns with unchanged data add no history.

## Decisions

- Active files: `lakehouse_consumer.py`, `postgres-connector.json`; older consumer and connector files removed.
- Kafka committed offset decides where to resume; `cdc_offsets.last_applied_offset` prevents duplicate application.
- Parquet snapshot is rebuilt from DuckDB on every (re)start.
- Precise-mode decimals: strings are decoded only as base64; scale fixed at 3.
- dbt reads the Parquet snapshot (not `lakehouse.duckdb`) and writes to its own `analytics.duckdb`, so both can run at the same time.
- Business status: CREATED → pending, PAID → successful, deleted → cancelled (see docs/METRICS.md).
- Daily history uses a dbt snapshot with the `timestamp` strategy on the change event time.

## Known limitations

See "Known limitations" in README.md and analytics/README.md. The main ones: one partition and one consumer; no dead-letter queue; daily history starts with the first snapshot run and depends on how often dbt runs.

## Next step

Schedule `dbt build` daily and add freshness checks and alerts (pipeline operations).
