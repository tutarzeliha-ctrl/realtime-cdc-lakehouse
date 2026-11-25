# Project Status

## Releases

- **v1.0** — CDC layer: PostgreSQL → Debezium → Kafka → Python consumer → DuckDB + Parquet.
- **v1.1** — Analytics layer (`analytics/`): dbt models on top of the Parquet snapshot.
- **v1.2** — Orchestration and monitoring (`orchestration/`): Airflow, freshness and connector checks, source reconciliation, alerts.
- **v1.3** — Continuous integration: GitHub Actions runs `pytest` and `dbt build` on sample data for every push and pull request.

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

Orchestration layer (live on Windows, Airflow in Docker):
- Quiet period stays green; a stopped consumer with a pending order turns the freshness check red, and it recovers by itself once the consumer catches up.
- `daily_analytics` passes end to end; PostgreSQL and the snapshot agree (2 orders, 175.000).
- Live test found a blind spot (stopped Debezium looks healthy); a connector check was added and verified live (Connect stopped → red, started → green).

## Decisions

- Active files: `lakehouse_consumer.py`, `postgres-connector.json`; older consumer and connector files removed.
- Kafka committed offset decides where to resume; `cdc_offsets.last_applied_offset` prevents duplicate application.
- Parquet snapshot is rebuilt from DuckDB on every (re)start.
- Precise-mode decimals: strings are decoded only as base64; scale fixed at 3.
- dbt reads the Parquet snapshot (not `lakehouse.duckdb`) and writes to its own `analytics.duckdb`, so both can run at the same time.
- Business status: CREATED → pending, PAID → successful, deleted → cancelled (see docs/METRICS.md).
- Daily history uses a dbt snapshot with the `timestamp` strategy on the change event time.
- Stale data = consumer lag > 0 AND Parquet snapshot older than 10 minutes, so quiet periods do not raise alerts.
- Airflow runs dbt in a separate virtual environment, with its own target folder and no partial parsing; alerts go to the log and, if configured, to Slack.
- A stopped Debezium connector is a failure in its own right, checked through the Kafka Connect REST API.
- CI runs only what needs no Docker stack (unit tests, dbt on sample data); the live pipeline is verified by hand. CI is pinned to Python 3.12 because `pyarrow 15.0.0` has no wheels for 3.13.

## Known limitations

See "Known limitations" in README.md and analytics/README.md. The main ones: one partition and one consumer; no dead-letter queue; daily history starts with the first snapshot run and depends on how often dbt runs.

## Roadmap

- **v1.3 CI:** done. Both jobs green on push; a deliberately broken test turned CI red (CI #2) and the revert turned it green (CI #3). See docs/VERIFICATION.md, section 10.
- **v1.4:** dead-letter handling for poison messages.
- **v1.5:** replication slot lag monitoring.

## Next step

v1.4: decide how the consumer handles a message it cannot decode or apply (dead-letter handling).
