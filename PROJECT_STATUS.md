# Project Status

## Current milestone

Project 1 — Real-Time CDC Lakehouse v1.0 (released).

Project 2 — Order Analytics (dbt):
- Milestone 1 (done): `stg_orders` and `fct_orders` (one row per order, current state). `dbt build` passes on the sample data and on the live snapshot on Windows (13/13).
- Milestone 2 (in progress): `orders_status_history` snapshot and `fct_order_status_daily` (one row per order per day). Tested with a three-day simulated scenario (20/20 per run); not yet run on Windows.

## Verified behaviour (evidence: docs/VERIFICATION.md)

- 13 unit / failure-scenario tests pass on Windows, Python 3.12.0.
- Live INSERT → UPDATE → DELETE reaches DuckDB and Parquet with exact decimals and soft delete.
- Restart does not re-apply messages.
- Forced replay from offset 0 skips all 10 already applied messages.
- Rebuild procedure (rename DuckDB file + rewind consumer group) restores state from Kafka history.
- Clean install from GitHub with only the README steps: 13 tests pass, seed row reaches DuckDB.

## Decisions

- Active files: `lakehouse_consumer.py`, `postgres-connector.json`; older consumer and connector files removed.
- Kafka committed offset decides where to resume; `cdc_offsets.last_applied_offset` prevents duplicate application.
- Parquet snapshot is rebuilt from DuckDB on every (re)start.
- Precise-mode decimals: strings are decoded only as base64; scale fixed at 3.
- Dependency: `kafka-python-ng 2.2.3` (`kafka-python 2.0.2` fails on Python 3.12).

## Known limitations

See the "Known limitations" section in README.md.

## Next step

Run milestone 2 on Windows against the live snapshot, then record a change on a later day and confirm the daily table shows both days.
