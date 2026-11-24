"""Health checks for the CDC pipeline.

The evaluate_* functions contain the decision logic and have no I/O, so they can be
unit-tested without Kafka, PostgreSQL or Airflow. The fetch/get functions read the
live systems and import their client libraries lazily.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional


# ---------------------------------------------------------------------------
# Freshness
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FreshnessResult:
    lag: int
    snapshot_age_seconds: Optional[float]
    stale: bool
    reason: str


def evaluate_freshness(lag: int, snapshot_age_seconds: Optional[float], max_age_seconds: float) -> FreshnessResult:
    """Decide whether the published data is stale.

    Stale means: there are unprocessed messages in Kafka (lag > 0) AND the consumer
    has not published a snapshot for longer than max_age_seconds. A quiet period with
    no new orders (lag == 0) is never stale, however old the snapshot is.
    """
    if lag < 0:
        raise ValueError(f"Consumer lag cannot be negative: {lag}")

    if lag == 0:
        return FreshnessResult(lag, snapshot_age_seconds, False, "No unprocessed messages; the consumer is up to date.")

    if snapshot_age_seconds is None:
        return FreshnessResult(lag, None, True, f"{lag} unprocessed message(s) and no Parquet snapshot has been published.")

    if snapshot_age_seconds > max_age_seconds:
        return FreshnessResult(
            lag, snapshot_age_seconds, True,
            f"{lag} unprocessed message(s) and the snapshot was last published "
            f"{snapshot_age_seconds / 60:.1f} min ago (limit {max_age_seconds / 60:.0f} min). "
            "The consumer has probably stopped.",
        )

    return FreshnessResult(
        lag, snapshot_age_seconds, False,
        f"{lag} message(s) in progress; snapshot published {snapshot_age_seconds / 60:.1f} min ago.",
    )


def get_snapshot_age_seconds(path: str, now: Optional[float] = None) -> Optional[float]:
    """Seconds since the Parquet snapshot was last replaced, or None if it does not exist.

    The consumer rewrites the snapshot after every applied message, so its modification
    time is the time of the last applied change.
    """
    try:
        modified = os.stat(path).st_mtime
    except FileNotFoundError:
        return None
    return max(0.0, (time.time() if now is None else now) - modified)


def get_consumer_lag(bootstrap_servers: str, group_id: str, topic: str) -> int:
    """Total number of messages in the topic that the consumer group has not committed."""
    from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers, enable_auto_commit=False)
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise RuntimeError(f"Topic {topic!r} not found or has no partitions.")
        tps = [TopicPartition(topic, p) for p in sorted(partitions)]
        end = consumer.end_offsets(tps)
        beginning = consumer.beginning_offsets(tps)
        committed = admin.list_consumer_group_offsets(group_id)
        return compute_lag(
            end_offsets={tp.partition: end[tp] for tp in tps},
            beginning_offsets={tp.partition: beginning[tp] for tp in tps},
            committed_offsets={tp.partition: meta.offset for tp, meta in committed.items() if tp.topic == topic},
        )
    finally:
        consumer.close()
        admin.close()


def compute_lag(end_offsets: dict, beginning_offsets: dict, committed_offsets: dict) -> int:
    """Lag per partition is end offset minus committed offset.

    A partition the group has never committed starts from the beginning offset
    (the consumer uses auto_offset_reset='earliest').
    """
    total = 0
    for partition, end in end_offsets.items():
        committed = committed_offsets.get(partition)
        if committed is None or committed < 0:
            committed = beginning_offsets.get(partition, 0)
        total += max(0, end - committed)
    return total


# ---------------------------------------------------------------------------
# Source reconciliation
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Totals:
    row_count: int
    total_amount: Decimal


def evaluate_reconciliation(source: Totals, snapshot: Totals) -> list[str]:
    """Compare live orders in PostgreSQL with non-deleted orders in the Parquet snapshot.

    PostgreSQL removes deleted orders; the snapshot keeps them as soft-deleted rows.
    Only non-deleted snapshot rows are therefore comparable. Returns a list of
    mismatch descriptions; an empty list means the two sides agree.
    """
    problems = []
    if source.row_count != snapshot.row_count:
        problems.append(f"Row count differs: PostgreSQL {source.row_count}, snapshot {snapshot.row_count}.")
    if source.total_amount != snapshot.total_amount:
        problems.append(f"Total amount differs: PostgreSQL {source.total_amount}, snapshot {snapshot.total_amount}.")
    return problems


def fetch_source_totals(host: str, port: int, dbname: str, user: str, password: str) -> Totals:
    import psycopg

    with psycopg.connect(host=host, port=port, dbname=dbname, user=user, password=password) as conn:
        row = conn.execute("SELECT count(*), coalesce(sum(amount), 0) FROM public.orders").fetchone()
    return Totals(int(row[0]), Decimal(row[1]))


def fetch_snapshot_totals(parquet_path: str) -> Totals:
    import duckdb

    con = duckdb.connect()
    try:
        row = con.execute(
            "SELECT count(*), coalesce(sum(amount), 0) FROM read_parquet(?) WHERE NOT is_deleted",
            [parquet_path],
        ).fetchone()
    finally:
        con.close()
    return Totals(int(row[0]), Decimal(row[1]))
