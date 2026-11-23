import base64
import json
import logging
import os
import tempfile
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import duckdb
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata, TopicPartition

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("lakehouse-cdc-consumer")

DEFAULT_BOOTSTRAP_SERVERS = ["127.0.0.1:9092"]
DEFAULT_TOPIC = "dbserver1.public.orders"
DEFAULT_GROUP_ID = "lakehouse-cdc-consumer-v1"
DEFAULT_DUCKDB_PATH = "lakehouse.duckdb"
DEFAULT_PARQUET_PATH = "orders_snapshot.parquet"


def decode_precise_decimal(value: Any, scale: int = 0) -> Decimal:
    """Decode Debezium precise decimal values (bytes, base64 string, int, or Decimal).

    With decimal.handling.mode=precise and the JSON converter, Debezium always sends
    the unscaled value as base64-encoded big-endian bytes. Strings are therefore
    decoded only as base64: a base64 value such as "1234" or "NAN0" must not be
    read as a plain number.
    """
    if value is None:
        raise ValueError("Decimal value cannot be None")

    scale_int = int(scale)
    quant = Decimal(1).scaleb(-scale_int)

    try:
        if isinstance(value, Decimal):
            return value.quantize(quant)

        if isinstance(value, bool):
            raise ValueError("Boolean is not a valid decimal value")

        if isinstance(value, int):
            unscaled = Decimal(value)
            if scale_int > 0:
                unscaled = unscaled.scaleb(-scale_int)
            return unscaled.quantize(quant)

        if isinstance(value, (bytes, bytearray)):
            raw_bytes = bytes(value)
        elif isinstance(value, str):
            raw_str = value.strip()
            if not raw_str:
                raise ValueError("Empty decimal string")
            raw_bytes = base64.b64decode(raw_str, validate=True)
        else:
            raise ValueError(f"Unsupported decimal representation: {type(value).__name__}")

        if len(raw_bytes) == 0:
            raise ValueError("Empty byte array for precise decimal")

        unscaled_int = int.from_bytes(raw_bytes, byteorder="big", signed=True)
        unscaled_dec = Decimal(unscaled_int).scaleb(-scale_int)
        return unscaled_dec.quantize(quant)
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"Failed to decode precise decimal value={value!r}, scale={scale!r}: {exc}") from exc


def init_duckdb(conn: duckdb.DuckDBPyConnection) -> None:
    """Initialize orders and cdc_offsets tables in DuckDB."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_id BIGINT PRIMARY KEY,
            customer_name VARCHAR NOT NULL,
            amount DECIMAL(18, 3) NOT NULL,
            status VARCHAR NOT NULL,
            created_at VARCHAR NOT NULL,
            updated_at VARCHAR NOT NULL,
            is_deleted BOOLEAN NOT NULL DEFAULT FALSE
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cdc_offsets (
            topic VARCHAR NOT NULL,
            partition_id INTEGER NOT NULL,
            last_applied_offset BIGINT NOT NULL,
            updated_at VARCHAR NOT NULL,
            PRIMARY KEY (topic, partition_id)
        );
        """
    )


def get_last_applied_offset(
    conn: duckdb.DuckDBPyConnection, topic: str, partition_id: int
) -> Optional[int]:
    """Return the last applied offset for a topic-partition, or None if not present."""
    row = conn.execute(
        """
        SELECT last_applied_offset
        FROM cdc_offsets
        WHERE topic = ? AND partition_id = ?
        """,
        [topic, partition_id],
    ).fetchone()
    if row is None:
        return None
    return int(row[0])


def upsert_last_applied_offset(
    conn: duckdb.DuckDBPyConnection, topic: str, partition_id: int, offset: int
) -> None:
    """Insert or update the last applied offset inside the active transaction."""
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    conn.execute(
        """
        INSERT INTO cdc_offsets (topic, partition_id, last_applied_offset, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (topic, partition_id) DO UPDATE SET
            last_applied_offset = excluded.last_applied_offset,
            updated_at = excluded.updated_at
        """,
        [topic, partition_id, int(offset), now_iso],
    )


def _extract_event_envelope(raw_value: Any) -> Optional[Dict[str, Any]]:
    """Parse raw Kafka message value and unwrap Debezium envelope if present."""
    if raw_value is None:
        return None

    if isinstance(raw_value, (bytes, bytearray)):
        text = raw_value.decode("utf-8").strip()
        if not text:
            return None
        payload = json.loads(text)
    elif isinstance(raw_value, str):
        text = raw_value.strip()
        if not text:
            return None
        payload = json.loads(text)
    elif isinstance(raw_value, dict):
        payload = raw_value
    else:
        raise ValueError(f"Unsupported message value type: {type(raw_value).__name__}")

    if payload is None:
        return None
    if not isinstance(payload, dict):
        raise ValueError("CDC message payload must be a JSON object")

    if "payload" in payload and isinstance(payload["payload"], dict):
        envelope = payload["payload"]
    else:
        envelope = payload

    if envelope is None:
        return None
    if not isinstance(envelope, dict):
        raise ValueError("Unwrapped CDC envelope must be a JSON object")

    return envelope


def _apply_mutation(conn: duckdb.DuckDBPyConnection, envelope: Dict[str, Any]) -> None:
    """Apply insert/snapshot (c/r), update (u), or delete (d) CDC mutation to orders."""
    op = envelope.get("op")
    if op not in ("c", "r", "u", "d"):
        raise ValueError(f"Unsupported CDC operation: {op!r}")

    event_ts_ms = envelope.get("ts_ms")
    if event_ts_ms is None and isinstance(envelope.get("source"), dict):
        event_ts_ms = envelope["source"].get("ts_ms")
    event_ts = str(event_ts_ms if event_ts_ms is not None else int(time.time() * 1000))

    if op in ("c", "r", "u"):
        after = envelope.get("after")
        if not isinstance(after, dict):
            raise ValueError(f"Missing 'after' object for op={op!r}")

        order_id = after.get("order_id")
        customer_name = after.get("customer_name")
        status = after.get("status")
        created_at = after.get("created_at")

        if order_id is None or customer_name is None or status is None or created_at is None:
            raise ValueError(f"Incomplete 'after' fields for op={op!r}: {after!r}")

        amount_scale = int(after.get("amount_scale", 3))
        amount = decode_precise_decimal(after.get("amount"), scale=amount_scale)

        existing = conn.execute(
            "SELECT created_at FROM orders WHERE order_id = ?",
            [int(order_id)],
        ).fetchone()
        effective_created_at = str(existing[0]) if existing is not None else str(created_at)

        conn.execute(
            """
            INSERT INTO orders (
                order_id, customer_name, amount, status, created_at, updated_at, is_deleted
            )
            VALUES (?, ?, ?, ?, ?, ?, FALSE)
            ON CONFLICT (order_id) DO UPDATE SET
                customer_name = excluded.customer_name,
                amount = excluded.amount,
                status = excluded.status,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                is_deleted = FALSE
            """,
            [
                int(order_id),
                str(customer_name),
                amount,
                str(status),
                effective_created_at,
                event_ts,
            ],
        )
        return

    # op == "d"
    before = envelope.get("before")
    order_id = before.get("order_id") if isinstance(before, dict) else None
    if order_id is None and isinstance(envelope.get("after"), dict):
        order_id = envelope["after"].get("order_id")
    if order_id is None:
        raise ValueError(f"Missing order_id for delete event: {envelope!r}")

    existing = conn.execute(
        "SELECT order_id, customer_name, amount, status, created_at FROM orders WHERE order_id = ?",
        [int(order_id)],
    ).fetchone()

    if existing is not None:
        conn.execute(
            """
            UPDATE orders
            SET status = 'DELETED',
                updated_at = ?,
                is_deleted = TRUE
            WHERE order_id = ?
            """,
            [event_ts, int(order_id)],
        )
    else:
        conn.execute(
            """
            INSERT INTO orders (
                order_id, customer_name, amount, status, created_at, updated_at, is_deleted
            )
            VALUES (?, ?, ?, 'DELETED', ?, ?, TRUE)
            """,
            [
                int(order_id),
                "",
                Decimal("0.000"),
                event_ts,
                event_ts,
            ],
        )


def export_parquet_atomic(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> None:
    """Export orders table to a Parquet file atomically via temporary file and os.replace."""
    target_path = Path(parquet_path)
    target_path.parent.mkdir(parents=True, exist_ok=True)

    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{target_path.stem}_",
        suffix=".parquet.tmp",
        dir=str(target_path.parent),
    )
    os.close(fd)
    tmp_path = Path(tmp_name)
    # DuckDB COPY TO fails if target temp file already exists with zero bytes
    if tmp_path.exists():
        tmp_path.unlink()

    escaped_tmp = str(tmp_path).replace("'", "''")
    try:
        conn.execute(
            f"""
            COPY (
                SELECT
                    order_id,
                    customer_name,
                    amount,
                    status,
                    created_at,
                    updated_at,
                    is_deleted
                FROM orders
                ORDER BY order_id
            ) TO '{escaped_tmp}' (FORMAT PARQUET);
            """
        )
        if not tmp_path.exists():
            raise RuntimeError(f"Temporary Parquet file was not created: {tmp_path}")
        os.replace(str(tmp_path), str(target_path))
    except Exception:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
        raise


def process_single_message(
    conn: duckdb.DuckDBPyConnection,
    consumer: Any,
    topic: str,
    partition_id: int,
    offset: int,
    raw_value: Any,
    parquet_path: str = DEFAULT_PARQUET_PATH,
) -> bool:
    """Process one Kafka message transactionally, export Parquet, and commit Kafka offset."""
    last_applied = get_last_applied_offset(conn, topic, partition_id)
    if last_applied is not None and int(offset) <= last_applied:
        logger.info(
            "Skipping already applied offset topic=%s partition=%s offset=%s (last_applied=%s)",
            topic,
            partition_id,
            offset,
            last_applied,
        )
        consumer.commit({TopicPartition(topic, partition_id): OffsetAndMetadata(int(offset) + 1, "")})
        return False

    envelope = _extract_event_envelope(raw_value)

    conn.execute("BEGIN TRANSACTION;")
    try:
        if envelope is not None:
            _apply_mutation(conn, envelope)
        else:
            logger.info(
                "Received tombstone message at topic=%s partition=%s offset=%s; advancing offset only",
                topic,
                partition_id,
                offset,
            )

        upsert_last_applied_offset(conn, topic, partition_id, int(offset))
        conn.execute("COMMIT;")
    except Exception:
        try:
            conn.execute("ROLLBACK;")
        except Exception:
            pass
        raise

    export_parquet_atomic(conn, parquet_path)

    tp = TopicPartition(topic, partition_id)
    offsets_to_commit = {tp: OffsetAndMetadata(int(offset) + 1, "")}
    try:
        consumer.commit(offsets_to_commit)
    except KafkaError as exc:
        raise RuntimeError(f"Failed to commit Kafka offset {topic}[{partition_id}]={int(offset)+1}: {exc}") from exc

    logger.info(
        "Committed Kafka offset %s[%s]->%s (last_applied_offset=%s) and refreshed Parquet snapshot.",
        topic,
        partition_id,
        int(offset) + 1,
        int(offset),
    )
    return True


def build_consumer(
    bootstrap_servers: Optional[list[str]] = None,
    group_id: str = DEFAULT_GROUP_ID,
    topic: str = DEFAULT_TOPIC,
) -> KafkaConsumer:
    """Create a KafkaConsumer configured for manual transactional offset commits."""
    servers = bootstrap_servers or DEFAULT_BOOTSTRAP_SERVERS
    consumer = KafkaConsumer(
        bootstrap_servers=servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: v,
        api_version=(2, 6, 0),
    )
    consumer.subscribe([topic])
    return consumer


def run_consumer_loop(
    duckdb_path: str = DEFAULT_DUCKDB_PATH,
    parquet_path: str = DEFAULT_PARQUET_PATH,
    bootstrap_servers: Optional[list[str]] = None,
    group_id: str = DEFAULT_GROUP_ID,
    topic: str = DEFAULT_TOPIC,
    poll_timeout_ms: int = 1000,
) -> None:
    """Run the CDC consumer loop with DuckDB persistence and atomic Parquet exports."""
    conn = duckdb.connect(duckdb_path)
    init_duckdb(conn)
    # Rebuild the snapshot from committed DuckDB state on every (re)start. If a previous
    # run committed to DuckDB but failed to export Parquet, the redelivered message is
    # skipped by the local offset check, so this is where the snapshot catches up.
    export_parquet_atomic(conn, parquet_path)
    consumer = build_consumer(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        topic=topic,
    )

    try:
        while True:
            records = consumer.poll(timeout_ms=poll_timeout_ms)
            if not records:
                continue

            for tp, messages in records.items():
                for msg in messages:
                    process_single_message(
                        conn=conn,
                        consumer=consumer,
                        topic=tp.topic,
                        partition_id=tp.partition,
                        offset=msg.offset,
                        raw_value=msg.value,
                        parquet_path=parquet_path,
                    )
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def main() -> None:
    """Entry point with resilience loop for transient errors."""
    duckdb_path = os.getenv("DUCKDB_PATH", DEFAULT_DUCKDB_PATH)
    parquet_path = os.getenv("PARQUET_PATH", DEFAULT_PARQUET_PATH)
    topic = os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC)
    group_id = os.getenv("KAFKA_GROUP_ID", DEFAULT_GROUP_ID)
    servers_env = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")
    bootstrap_servers = (
        [s.strip() for s in servers_env.split(",") if s.strip()]
        if servers_env.strip()
        else DEFAULT_BOOTSTRAP_SERVERS
    )

    while True:
        try:
            run_consumer_loop(
                duckdb_path=duckdb_path,
                parquet_path=parquet_path,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                topic=topic,
            )
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user; shutting down cleanly.")
            break
        except Exception as exc:
            logger.error("Consumer crashed, retrying in 3s: %s", exc)
            time.sleep(3)


if __name__ == "__main__":
    main()