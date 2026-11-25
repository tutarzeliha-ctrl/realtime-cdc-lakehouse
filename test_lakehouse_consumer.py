import base64
import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest
from kafka.structs import OffsetAndMetadata, TopicPartition

import lakehouse_consumer as consumer


@dataclass
class FakeMessage:
    topic: str
    partition: int
    offset: int
    value: bytes | None


def _wrap_debezium_payload(payload: dict) -> bytes:
    envelope = {
        "schema": {"type": "struct", "optional": False, "name": "test.Envelope"},
        "payload": payload,
    }
    return json.dumps(envelope).encode("utf-8")


def _encode_precise_b64(val: Decimal, scale: int = 3) -> str:
    scaled = int((val * (Decimal(10) ** scale)).to_integral_value())
    byte_len = max(1, (scaled.bit_length() + 8) // 8)
    raw = scaled.to_bytes(byte_len, byteorder="big", signed=True)
    return base64.b64encode(raw).decode("ascii")


@pytest.fixture
def db_conn(tmp_path: Path):
    db_file = tmp_path / "test_lakehouse.duckdb"
    conn = duckdb.connect(str(db_file))
    consumer.init_duckdb(conn)
    try:
        yield conn
    finally:
        conn.close()


def test_decode_precise_decimal_variants():
    with pytest.raises(ValueError):
        consumer.decode_precise_decimal(None, scale=3)

    # Plain numeric strings are not part of the precise-mode contract.
    with pytest.raises(ValueError):
        consumer.decode_precise_decimal("125.500", scale=3)
    assert consumer.decode_precise_decimal(75500, scale=3) == Decimal("75.500")

    b64_val = _encode_precise_b64(Decimal("49.750"), scale=3)
    assert consumer.decode_precise_decimal(b64_val, scale=3) == Decimal("49.750")

    raw_bytes = base64.b64decode(_encode_precise_b64(Decimal("12.345"), scale=3))
    assert consumer.decode_precise_decimal(raw_bytes, scale=3) == Decimal("12.345")

    with pytest.raises(ValueError):
        consumer.decode_precise_decimal(320.5, scale=3)


def test_tombstone_is_ignored_safely(db_conn, tmp_path: Path):
    parquet_file = tmp_path / "orders_snapshot.parquet"
    mock_consumer = MagicMock()

    applied = consumer.process_single_message(
        conn=db_conn,
        consumer=mock_consumer,
        topic="dbserver1.public.orders",
        partition_id=0,
        offset=0,
        raw_value=None,
        parquet_path=str(parquet_file),
    )

    assert applied is True
    assert db_conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 0
    assert db_conn.execute(
        "SELECT last_applied_offset FROM cdc_offsets WHERE partition_id = 0"
    ).fetchall() == [(0,)]
    mock_consumer.commit.assert_called_once_with(
        {TopicPartition("dbserver1.public.orders", 0): OffsetAndMetadata(1, "")}
    )


def test_snapshot_create_update_delete_and_parquet_export(db_conn, tmp_path: Path):
    parquet_file = tmp_path / "orders_snapshot.parquet"

    snap_env = {
        "op": "r",
        "ts_ms": 1795126206939,
        "source": {"ts_ms": 1795126206513},
        "after": {
            "order_id": 10,
            "customer_name": "Zeynep Demir",
            "amount": _encode_precise_b64(Decimal("100.000"), scale=3),
            "status": "CREATED",
            "created_at": "2026-11-20T01:00:00Z",
        },
    }
    consumer._apply_mutation(db_conn, snap_env)
    consumer.export_parquet_atomic(db_conn, str(parquet_file))
    assert duckdb.sql(
        f"SELECT order_id, customer_name, amount, status, is_deleted FROM read_parquet('{parquet_file}')"
    ).fetchall() == [(10, "Zeynep Demir", Decimal("100.000"), "CREATED", False)]

    upd_env = {
        "op": "u",
        "ts_ms": 1795126208000,
        "source": {"ts_ms": 1795126207500},
        "before": {"order_id": 10},
        "after": {
            "order_id": 10,
            "customer_name": "Zeynep Demir",
            "amount": _encode_precise_b64(Decimal("185.250"), scale=3),
            "status": "PAID",
            "created_at": "2026-11-20T01:00:00Z",
        },
    }
    consumer._apply_mutation(db_conn, upd_env)
    consumer.export_parquet_atomic(db_conn, str(parquet_file))
    assert duckdb.sql(
        f"SELECT order_id, amount, status, is_deleted FROM read_parquet('{parquet_file}')"
    ).fetchall() == [(10, Decimal("185.250"), "PAID", False)]

    del_env = {
        "op": "d",
        "ts_ms": 1795126209000,
        "source": {"ts_ms": 1795126208500},
        "before": {"order_id": 10},
        "after": None,
    }
    consumer._apply_mutation(db_conn, del_env)
    consumer.export_parquet_atomic(db_conn, str(parquet_file))
    assert duckdb.sql(
        f"SELECT order_id, status, is_deleted FROM read_parquet('{parquet_file}')"
    ).fetchall() == [(10, "DELETED", True)]


def test_idempotent_replay_and_last_applied_offset_tracking(db_conn):
    create_env = {
        "op": "c",
        "ts_ms": 1795126210500,
        "source": {"ts_ms": 1795126210000},
        "after": {
            "order_id": 42,
            "customer_name": "Can Yilmaz",
            "amount": _encode_precise_b64(Decimal("75.500"), scale=3),
            "status": "CREATED",
            "created_at": "2026-11-20T01:10:00Z",
        },
    }
    for _ in range(3):
        consumer._apply_mutation(db_conn, create_env)

    consumer.upsert_last_applied_offset(db_conn, "dbserver1.public.orders", 0, 5)
    consumer.upsert_last_applied_offset(db_conn, "dbserver1.public.orders", 0, 5)

    assert db_conn.execute("SELECT order_id, amount FROM orders").fetchall() == [
        (42, Decimal("75.500"))
    ]
    assert db_conn.execute(
        "SELECT topic, partition_id, last_applied_offset FROM cdc_offsets"
    ).fetchall() == [("dbserver1.public.orders", 0, 5)]


def test_malformed_events_raise_errors(db_conn):
    with pytest.raises(json.JSONDecodeError):
        consumer._extract_event_envelope(b"{invalid-json")

    with pytest.raises(ValueError):
        consumer._apply_mutation(
            db_conn,
            {"op": "c", "after": {"customer_name": "Test", "status": "CREATED", "created_at": "2026-11-20T01:00:00Z"}},
        )

    with pytest.raises(ValueError):
        consumer._apply_mutation(
            db_conn,
            {
                "op": "c",
                "after": {
                    "order_id": 99,
                    "customer_name": "Test",
                    "amount": None,
                    "status": "CREATED",
                    "created_at": "2026-11-20T01:00:00Z",
                },
            },
        )


def test_multi_message_poll_commits_exact_partition_and_next_offset(db_conn, tmp_path: Path):
    parquet_file = tmp_path / "orders_snapshot.parquet"
    mock_consumer = MagicMock()

    tp0 = TopicPartition("dbserver1.public.orders", 0)
    tp1 = TopicPartition("dbserver1.public.orders", 1)

    polled_batch = {
        tp0: [
            FakeMessage(
                "dbserver1.public.orders",
                0,
                10,
                _wrap_debezium_payload(
                    {
                        "op": "c",
                        "after": {
                            "order_id": 101,
                            "customer_name": "Ali Veli",
                            "amount": _encode_precise_b64(Decimal("50.000"), scale=3),
                            "status": "CREATED",
                            "created_at": "2026-11-20T01:20:00Z",
                        },
                    }
                ),
            ),
            FakeMessage(
                "dbserver1.public.orders",
                0,
                11,
                _wrap_debezium_payload(
                    {
                        "op": "u",
                        "after": {
                            "order_id": 101,
                            "customer_name": "Ali Veli",
                            "amount": _encode_precise_b64(Decimal("65.000"), scale=3),
                            "status": "PAID",
                            "created_at": "2026-11-20T01:20:00Z",
                        },
                    }
                ),
            ),
        ],
        tp1: [
            FakeMessage(
                "dbserver1.public.orders",
                1,
                3,
                _wrap_debezium_payload(
                    {
                        "op": "c",
                        "after": {
                            "order_id": 202,
                            "customer_name": "Ece Kara",
                            "amount": _encode_precise_b64(Decimal("120.000"), scale=3),
                            "status": "CREATED",
                            "created_at": "2026-11-20T01:21:00Z",
                        },
                    }
                ),
            )
        ],
    }

    for tp, batch in polled_batch.items():
        for msg in batch:
            consumer.process_single_message(
                conn=db_conn,
                consumer=mock_consumer,
                topic=tp.topic,
                partition_id=tp.partition,
                offset=msg.offset,
                raw_value=msg.value,
                parquet_path=str(parquet_file),
            )

    assert mock_consumer.commit.call_count == 3
    committed_entries = [
        (k.topic, k.partition, v.offset)
        for call in mock_consumer.commit.call_args_list
        for offset_map in call.args
        for k, v in offset_map.items()
    ]
    assert committed_entries == [
        ("dbserver1.public.orders", 0, 11),
        ("dbserver1.public.orders", 0, 12),
        ("dbserver1.public.orders", 1, 4),
    ]


def test_transaction_rollback_on_mutation_failure_skips_commit(db_conn, tmp_path: Path):
    parquet_file = tmp_path / "orders_snapshot.parquet"
    mock_consumer = MagicMock()

    consumer.process_single_message(
        conn=db_conn,
        consumer=mock_consumer,
        topic="dbserver1.public.orders",
        partition_id=0,
        offset=0,
        raw_value=_wrap_debezium_payload(
            {
                "op": "c",
                "after": {
                    "order_id": 1,
                    "customer_name": "Initial",
                    "amount": _encode_precise_b64(Decimal("10.000"), scale=3),
                    "status": "CREATED",
                    "created_at": "2026-11-20T01:30:00Z",
                },
            }
        ),
        parquet_path=str(parquet_file),
    )
    mock_consumer.commit.reset_mock()

    with pytest.raises(ValueError):
        consumer.process_single_message(
            conn=db_conn,
            consumer=mock_consumer,
            topic="dbserver1.public.orders",
            partition_id=0,
            offset=1,
            raw_value=_wrap_debezium_payload(
                {
                    "op": "c",
                    "after": {
                        "order_id": 2,
                        "customer_name": "Broken",
                        "amount": None,
                        "status": "CREATED",
                        "created_at": "2026-11-20T01:31:00Z",
                    },
                }
            ),
            parquet_path=str(parquet_file),
        )

    mock_consumer.commit.assert_not_called()
    assert db_conn.execute("SELECT order_id FROM orders ORDER BY order_id").fetchall() == [(1,)]
    assert db_conn.execute(
        "SELECT last_applied_offset FROM cdc_offsets WHERE partition_id = 0"
    ).fetchall() == [(0,)]


def test_parquet_export_failure_skips_kafka_commit(db_conn, monkeypatch, tmp_path: Path):
    mock_consumer = MagicMock()
    monkeypatch.setattr(
        consumer,
        "export_parquet_atomic",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("Simulated Parquet export failure")),
    )

    with pytest.raises(OSError, match="Simulated Parquet export failure"):
        consumer.process_single_message(
            conn=db_conn,
            consumer=mock_consumer,
            topic="dbserver1.public.orders",
            partition_id=0,
            offset=7,
            raw_value=_wrap_debezium_payload(
                {
                    "op": "c",
                    "after": {
                        "order_id": 5,
                        "customer_name": "Export Fail",
                        "amount": _encode_precise_b64(Decimal("30.000"), scale=3),
                        "status": "CREATED",
                        "created_at": "2026-11-20T01:40:00Z",
                    },
                }
            ),
            parquet_path=str(tmp_path / "orders_snapshot.parquet"),
        )

    mock_consumer.commit.assert_not_called()


def test_interruption_before_kafka_commit_leaves_kafka_offset_uncommitted(db_conn, tmp_path: Path):
    parquet_file = tmp_path / "orders_snapshot.parquet"
    mock_consumer = MagicMock()
    mock_consumer.commit.side_effect = RuntimeError("Crash before Kafka commit")

    with pytest.raises(RuntimeError, match="Crash before Kafka commit"):
        consumer.process_single_message(
            conn=db_conn,
            consumer=mock_consumer,
            topic="dbserver1.public.orders",
            partition_id=0,
            offset=4,
            raw_value=_wrap_debezium_payload(
                {
                    "op": "c",
                    "after": {
                        "order_id": 8,
                        "customer_name": "Pre-Commit Crash",
                        "amount": _encode_precise_b64(Decimal("90.000"), scale=3),
                        "status": "CREATED",
                        "created_at": "2026-11-20T01:45:00Z",
                    },
                }
            ),
            parquet_path=str(parquet_file),
        )

    assert db_conn.execute(
        "SELECT last_applied_offset FROM cdc_offsets WHERE partition_id = 0"
    ).fetchall() == [(4,)]
    assert parquet_file.exists()


def test_offset_upsert_failure_rolls_back_mutation_and_skips_commit(
    db_conn, monkeypatch, tmp_path: Path
):
    parquet_file = tmp_path / "orders_snapshot.parquet"
    mock_consumer = MagicMock()

    msg = _wrap_debezium_payload(
        {
            "op": "c",
            "ts_ms": 1795126214000,
            "source": {"ts_ms": 1795126213500},
            "after": {
                "order_id": 77,
                "customer_name": "Rollback Check",
                "amount": _encode_precise_b64(Decimal("55.250"), scale=3),
                "status": "CREATED",
                "created_at": "2026-11-20T01:50:00Z",
            },
        }
    )

    def _fail_offset(*args, **kwargs):
        raise RuntimeError("Simulated offset write failure")

    monkeypatch.setattr(consumer, "upsert_last_applied_offset", _fail_offset)

    with pytest.raises(RuntimeError, match="Simulated offset write failure"):
        consumer.process_single_message(
            conn=db_conn,
            consumer=mock_consumer,
            topic="dbserver1.public.orders",
            partition_id=0,
            offset=9,
            raw_value=msg,
            parquet_path=str(parquet_file),
        )

    mock_consumer.commit.assert_not_called()
    assert db_conn.execute("SELECT * FROM orders WHERE order_id = 77").fetchall() == []
    assert db_conn.execute("SELECT * FROM cdc_offsets").fetchall() == []

class _StopLoop(Exception):
    pass


def _create_msg(order_id: int, amount: str) -> bytes:
    return _wrap_debezium_payload(
        {
            "op": "c",
            "after": {
                "order_id": order_id,
                "customer_name": f"Customer {order_id}",
                "amount": _encode_precise_b64(Decimal(amount), scale=3),
                "status": "CREATED",
                "created_at": "2026-11-20T02:00:00Z",
            },
        }
    )


def _parquet_order_ids(parquet_file: Path) -> list:
    return duckdb.sql(
        f"SELECT order_id FROM read_parquet('{parquet_file}') ORDER BY order_id"
    ).fetchall()


def test_restart_refreshes_stale_parquet_after_export_failure(monkeypatch, tmp_path: Path):
    """DuckDB commit succeeds, Parquet export fails, the message is redelivered and
    skipped by the local offset check. The snapshot must be rebuilt on (re)start."""
    db_file = tmp_path / "lakehouse.duckdb"
    parquet_file = tmp_path / "orders_snapshot.parquet"
    topic = "dbserver1.public.orders"
    mock_consumer = MagicMock()

    conn = duckdb.connect(str(db_file))
    consumer.init_duckdb(conn)
    consumer.process_single_message(conn, mock_consumer, topic, 0, 0, _create_msg(1, "10.000"), str(parquet_file))

    real_export = consumer.export_parquet_atomic
    monkeypatch.setattr(
        consumer,
        "export_parquet_atomic",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("Simulated Parquet export failure")),
    )
    with pytest.raises(OSError):
        consumer.process_single_message(conn, mock_consumer, topic, 0, 1, _create_msg(2, "20.000"), str(parquet_file))
    monkeypatch.setattr(consumer, "export_parquet_atomic", real_export)

    # Kafka offset 1 was not committed, so Kafka redelivers it; the local offset skips it.
    applied = consumer.process_single_message(conn, mock_consumer, topic, 0, 1, _create_msg(2, "20.000"), str(parquet_file))
    assert applied is False
    assert conn.execute("SELECT order_id FROM orders ORDER BY order_id").fetchall() == [(1,), (2,)]
    assert _parquet_order_ids(parquet_file) == [(1,)]  # stale snapshot before restart
    conn.close()

    fake_kafka = MagicMock()
    fake_kafka.poll.side_effect = _StopLoop()
    monkeypatch.setattr(consumer, "build_consumer", lambda **kwargs: fake_kafka)

    with pytest.raises(_StopLoop):
        consumer.run_consumer_loop(duckdb_path=str(db_file), parquet_path=str(parquet_file))

    assert _parquet_order_ids(parquet_file) == [(1,), (2,)]


@pytest.mark.parametrize(
    "wire_value, expected",
    [
        ("1234", Decimal("-2658.825")),  # base64 that looks like a number
        ("NAN0", Decimal("3408.756")),  # base64 that Decimal() parses as NaN
    ],
)
def test_precise_decimal_strings_are_always_base64(wire_value, expected):
    assert consumer.decode_precise_decimal(wire_value, scale=3) == expected
