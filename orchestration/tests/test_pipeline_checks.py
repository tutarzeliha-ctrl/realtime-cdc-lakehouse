import os
import sys
import time
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))

import alerts  # noqa: E402
import pipeline_checks as checks  # noqa: E402

TEN_MIN = 600


# --- freshness rule --------------------------------------------------------

def test_quiet_period_is_not_stale_even_with_old_snapshot():
    # No new orders for hours: lag is 0, the snapshot is old, and that is fine.
    result = checks.evaluate_freshness(lag=0, snapshot_age_seconds=3 * 3600, max_age_seconds=TEN_MIN)
    assert result.stale is False


def test_stopped_consumer_is_stale():
    result = checks.evaluate_freshness(lag=340, snapshot_age_seconds=48 * 3600, max_age_seconds=TEN_MIN)
    assert result.stale is True
    assert "340 unprocessed" in result.reason


def test_burst_being_processed_is_not_stale():
    # Messages are waiting, but the consumer published a snapshot a minute ago.
    result = checks.evaluate_freshness(lag=25, snapshot_age_seconds=60, max_age_seconds=TEN_MIN)
    assert result.stale is False


def test_messages_but_no_snapshot_is_stale():
    result = checks.evaluate_freshness(lag=3, snapshot_age_seconds=None, max_age_seconds=TEN_MIN)
    assert result.stale is True


def test_negative_lag_is_rejected():
    with pytest.raises(ValueError):
        checks.evaluate_freshness(lag=-1, snapshot_age_seconds=0, max_age_seconds=TEN_MIN)


# --- lag calculation -------------------------------------------------------

def test_lag_uses_committed_offset_per_partition():
    lag = checks.compute_lag(
        end_offsets={0: 25, 1: 10},
        beginning_offsets={0: 0, 1: 0},
        committed_offsets={0: 25, 1: 7},
    )
    assert lag == 3


def test_lag_for_never_committed_partition_starts_at_beginning():
    # A consumer group that has never committed (e.g. first start) has all retained messages pending.
    lag = checks.compute_lag(end_offsets={0: 25}, beginning_offsets={0: 5}, committed_offsets={})
    assert lag == 20


# --- snapshot age ----------------------------------------------------------

def test_snapshot_age_from_file_modification_time(tmp_path):
    f = tmp_path / "orders_snapshot.parquet"
    f.write_bytes(b"x")
    old = time.time() - 900
    os.utime(f, (old, old))
    assert checks.get_snapshot_age_seconds(str(f)) == pytest.approx(900, abs=5)


def test_missing_snapshot_has_no_age(tmp_path):
    assert checks.get_snapshot_age_seconds(str(tmp_path / "missing.parquet")) is None


# --- source reconciliation -------------------------------------------------

def test_reconciliation_passes_when_totals_match():
    source = checks.Totals(2, Decimal("175.000"))
    assert checks.evaluate_reconciliation(source, checks.Totals(2, Decimal("175.000"))) == []


def test_reconciliation_reports_count_and_amount_differences():
    problems = checks.evaluate_reconciliation(checks.Totals(3, Decimal("200.000")), checks.Totals(2, Decimal("175.000")))
    assert len(problems) == 2


def test_snapshot_totals_ignore_soft_deleted_rows(tmp_path):
    path = tmp_path / "orders_snapshot.parquet"
    con = duckdb.connect()
    con.execute("CREATE TABLE o (order_id BIGINT, amount DECIMAL(18,3), is_deleted BOOLEAN)")
    con.execute("INSERT INTO o VALUES (1, 25.500, FALSE), (2, 149.500, FALSE), (3, 210.750, TRUE)")
    con.execute(f"COPY o TO '{path.as_posix()}' (FORMAT PARQUET)")
    con.close()

    totals = checks.fetch_snapshot_totals(str(path))
    assert totals == checks.Totals(2, Decimal("175.000"))


# --- alerts ----------------------------------------------------------------

def test_alert_without_webhook_only_logs(monkeypatch, caplog):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    called = []
    monkeypatch.setattr(alerts.urllib.request, "urlopen", lambda *a, **k: called.append(a))

    class TI:
        dag_id, task_id, run_id = "cdc_freshness_check", "consumer_is_up_to_date", "manual__1"

    alerts.notify_failure({"task_instance": TI(), "exception": "Stale data"})
    assert called == []
    assert "Stale data" in caplog.text


def test_alert_with_webhook_posts_message(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.example.invalid/x")
    sent = []
    monkeypatch.setattr(alerts.urllib.request, "urlopen", lambda req, timeout: sent.append(req.data))
    alerts.notify_failure({"task_instance": None, "exception": "Stale data"})
    assert sent and b"Stale data" in sent[0]


# --- Debezium connector ----------------------------------------------------

RUNNING = {"name": "orders-cdc-connector", "connector": {"state": "RUNNING"}, "tasks": [{"id": 0, "state": "RUNNING"}]}


def test_running_connector_has_no_problems():
    assert checks.evaluate_connector_status(RUNNING) == []


def test_unreachable_connect_is_reported():
    # What happened in the live test: the connect container had stopped, lag stayed 0.
    problems = checks.evaluate_connector_status(None)
    assert problems and "did not respond" in problems[0]


def test_failed_task_is_reported_with_first_trace_line():
    status = {"name": "orders-cdc-connector", "connector": {"state": "RUNNING"},
              "tasks": [{"id": 0, "state": "FAILED", "trace": "org.apache.kafka.connect.errors.ConnectException: slot lost\n\tat ..."}]}
    problems = checks.evaluate_connector_status(status)
    assert problems == ["Task 0 is FAILED: org.apache.kafka.connect.errors.ConnectException: slot lost"]


def test_paused_connector_and_missing_tasks_are_reported():
    status = {"name": "orders-cdc-connector", "connector": {"state": "PAUSED"}, "tasks": []}
    assert len(checks.evaluate_connector_status(status)) == 2


def test_fetch_connector_status_returns_none_when_connect_is_down():
    assert checks.fetch_connector_status("http://127.0.0.1:9", "orders-cdc-connector", timeout=1) is None
