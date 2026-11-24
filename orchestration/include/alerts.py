"""Failure notifications for the pipeline DAGs.

Every failure is written to the task log. When SLACK_WEBHOOK_URL is set (in the
project's .env file, which is not committed), the same message is also sent to Slack.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request

log = logging.getLogger(__name__)


def build_message(dag_id: str, task_id: str, run_id: str, error: object) -> str:
    return f":red_circle: Pipeline check failed\nDAG: {dag_id}\nTask: {task_id}\nRun: {run_id}\nReason: {error}"


def notify_failure(context: dict) -> None:
    ti = context.get("task_instance")
    message = build_message(
        dag_id=getattr(ti, "dag_id", "?"),
        task_id=getattr(ti, "task_id", "?"),
        run_id=getattr(ti, "run_id", "?"),
        error=context.get("exception", "unknown error"),
    )
    log.error(message)

    webhook = os.environ.get("SLACK_WEBHOOK_URL", "").strip()
    if not webhook:
        return
    try:
        request = urllib.request.Request(
            webhook,
            data=json.dumps({"text": message}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(request, timeout=10)
    except Exception as exc:  # an alerting problem must not hide the original failure
        log.error("Could not send Slack alert: %s", exc)
