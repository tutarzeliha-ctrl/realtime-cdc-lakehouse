# Airflow image for operating the CDC pipeline.
# Build context is the repository root (see docker-compose.yml).
FROM apache/airflow:3.4.0-python3.12

# dbt runs in its own virtual environment, so its dependencies cannot conflict with Airflow's.
USER root
RUN python -m venv /opt/dbt_venv && chown -R airflow: /opt/dbt_venv
USER airflow

COPY analytics/requirements.txt /tmp/dbt-requirements.txt
RUN /opt/dbt_venv/bin/pip install --no-cache-dir -r /tmp/dbt-requirements.txt

# Libraries used by the health checks, installed next to the pinned Airflow version.
COPY orchestration/requirements.txt /tmp/ops-requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/ops-requirements.txt
