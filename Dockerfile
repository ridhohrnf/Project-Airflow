FROM apache/airflow:latest-python3.11

USER airflow

COPY requirements.txt /

RUN pip install --no-cache-dir "apache-airflow[statsd]==${AIRFLOW_VERSION}" -r /requirements.txt