FROM apache/airflow:3.3.0-python3.12

USER root

RUN apt-get update \
    && apt-get install --yes --no-install-recommends openjdk-17-jre-headless \
    && mkdir -p /home/airflow/.ivy2/cache /home/airflow/.ivy2/jars \
    && chown -R airflow:root /home/airflow/.ivy2 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY airflow/requirements.txt /tmp/airflow-requirements.txt

RUN pip install --no-cache-dir "apache-airflow==3.3.0" -r /tmp/airflow-requirements.txt
