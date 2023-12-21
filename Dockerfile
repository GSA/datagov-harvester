FROM apache/airflow:slim-latest-python3.10

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
    && apt-get clean

USER airflow

COPY 2.8.0-constraints-3.10.txt /constraints.txt
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt -c /constraints.txt
