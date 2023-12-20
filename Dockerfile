FROM apache/airflow:latest-python3.10
RUN pip install --upgrade pip
COPY docker-requirements.txt ./requirements.txt
RUN pip install -r requirements.txt
