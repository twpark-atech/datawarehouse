# airflow.Dockerfile
FROM apache/airflow:2.8.1-python3.10

USER root
COPY ./airflow/requirements.txt /requirements.txt

RUN groupadd -g 1001 docker && usermod -aG docker airflow

USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
