# bis_pipeline_dag.py
import sys
import os
project_root = os.path.abspath(os.path.dirname(__file__) + "/../")
sys.path.append(os.path.join(os.path.dirname(__file__)))

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# DAG 설정
with DAG(
    dag_id='bis_pipeline_dag',
    start_date=datetime(2025, 7, 24),
    schedule_interval=None,
    catchup=False
) as dag:
    spark_image='my-spark:3.3-kafka'

    common_mounts = [
        Mount(source='/home/atech/datawarehouse/spark-app', target='/app', type='bind'),
        Mount(source='/home/atech/datawarehouse/jars', target='/opt/spark/jars', type='bind'),
    ]

    # 1단계: API 수집
    collect_bronze = BashOperator(
        task_id='collect_bronze',
        bash_command='python /opt/airflow/dags/kafka/api_producer.py'
    )

    # 2단계: Kafka -> Bronze
    send_bronze = DockerOperator(
        task_id='kafka_to_bronze',
        image=spark_image,
        command='/opt/bitnami/spark/bin/spark-submit  --conf spark.driver.extraJavaOptions=-Dspark.conf.dir=/app/spark-conf /app/bronze.py',
        network_mode='datawarehouse',
        docker_url='unix://var/run/docker.sock',
        mounts=common_mounts,
        auto_remove=True
    )
    # 3단계: Bronze -> Silver
    send_silver = DockerOperator(
        task_id='bronze_to_silver',
        image=spark_image,
        command='/opt/bitnami/spark/bin/spark-submit --conf spark.driver.extraJavaOptions=-Dspark.conf.dir=/app/spark-conf /app/silver.py',
        network_mode='datawarehouse',
        mounts=common_mounts,
        auto_remove=True
    )
    # 4단계: Silver -> Gold
    send_gold = DockerOperator(
        task_id='silver_to_gold',
        image=spark_image,
        command='/opt/bitnami/spark/bin/spark-submit --conf spark.driver.extraJavaOptions=-Dspark.conf.dir=/app/spark-conf /app/gold.py',
        network_mode='datawarehouse',
        mounts=common_mounts,
        auto_remove=True
    )
    # 5단계: Gold -> PostgreSQL
    send_postgres = DockerOperator(
        task_id='load_to_postgres',
        image=spark_image,
        command='/opt/bitnami/spark/bin/spark-submit --conf spark.driver.extraJavaOptions=-Dspark.conf.dir=/app/spark-conf /app/load_to_postgres.py',
        network_mode='datawarehouse',
        mounts=common_mounts,
        auto_remove=True
    )
    collect_bronze >> send_bronze >> send_silver >> send_gold >> send_postgres