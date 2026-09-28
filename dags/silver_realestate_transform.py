from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.hooks.base import BaseHook

# Spark 미설치 환경에서도 DAG 파싱 및 인자 인식을 지원하는 Fallback Operator
try:
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
except ImportError:
    from airflow.models.baseoperator import BaseOperator
    class SparkSubmitOperator(BaseOperator):
        def __init__(self, *args, **kwargs):
            task_id = kwargs.pop('task_id', 'spark_task')
            super().__init__(task_id=task_id, **kwargs)
        def execute(self, context):
            pass

# JAVA_HOME 강제 설정
if "JAVA_HOME" not in os.environ:
    os.environ["JAVA_HOME"] = "/opt/java/openjdk"
if "/opt/java/openjdk/bin" not in os.environ.get("PATH", ""):
    os.environ["PATH"] = f"/opt/java/openjdk/bin:{os.environ.get('PATH', '')}"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 기본 Spark, 직렬화(Kryo), 드라이버 메모리 및 S3A 설정
spark_conf = {
    'spark.master': 'spark://spark-master:7077',
    'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
    'spark.kryoserializer.buffer.max': '512m',
    'spark.kryoserializer.buffer': '64m',
    'spark.driver.maxResultSize': '2g',
    # S3A 설정
    'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
}

try:
    aws_conn = BaseHook.get_connection("aws_default")
    if aws_conn.login:
        spark_conf['spark.hadoop.fs.s3a.access.key'] = aws_conn.login
    if aws_conn.password:
        spark_conf['spark.hadoop.fs.s3a.secret.key'] = aws_conn.password
    
    extra = aws_conn.extra_dejson
    endpoint = extra.get("endpoint_url") or extra.get("host")
    if endpoint:
        spark_conf['spark.hadoop.fs.s3a.endpoint'] = endpoint
        spark_conf['spark.hadoop.fs.s3a.path.style.access'] = 'true'
        spark_conf['spark.hadoop.fs.s3a.connection.ssl.enabled'] = 'false'
except Exception as e:
    print(f"Warning: Failed to load aws_default connection: {e}")

with DAG(
    dag_id='silver_realestate_transform',
    default_args=default_args,
    schedule='@monthly',
    start_date=datetime(2024, 12, 1),
    end_date=datetime(2025, 1, 1),
    catchup=True,
    tags=['silver', 'realestate'],
) as dag:

    wait_for_bronze = ExternalTaskSensor(
        task_id='wait_for_bronze_collect',
        external_dag_id='bronze_realestate_collect',
        external_task_id=None,
        allowed_states=['success'],
        poke_interval=30,
        timeout=600,
        mode='reschedule'
    )

    spark_transform_task = SparkSubmitOperator(
        task_id='spark_transform_silver',
        application='/opt/airflow/scripts/q2/silver_spark.py',
        name='silver_realestate_transform',
        conn_id='spark_default',
        conf=spark_conf,
        packages=(
            'org.apache.hadoop:hadoop-aws:3.4.0,'
            'com.amazonaws:aws-java-sdk-bundle:1.12.720'
        ),
        verbose=True
    )

    wait_for_bronze >> spark_transform_task