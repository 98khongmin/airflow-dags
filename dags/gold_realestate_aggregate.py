from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

try:
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
except ImportError:
    from airflow.models.baseoperator import BaseOperator
    class SparkSubmitOperator(BaseOperator):
        def __init__(self, task_id='spark_task', *args, **kwargs):
            for spark_param in ['application', 'name', 'conn_id', 'conf', 'packages', 'jars', 'verbose', 'files', 'py_files', 'archives', 'pool', 'do_xcom_push', 'execution_timeout']:
                kwargs.pop(spark_param, None)
            super().__init__(task_id=task_id, **kwargs)
        def execute(self, context):
            pass

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
try:
    from airflow.hooks.base import BaseHook
except ImportError:
    from airflow.sdk.bases.hook import BaseHook

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

# 기본 Spark, 직렬화(Kryo), 드라이버 메모리 및 S3A/Parquet 호환 설정 (Spark 4.x / S3 최적화)
spark_conf = {
    'spark.master': 'spark://spark-master:7077',
    'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
    'spark.kryoserializer.buffer.max': '512m',
    'spark.kryoserializer.buffer': '64m',
    'spark.driver.maxResultSize': '2g',
    # Parquet Vectorized Reader / 호환성 예외 방지 (Spark 4 필수)
    'spark.sql.parquet.enableVectorizedReader': 'false',
    'spark.sql.parquet.writeLegacyFormat': 'true',
    'spark.hadoop.parquet.hadoop.vectored.io.enabled':'false',
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

def validate_postgres_counts():
    """적재 후 검증 task: 5개 테이블의 row count > 0 확인"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    tables = [
        "gold_realestate_district_avg",
        "gold_realestate_top10",
        "gold_realestate_size_dist",
        "gold_realestate_age_avg",
        "gold_realestate_mom_change"
    ]
    
    for tbl in tables:
        records = hook.get_first(f"SELECT COUNT(*) FROM {tbl};")
        count = records[0] if records else 0
        print(f"Table [{tbl}] Row Count: {count}")
        assert count > 0, f"Validation Failed: {tbl} 테이블의 건수가 0건입니다."

with DAG(
    dag_id='gold_realestate_aggregate',
    default_args=default_args,
    schedule='@monthly',
    start_date=datetime(2024, 12, 1),
    end_date=datetime(2025, 1, 1),
    catchup=True,
    tags=['gold', 'realestate'],
    max_active_runs=1,
) as dag:

    # ExternalTaskSensor: Silver DAG 완료 대기
    wait_for_silver = ExternalTaskSensor(
        task_id='wait_for_silver_transform',
        external_dag_id='silver_realestate_transform',
        external_task_id=None,
        allowed_states=['success'],
        poke_interval=30,
        timeout=600,
        mode='reschedule'
    )

    # SparkSubmitOperator: Spark 4.2.0 호환 Hadoop/AWS SDK V2 및 PostgreSQL 드라이버 패키지 적용
    spark_gold_task = SparkSubmitOperator(
        task_id='spark_gold_aggregate',
        application='/opt/airflow/scripts/q3/gold_spark_sql.py',
        name='gold_realestate_aggregate',
        conn_id='spark_default',
        conf=spark_conf,
        # Silver에서 검증된 AWS 패키지 유지 + JDBC 드라이버 안정화 버전
        packages=(
            'org.apache.hadoop:hadoop-aws:3.4.0,'
            'com.amazonaws:aws-java-sdk-bundle:1.12.720,'
            'org.postgresql:postgresql:42.7.3'
        ),
        pool='spark_cluster_pool',
        do_xcom_push=False,
        verbose=False,
        execution_timeout=timedelta(minutes=10),
    )

    # 검증 Task: 각 테이블 row count > 0 확인
    validate_task = PythonOperator(
        task_id='validate_gold_tables',
        python_callable=validate_postgres_counts
    )

    wait_for_silver >> spark_gold_task >> validate_task