
from airflow import DAG

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="example_pi_example",
    description="WorldShare Commodity Monthly",
    start_date=datetime(2025, 1, 1),
    schedule="@monthly",
) as dag:
    spark_submit_operator = SparkSubmitOperator(
        task_id="example_pi_exampleTask",
        application="${SPARK_HOME}/examples/src/main/python/pi.py",
        conn_id="spark_default",
        verbose=True
    )