from airflow import DAG

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="worldshare_commodity_monthly",
    description="WorldShare Commodity Monthly",
    start_date=datetime(2025, 1, 1),
    schedule="@monthly",
) as dag:
    spark_submit_operator = SparkSubmitOperator(
        task_id="worldshare_commodity_monthly",
        application="/home/yanis/Macrotrade-pyspark/astro/include/getWorldShareCommoditiesMonthly.py",
        conn_id="spark_default",
        application_args=["data/delete/worldshare/commodity/monthly"],
        verbose=True,
    )
