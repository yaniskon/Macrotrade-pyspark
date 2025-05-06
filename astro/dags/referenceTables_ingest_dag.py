# referenceTables_ingest_dag.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='referenceTables_ingest',
    default_args=default_args,
    description='Run referenceTables_ingest.py with uv',
    schedule=None,  # Set your schedule, e.g., '@daily'
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['referenceTables', 'ingest'],
) as dag:

    run_referenceTables_ingest = BashOperator(
        task_id='run_referenceTables_ingest',
        bash_command= 'cd /home/yanis/Macrotrade-pyspark ; pwd',
        #cwd='/home/yanis/Macrotrade-pyspark/',  # Set this to your project root if needed
    )

