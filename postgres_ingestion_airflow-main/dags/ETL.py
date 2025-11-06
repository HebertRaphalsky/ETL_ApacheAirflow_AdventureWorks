from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Importa a função do script ETL
from scripts.ext import executar_etl

default_args = {
    "owner": "Extracao",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 22),
}

with DAG(
    "Extracao",
    default_args=default_args,
    description="Extracao - AdventureWorks",
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
) as dag:

    run_task_1 = PythonOperator(
        task_id="task_Extracao",
        python_callable=executar_etl,
    )