from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from mta_etl import run_mta_etl


dag = DAG("mta_dag", description='MTA assignment dag',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False, tags=["mta"])

run_etl = PythonOperator(
  task_id="complete_mta_etl",
  python_callable=run_mta_etl,
  dag=dag,
)

print("hello I am in the mta dag")
run_etl

