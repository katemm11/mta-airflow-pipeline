from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from mta_etl_1 import get_station_data
from mta_etl_2 import get_rider_data


my_dag = DAG("mta_dag", description='MTA assignment dag',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False, tags=["mta"])

run_etl_1 = PythonOperator(
  task_id="mta_etl_1",
  python_callable=get_station_data,
  dag=my_dag,
)

run_etl_2 = PythonOperator(
  task_id="mta_etl_2",
  python_callable=get_rider_data,
  dag=my_dag,
)


run_etl_1 >> run_etl_2
