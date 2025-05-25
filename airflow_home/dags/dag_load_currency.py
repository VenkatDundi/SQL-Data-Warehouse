from airflow import DAG
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from time import time
import sys
from pathlib import Path

# Add scripts path to sys.path - This helps in path detection in docker container
sys.path.append('/opt/airflow/scripts')
  
# Import ETL functions from currency scripts
#from etl_python.currency.bulk_currency import load_currency # type: ignore
from etl_python.bronze.bulk_insert_procedure import load_bulk_data # type: ignore


default_args = {
    'owner' : 'Venkat',
    'start_date' : datetime(2025,5,20),
    'retries' : 1,
    'retry_delay': timedelta(minutes=1)
}


with DAG(

    dag_id = "dag_currency_layer",
    default_args=default_args,
    schedule = "@daily",
    catchup=False

) as dag:

    def task_load_data(ti, **kwargs):
        start = time()
        load_bulk_data("E:/Portfolio/SQL-Data-Warehouse/datasources/")
        ti.xcom_push(key='task_update', value=f'Bulk Ingestion Executed - Products, Exchange Rates, Stores, Customers, Category Tables') 
        ti.xcom_push(key = 'state', value = 'success')
        end = time()
        ti.xcom_push(key = 'duration', value = round((end - start), 4))  
    
    def printing(ti, **kwargs):

        print(ti.xcom_pull(task_ids = 'bulk_insert', key='task_update'))
        print(ti.xcom_pull(task_ids = 'bulk_insert', key='state'))
        print(ti.xcom_pull(task_ids = 'bulk_insert', key='duration'))

    task_print = PythonOperator(
        task_id = 'Just_print',
        python_callable = printing
    )

    task_bulk_currency = PythonOperator(                        

        task_id = 'bulk_insert',
        python_callable=task_load_data
    )
    
    
    task_bulk_currency >> task_print