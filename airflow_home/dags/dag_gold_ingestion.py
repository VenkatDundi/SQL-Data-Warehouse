from airflow import DAG
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
#from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from time import time
import sys
#import pandas as pd
#import os

# Add scripts path to sys.path - This helps in path detection in docker container
sys.path.append('/opt/airflow/scripts')
  
# Import ETL functions from silver scripts
from etl_python.gold.load_gold import load_to_gold # type: ignore

default_args = {
    'owner' : 'Venkat',
    'start_date' : datetime(2025,5,20),
    'retries' : 1,
    'retry_delay': timedelta(minutes=1)
}


with DAG(

    dag_id = "dag_gold_layer",
    default_args=default_args,
    schedule = None,
    catchup=False

) as dag:
    
    
    
    def task_load_gold_layer(ti, **kwargs):
        start = time()
        result = load_to_gold()        # Silver layer stored procedure
        if result == True:
            ti.xcom_push(key='task_update', value=f'Creation of Facts and Dimenions in Gold layer completed successfully') 
            ti.xcom_push(key = 'state', value = 'success')
            end = time()
            ti.xcom_push(key = 'duration', value = round((end - start), 4))
        else:
            ti.xcom_push(key='task_update', value=f'View creation in gold layer had failed!')
            ti.xcom_push(key = 'state', value = 'failure')
        return True
                                                                                                        # Jinja template in airflow - {{  }}
    def generate_email_content(ti, ds, **kwargs):                    # Creates a HTML string with all the task details collected with the respective task instance 
        
        dag_id = ti.dag_id
        execution_date = ds
        run_id = ti.run_id

        result2 = ti.xcom_pull(task_ids = 'view_count_Final')

        html = f""" 
            <!DOCTYPE html>
                <html lang="en">
                    <head>
                    <meta charset="UTF-8">
                    <title>DAG Email Report</title>
                </head>
                
                <body style="font-family: Arial, sans-serif; background-color: #ffffff; color: #333333;">

                    <div style="margin: 20px;">
                        <div style="border: 1px solid #ccc; border-radius: 8px; padding: 20px; background-color: #f4f4f4;">
                        <h2 style="color: #084211;">DAG Run Succeeded</h2>
                        <p><strong>DAG:</strong> {dag_id}</p>
                        <p><strong>Execution Date:</strong> {execution_date}</p>
                        <p><strong>Run ID:</strong> {run_id}</p>
                        </div>
                    </div>
                    
                    <div style="text-align: center; margin: 30px 0;">
                        <h2 style="background-color: #fbd94c; padding: 10px; width: 250px; margin: auto;">Task Summary</h2>
                    </div>

                    <div style="margin: 0 20px;">
                        <table style="width: 100%; border-collapse: collapse;">
                            <thead>
                                <tr style="background-color: #333; color: white;">
                                    <th style="padding: 10px; border: 1px solid #ccc;">Task ID</th>
                                    <th style="padding: 10px; border: 1px solid #ccc;">Status</th>
                                    <th style="padding: 10px; border: 1px solid #ccc;">Duration</th>
                                    <th style="padding: 10px; border: 1px solid #ccc;">Task Update</th>
                                </tr>
                            </thead>
                            <tbody>"""
        
        for task_id in dag.task_ids:
            if task_id in ['generate_html_email', 'send_email', 'view_count_Final']:             # Skip the details for email trigger
                continue
            state = ti.xcom_pull(task_ids = task_id, key = 'state') or 'N/A'
            duration = ti.xcom_pull(task_ids = task_id, key = 'duration') or 'N/A'
            update = ti.xcom_pull(task_ids = task_id, key = 'task_update') or 'N/A'
            
            color = "#28a745" if state.lower() == "success" else "#dc3545"
            
            # Append html content over all tasks except for email generation 
            html = html + f"""                                                          
                    <tr>
                        <td style="padding: 10px; border: 1px solid #ccc;">{task_id}</td>
                        <td style="padding: 10px; border: 1px solid #ccc; color: {color};">{state}</td>
                        <td style="padding: 10px; border: 1px solid #ccc;">{duration} Seconds</td>
                        <td style="padding: 10px; border: 1px solid #ccc;">{update}</td>
                    </tr>
                """
        html = html + "</tbody></table>"

        html += f"""<h3>Data Warehouse Table Summary</h3>
                    <table border="1" cellpadding="5" cellspacing="0">
                        <thead><tr><th>Table Info</th></tr></thead><tbody>"""

        for result_set in result2:
            for row in result_set:
                html += f"<tr><td>{row}</td></tr>"
        html += "</tbody></table></div></body></html>"

        ti.xcom_push(key = 'html_content', value = html)
        print("Email triggered through Airflow SMTP service successfully")
  

    task_view_count_Final = SQLExecuteQueryOperator(

        task_id = 'view_count_Final',
        conn_id = 'sql_server',
        database = 'DataWarehouse',
        sql = """ EXEC DataWarehouse.gold.sp_PrintTableRowCounts """,
        do_xcom_push = True

    )

    task_load_gold = PythonOperator(                        

        task_id = 'load_gold',
        python_callable=task_load_gold_layer
    )

    trigger_email = PythonOperator(

        task_id = 'generate_html_email',
        python_callable = generate_email_content,
        op_kwargs = {"ds": "{{ds}}"}
    )
    
    send_email = EmailOperator(
        task_id='send_email',
        to='gnanireddy7@gmail.com',
        subject='Airflow DAG Summary: "{{ dag.dag_id }}" - Successful Run on  {{ ds }}',
        html_content="{{ti.xcom_pull(task_ids = 'generate_html_email', key = 'html_content')}}",             # context variable can't be used in jinja template
        trigger_rule='all_success'
    )

    task_load_gold >> task_view_count_Final >> trigger_email >> send_email