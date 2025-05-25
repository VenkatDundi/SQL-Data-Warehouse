from airflow import DAG
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from time import time
import sys
import pandas as pd
import os
from pathlib import Path

# Add scripts path to sys.path - This helps in path detection in docker container
sys.path.append('/opt/airflow/scripts')
  
# Import ETL functions from bronze scripts
from etl_python.bronze.api_subcategory import extract_from_api, ingest_api_subcategory # type: ignore
from etl_python.bronze.bulk_insert_procedure import load_bulk_data # type: ignore
from etl_python.bronze.incremental_sales import load_sales # type: ignore
from etl_python.bronze.scd_customers import slowly_change_dim_customers # type: ignore
from etl_python.bronze.web_currency import scrape_currencydata, ingest_web_currency # type: ignore """

default_args = {
    'owner' : 'Venkat',
    'start_date' : datetime(2025,5,12),
    'retries' : 1,
    'retry_delay': timedelta(minutes=1)
}


with DAG(

    dag_id = "dag_bronze_layer",
    default_args=default_args,
    schedule = "@daily",
    catchup=False

) as dag:
    

    def task_extract_from_api(ti, **kwargs):               # context is defined to access the run time metadata
        start = time()
        api_endpoint = 'https://api-working-2byz.onrender.com/subcategories'        # API end point
        status, df = extract_from_api(api_endpoint)
        print(status, type(status))
        ti.xcom_push(key='status', value=status)
        #ti.xcom_push(key = 'resultant_data', value = resultant_data)
        if status == 200:
            tmp_path = '/opt/airflow/logs/tmp/api_subcategory.json'         
            os.makedirs(os.path.dirname(tmp_path), exist_ok=True)
            df.to_json(tmp_path, orient='records', lines=True)                      # df to json file conversion due to PyArrow incompatibility
            ti.xcom_push(key='api_data_path', value=tmp_path)            # pass json file path to downstream tasks
            print("Status success - Json file path saved")
            ti.xcom_push(key = 'state', value = 'success')
            ti.xcom_push(key='task_update', value=f'Subcategory records extracted from API: {len(df)}')
        else:
            ti.xcom_push(key = 'state', value = 'failure')
            ti.xcom_push(key='task_update', value=f'Subcategory records extraction had failed!')
        
        end = time()
        ti.xcom_push(key = 'duration', value = round((end - start), 4))

            

    def task_status_validation(ti, **kwargs):
        start = time()
        status = ti.xcom_pull(task_ids='SubCategory.extract_api', key='status')
        if status != 200:
            print(f"API returned non-200 status: {status}")
            ti.xcom_push(key = 'status', value = 'failure')
            return False
        ti.xcom_push(key = 'state', value = 'success')
        end = time()
        ti.xcom_push(key = 'duration', value = round((end-start), 4))
        ti.xcom_push(key='task_update', value=f'API status validated: {status}')
        return True


    def task_ingest_api_data(ti, **kwargs):
        start = time()
        path = ti.xcom_pull(task_ids='SubCategory.extract_api', key='api_data_path')                 # Pull the Json file path
        if path and os.path.exists(path):
            df = pd.read_json(path, orient='records', lines=True)           # convert back to df from JSON as we are using df to ingest data to table
            df.replace({float('nan'): None}, inplace=True)
            ingest_api_subcategory(df)
            # Push to XCom
            ti.xcom_push(key='task_update', value=f'Subcategory Table - Rows Updated: {len(df)}')
            ti.xcom_push(key = 'state', value = 'success')
        else:
            ti.xcom_push(key = 'state', value = 'failure')
            raise ValueError("Subcategory data file not found or not passed via XCom from upstreams")
        end = time()
        ti.xcom_push(key = 'duration', value = round((end - start), 4))

    def task_load_bulk_data(ti, **kwargs):
        start = time()
        load_bulk_data("E:/Portfolio/SQL-Data-Warehouse/datasources/")          # Hard code local path due to SQL Server unable to capture the bulk insert path of the files in docker volumes
        ti.xcom_push(key='task_update', value=f'Bulk Ingestion Executed - Products, Exchange Rates, Stores, Customers, Category Tables') 
        ti.xcom_push(key = 'state', value = 'success')
        end = time()
        ti.xcom_push(key = 'duration', value = round((end - start), 4))
        
    
    def task_load_sales(ti, **kwargs):
        start = time()
        rows = load_sales("/opt/airflow/datasources/Sales.csv")
        ti.xcom_push(key='task_update', value=f'Incremental Load on Sales Table - Rows Updated: {rows}')
        ti.xcom_push(key = 'state', value = 'success')
        end = time()
        ti.xcom_push(key = 'duration', value = round((end - start), 4))

    def task_slowly_changing_customers(ti, **kwargs):
        start = time()
        rows = slowly_change_dim_customers("/opt/airflow/datasources/NewCustomers.csv")
        ti.xcom_push(key='task_update', value=f'Slowly Changing Dimension on Customers Table - Rows Updated: {rows}')
        ti.xcom_push(key = 'state', value = 'success')
        end = time()
        ti.xcom_push(key = 'duration', value = round((end - start), 4))


    def task_scrape_currency(ti, **kwargs):               # context is defined to access the run time metadata
        start = time()
        url = "https://venkatdundi.github.io/Web-Scrapping/Github-Pages-Scraping/index.html"            # Github Static Webpage
        resultant = scrape_currencydata(url)
        ti.xcom_push(key = 'resultant', value = resultant)
        ti.xcom_push(key = 'generated_file_path', value = '/opt/airflow/datasources/')
        ti.xcom_push(key = 'state', value = 'success')
        end = time()
        ti.xcom_push(key = 'duration', value = round((end - start), 4))
        ti.xcom_push(key='task_update', value=f'Web Scraping currency data page')


    def task_scrape_status(ti, **kwargs):
        start = time()
        status = ti.xcom_pull(task_ids='Currency.scrape_web_data', key='resultant')
        if status != True:
            print(f"Web Scraping Error")
            ti.xcom_push(key = 'state', value = 'failure')
            return False
        print("*** Scrape Successful ***")
        ti.xcom_push(key = 'state', value = 'success')
        end = time()
        ti.xcom_push(key = 'duration', value = round((end - start), 4))
        ti.xcom_push(key='task_update', value=f'Data Extraction status: {status}')
        return True


    def task_ingest_web_currency(ti, **kwargs):
        start = time()
        generated_file_path = ti.xcom_pull(task_ids='Currency.scrape_web_data', key = 'generated_file_path')
         
        if generated_file_path and os.path.exists(generated_file_path):
            print(generated_file_path) # Docker volume file path --- It can't be detected by the SQL server available in the local system
            # Hard code local path as SQL Server unable to capture the bulk insert path of the files in docker volumes
            ingest_web_currency('E:/Portfolio/SQL-Data-Warehouse/datasources/')
            ti.xcom_push(key = 'state', value = 'success')
            ti.xcom_push(key='task_update', value=f'Bulk Ingestion of currency data extarcted from Web - Currency Table')
        else:
            ti.xcom_push(key = 'state', value = 'failure')
            raise ValueError("Scraped csv file for product Category not found or not passed via XCom from upstreams")
        end = time()
        ti.xcom_push(key = 'duration', value = round((end - start), 4))
                                                                                                        # Jinja template in airflow - {{  }}
    def generate_email_content(ti, ds, **kwargs):                    # Creates a HTML string with all the task details collected with the respective task instance 
        
        dag_id = ti.dag_id
        execution_date = ds
        run_id = ti.run_id

        result1 = ti.xcom_pull(task_ids = 'get_rows_initial')
        result2 = ti.xcom_pull(task_ids = 'get_rows_final')

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

                    <h3>Data Warehouse Table Summary</h3>
                    <table border="1" cellpadding="5" cellspacing="0">
                        <thead><tr><th>Table Info</th></tr></thead><tbody>"""
        
        for result_set in result1:
            for row in result_set:
                html += f"<tr><td>{row}</td></tr>"
        html += "</tbody></table>"

        html += f"""<div style="text-align: center; margin: 30px 0;">
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
            if task_id in ['generate_html_email', 'send_email', 'get_rows_initial', 'get_rows_final']:             # Skip the details for email trigger
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
 

    with TaskGroup("SubCategory") as subcategory_group:

        task_extract = PythonOperator(
            task_id = 'extract_api',
            python_callable=task_extract_from_api
        )

        task_status = ShortCircuitOperator(
            task_id = 'check_status',
            python_callable = task_status_validation
        )

        task_ingestion = PythonOperator(
            task_id = 'ingestion_api',
            python_callable=task_ingest_api_data
        )

        task_extract >> task_status >> task_ingestion
    
    task_bulk_load = PythonOperator(                        

        task_id = 'bulk_load',
        python_callable=task_load_bulk_data
    )

    task_incr_sales = PythonOperator(

        task_id = 'incr_sales',
        python_callable=task_load_sales
    )

    task_scd_customers = PythonOperator(

        task_id = 'scd_customers',
        python_callable=task_slowly_changing_customers
    )

    with TaskGroup("Currency") as currency_group:

        task_web_scrape = PythonOperator(
            task_id = 'scrape_web_data',
            python_callable=task_scrape_currency
        )

        task_web_status = ShortCircuitOperator(
            task_id = 'check_scrape_status',
            python_callable = task_scrape_status
        )

        task_web_ingestion = PythonOperator(
            task_id = 'ingestion_currency_data',
            python_callable=task_ingest_web_currency
        )

        task_web_scrape >> task_web_status >> task_web_ingestion
    

    task_tablerows_Initial = SQLExecuteQueryOperator(
        
        task_id = 'get_rows_initial',
        conn_id='sql_server',
        database='DataWarehouse',
        sql = """ EXEC DataWarehouse.bronze.sp_PrintTableRowCounts """,
        do_xcom_push = True
    )

    task_tablerows_Final = SQLExecuteQueryOperator(
        
        task_id = 'get_rows_final',
        conn_id='sql_server',
        database='DataWarehouse',
        sql = """ EXEC DataWarehouse.bronze.sp_PrintTableRowCounts """,
        do_xcom_push = True
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

    [task_tablerows_Initial >> subcategory_group >> task_bulk_load >> task_incr_sales >> task_scd_customers >> currency_group >> task_tablerows_Final >> trigger_email] >> send_email