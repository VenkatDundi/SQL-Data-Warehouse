from airflow import DAG
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import sys


# Add scripts path to sys.path - This helps in path detection in docker container
sys.path.append('/opt/airflow/scripts')
  


default_args = {
    'owner' : 'Venkat',
    'start_date' : datetime(2025,5,19),
    'retries' : 1,
    'retry_delay': timedelta(minutes=1)
}


with DAG(

    dag_id = "dag_quarantine_layer",
    default_args=default_args,
    schedule = None,
    catchup=False

) as dag:
    
                                                                                              
    def generate_email_content(ti, ds, **kwargs):                    # Creates a HTML string with all the details from both tables
        
        dag_id = ti.dag_id
        execution_date = ds
        run_id = ti.run_id
        
        customers_result = ti.xcom_pull(task_ids='quarantine_customer_check')
        products_result = ti.xcom_pull(task_ids='quarantine_product_check') 

        html = "<h2 style='color: #084211;'>Quarantine Tables Summary - {}</h2>".format(ds)

        if customers_result:
            html += f"""

            <div style="margin: 20px;">
                <div style="border: 1px solid #ccc; border-radius: 8px; padding: 20px; background-color: #f4f4f4;">
                <h2 style="color: #084211;">DAG Run Succeeded</h2>
                <p><strong>DAG:</strong> {dag_id}</p>
                <p><strong>Execution Date:</strong> {execution_date}</p>
                <p><strong>Run ID:</strong> {run_id}</p>
                </div>
            </div>
            <h3>Quarantine Table: <code>quarantine.Customers</code></h3>
            <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse; font-family: Arial, sans-serif;">
                <thead style="background-color: #f2f2f2;">
                    <tr>
                        <th>QuarantineID</th>
                        <th>CustomerKey</th>
                        <th>FieldName</th>
                        <th>FieldValue</th>
                        <th>ErrorReason</th>
                        <th>LoadDate</th>
                    </tr>
                </thead>
                <tbody>
            """
            for row1 in customers_result:
                html += "<tr>" + "".join(f"<td>{cell}</td>" for cell in row1) + "</tr>"
            html += "</tbody></table>"
        
        if products_result:
            html += """
                    <h3>Quarantine Table: <code>quarantine.Products</code></h3>
                <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse; font-family: Arial, sans-serif;">
                    <thead style="background-color: #f2f2f2;">
                        <tr>
                            <th>QuarantineID</th>
                            <th>ProductKey</th>
                            <th>FieldName</th>
                            <th>FieldValue</th>
                            <th>ErrorReason</th>
                            <th>LoadDate</th>
                        </tr>
                    </thead>
                    <tbody>
                """
            for row2 in products_result:
                html += "<tr>" + "".join(f"<td>{cell}</td>" for cell in row2) + "</tr>"
            html += "</tbody></table>"
        
        if not customers_result and not products_result:
            html += "<p>No quarantine records retrieved for Customers and Products.</p>"

        ti.xcom_push(key='quarantine_html', value=html)
        print("Email triggered through Airflow SMTP service successfully")
        return True


    trigger_email = PythonOperator(
        task_id = 'generate_html_email',
        python_callable = generate_email_content,
        op_kwargs = {"ds": "{{ds}}"}
    )
    
    send_email = EmailOperator(
        task_id='send_email',
        to='gnanireddy7@gmail.com',
        subject='Airflow DAG Summary: "{{ dag.dag_id }}" - Successful Run on  {{ ds }}',
        html_content="{{ti.xcom_pull(task_ids = 'generate_html_email', key = 'quarantine_html')}}",   
        trigger_rule='all_success'
    )

    run_quarantine_customers = SQLExecuteQueryOperator(
        task_id='quarantine_customer_check',
        conn_id='sql_server',
        sql="""

            PRINT '--- Details of quarantine.Customers ---'
            Select * from quarantine.Customers;
            
            """, 
        database='DataWarehouse',
        do_xcom_push = True
    )

    run_quarantine_products = SQLExecuteQueryOperator(
        task_id='quarantine_product_check',
        conn_id='sql_server',
        sql="""

            PRINT '--- Details of quarantine.Products ---'
            Select * from quarantine.Products;
            
            """, 
        database='DataWarehouse',
        do_xcom_push = True
    )

    trigger_gold = TriggerDagRunOperator(
        task_id='trigger_gold_dag',
        trigger_dag_id='dag_gold_layer',
        wait_for_completion=True,
        poke_interval=60,
    )



    run_quarantine_customers >> run_quarantine_products >> trigger_email >> send_email >> trigger_gold