import pandas as pd
import pyodbc
from bs4 import BeautifulSoup as bs
import os
from ..utils.db import get_sql_connection

pd.set_option('display.max_columns', None)


def load_sales(Sales_Extract):
    
    # Incremental Load sales data
    sales_data = pd.read_csv(Sales_Extract)
    sales_data['sales_RowKey'] = (sales_data['Order Number'].astype(str).str.zfill(7) + '_' + sales_data['Line Item'].astype(str).str.zfill(3))   # Padding '0' as it string comparison is in alphabetic
    #sales_data.isna().sum()) -- To check for nulls in columns
    sales_data.replace({float('nan') : None}, inplace=True)
    connection = None
    
    try:
        
        connection = get_sql_connection()          # Establish the connection
        cursor = connection.cursor()
        
        print("Connection to SQL Server successful")
        cursor.execute("""select max([sales_RowKey]) as 'Most_Recent_Older_Txn' from bronze.pos_Sales;""")          # Truncate the existing table
        print("Fetched Maximum RowKey --- Processed earlier")

        recent_row_key = cursor.fetchone()
        #print(recent_row_key)

        if recent_row_key[0] is None:               # Insert the complete sales data extract --- When max(row_key) is None: Initial load
            sales_df = sales_data

        else:                                       # Only consider the recent RowKeys --- Incremental Load validation
            sales_df = sales_data[sales_data['sales_RowKey'] > recent_row_key[0]]

        chunk_size = 5000                           # Chunk processing
        query_sales = """
                    INSERT INTO bronze.pos_Sales (
                        sales_OrderNumber, sales_LineItem, sales_OrderDate, sales_DeliveryDate,
                        sales_CustomerKey, sales_StoreKey, sales_ProductKey, sales_Quantity, sales_CurrencyCode, sales_RowKey
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """

        for i in range(0, len(sales_df), chunk_size):
            chunk = sales_df[i:i+chunk_size]
            data = list(chunk.itertuples(index=False, name=None))
            cursor.executemany(query_sales, data)
            print("Load Processed Chunk {}".format(int(i/5000)))
        
        connection.commit()
        print(">>> Commit completed - Sales data ingestion on Incremental Load")
        return len(sales_df)
    
    except Exception as e:
        print(f"Error while performing the Incremental load with reason: {e}")
        if connection:
            connection.rollback()
        print(f">>> Rollback has been completed due to an error during the Incremental load")
        raise  # Show full traceback

    finally:
        if connection:
            connection.close()
            print("Connection closed")

# *** Handled by Airflow in Docker ***
""" for r,d,f in os.walk(os.getcwd()):
    for i in f:
        if i.endswith("Sales.csv"):                       # Capture the file path for the Sales data extract file (Incremental Load)
            sales_file = os.path.join(r,i)


result_incremental_sales = load_sales(sales_file) """