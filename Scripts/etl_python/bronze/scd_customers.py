import pandas as pd
import pyodbc
from bs4 import BeautifulSoup as bs
import os
from ..utils.db import get_sql_connection

pd.set_option('display.max_columns', None)


def slowly_change_dim_customers(New_Customers):          # Implementation of SCD 2 for Customers

    new_data = pd.read_csv(New_Customers)
    connection = None
    
    try:
        connection = get_sql_connection()          # Establish the connection
        cursor = connection.cursor()
        
        print("Connection to SQL Server successful")

        for index, row in new_data.iterrows():
            cust = cursor.execute("SELECT * FROM bronze.crm_Customers WHERE cst_CustomerKey = ?", row['CustomerKey']).fetchone()            

            if cust is None:       # New Customer validation
                cursor.execute("""EXEC bronze.sp_InsertCustomer ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?""",
                               row['CustomerKey'],row['Gender'],row['Name'],row['City'],row['State Code'],row['State'],
                                row['Zip Code'],row['Country'],row['Continent'],row['Birthday'],pd.Timestamp.today().normalize().strftime('%Y-%m-%d'),
                                None,1)
                print("New Customer details have been saved!")
            else:
                cursor.execute("Update bronze.crm_Customers set cst_EndDate = CAST(GETDATE() AS DATE), cst_IsActive=0 where cst_CustomerKey = ? and cst_IsActive=1", row.CustomerKey)
                
                cursor.execute("""EXEC bronze.sp_InsertCustomer ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?""",
                               row['CustomerKey'],row['Gender'],row['Name'],row['City'],row['State Code'],row['State'],
                                row['Zip Code'],row['Country'],row['Continent'],row['Birthday'], pd.Timestamp.today().normalize().strftime('%Y-%m-%d'),
                                None,1)
                print("Existing Customer details were updated!")
        connection.commit()
        print("Commit completed - SCD Update on Customers")
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Error connecting to SQL Server: {ex.args}")

    finally:
        if connection:
            connection.close()
            print("Connection closed")


for r,d,f in os.walk(os.getcwd()):
    for i in f:
        if i.endswith("NewCustomers.csv"):                  # Capture the file path for the New Customers file (Slowly Changing Dimension)
            new_customers_file = os.path.join(r,i)

result_dim_customers = slowly_change_dim_customers(new_customers_file)