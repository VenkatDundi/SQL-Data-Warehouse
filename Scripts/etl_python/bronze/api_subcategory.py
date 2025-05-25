import requests
import pandas as pd
import pyodbc
import time

from ..utils.db import get_sql_connection

#api_endpoint = r'https://api-working-2byz.onrender.com/subcategories'


def extract_from_api(api_endpoint):

    try:
        api_status = requests.get(url=api_endpoint, timeout=90).status_code              # Wait for the render API to Invoke - Inactive for every 15 minutes
    
        #expected_columns = ["subcategoryKey", "Subcategory", "categoryKey","subcategoryManager", "targetMarketSegment","productCount", "avgUnitPrice", "subcategoryStatus","createdDate", "updatedDate"]

        if api_status == 200:                                       

            print("status OK")
            data = requests.get(url=api_endpoint, timeout=60)                # Capture the data using GET       
            content_type = data.headers.get("Content-Type")         #application/json
            
            df = pd.json_normalize(data.json())                     # Format the JSON structure
            print("JSON OK")
            #df.isna().sum()                                        # Check Nulls count in each columns of the dataframe
            #df[df.isna().any(axis=1)]                              # Get all rows with atleast one Null value

            #df_columns = [i.strip().lower() for i in df.columns.to_list()]             # Check for missing fields
            #missing_fields = [j.strip().lower() for j in expected_columns if j not in df_columns]

            #df.replace({float('nan'): None}, inplace=True)          # Replace nan with None ---> To insert in SQL db

            print("Data Frame OK")
            return ([api_status, df])

        else:
            return ([api_status, requests.get(api_endpoint).reason])


    except requests.exceptions.RequestException as e:
        print("Error connecting to API:")
        print(type(e).__name__, "→", str(e))


def ingest_api_subcategory(api_data):

    chunk_size = 8                  # Chunk Size for batch processing
    connection = None
    try:
        
        connection = get_sql_connection()          # Establish the connection
        cursor = connection.cursor()
        
        print(">> Connection to SQL Server successful")

        cursor.execute("TRUNCATE TABLE DataWarehouse.bronze.erp_SubCategory;")          # Truncate the existing table

        print(">> Truncated Table!")

        for i in range(0, len(api_data), chunk_size):          
            
            chunk = api_data[i:i+chunk_size]                                                # Traverse through each chunk

            for index, row in chunk.iterrows():
                print("Index - {}, Key - {}".format(index, row))
                cursor.execute("""EXEC bronze.sp_InsertSubcategory ?, ?, ?, ?, ?, ?, ?, ?, ?, ?""",        # Stored Procedure to insert SubCategory Data
                    row.SubcategoryKey, row.Subcategory, row.CategoryKey,
                    row.SubcategoryManager, row.TargetMarketSegment, row.ProductCount,
                    row.AvgUnitPrice, row.SubcategoryStatus, row.CreatedDate, row.UpdatedDate)
            
            connection.commit()     # Commit on each chunk
            print("Chunk - {} Processed".format(i))
            time.sleep(1)
        print(">>> Commit Completed on SubCategory table Insertion")

    except Exception as e:
        print(f"Error connecting to SQL Server: {e}")
        if connection:
            connection.rollback()
        print(f">>> Rollback has been completed due to an error during subcategory ingestion")
        raise  # Show full traceback

    finally:
        if connection:
            connection.close()
            print("Connection closed")


# *** Handled by Airflow in Docker ***

""" result_extract = extract_from_api(api_endpoint)                 # Result from API extraction

if (result_extract[0] == 200):                                  
    result = ingest_api_subcategory(result_extract[1])
else:
    print("Failed to extract data from API") """