import pyodbc
import os

from ..utils.db import get_sql_connection


def load_currency(folder_path):
    
    connection = None

    try:

        connection = get_sql_connection()              # connection to db
        cursor = connection.cursor()
        print(cursor)
        print("Connection to SQL Server successful")

        print(folder_path)
        cursor.execute("""EXEC DataWarehouse.bronze.load_currency ?""", folder_path)        # Stored procedure call with folder_path as parameter

        connection.commit()
        print(">>> Commit completed - Bulk Ingestion of Currency has been completed")

    except Exception as e:
        print(f"Error while performing the bulk insert currency with reason: {e}")
        if connection:
            connection.rollback()
        print(f">>> Rollback has been completed due to an error during bulk ingestion")
        raise  # Show full traceback

    finally:
        if connection:
            connection.close()
            print("Connection closed")

# *** Handled by Airflow in Docker ***
""" initial_dir = os.path.abspath(os.path.join(os.getcwd(), "..", "..", ".."))

for root, dirs, files in os.walk(initial_dir):
    if "DataSources" in dirs:
        folder_path = os.path(dirs)

result_bulk_load = load_bulk_data(folder_path) """
