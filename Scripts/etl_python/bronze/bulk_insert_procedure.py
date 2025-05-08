import pyodbc
import os


from ..utils.db import get_sql_connection


def load_bulk_data(folder_path):

    connection = None

    try:

        connection = get_sql_connection()              # connection to db
        cursor = connection.cursor()
        print("Connection to SQL Server successful")

        cursor.execute("""EXEC DataWarehouse.bronze.load_BulkData ?""", folder_path)        # Stored procedure call with folder_path as parameter

        connection.commit()
        print("> Commit completed - Bulk Ingestion from 5 data sources has been completed")

    except pyodbc.Error as e:
        print("Error while performing the bulk insert on 5 tables with reason: ", e.args)
        if connection:
            connection.rollback()
            print("> Rollback has been completed due to an error during bulk ingestion")
    

initial_dir = os.path.abspath(os.path.join(os.getcwd(), "..", "..", ".."))

for root, dirs, files in os.walk(initial_dir):
    if "DataSources" in dirs:
        folder_path = os.path(dirs)

result_bulk_load = load_bulk_data(folder_path)
