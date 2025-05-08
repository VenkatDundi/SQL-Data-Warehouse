import pyodbc

from ..utils.db import get_sql_connection


def load_to_silver():

    connection = None

    try:

        connection = get_sql_connection()              # connection to db
        cursor = connection.cursor()
        print("Connection to SQL Server successful")

        cursor.execute("""EXEC DataWarehouse.silver.load_silver""")        # Stored procedure call with folder_path as parameter

        connection.commit()
        print("> Commit completed - Silver table ingestion has been completed")

    except pyodbc.Error as e:
        print("Error while performing the Silver table ingestion with reason: ", e.args)
        if connection:
            connection.rollback()
            print("> Rollback has been completed due to an error during silver layer ingestion")
    

result_load_silver = load_to_silver()
