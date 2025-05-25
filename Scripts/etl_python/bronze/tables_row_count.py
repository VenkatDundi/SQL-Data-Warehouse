import pyodbc

from ..utils.db import get_sql_connection


def get_TableRowCount():

    connection = None
    try:
        connection = get_sql_connection()              # connection to db
        cursor = connection.cursor()
        print("Connection to SQL Server successful")

        cursor.execute("""EXEC DataWarehouse.bronze.sp_PrintTableRowCounts""")        # Stored procedure call with folder_path as parameter

        connection.commit()
        print(">>> Tables Row count has been retrived")

    except pyodbc.Error as e:
        print("Error while performing the table count retrieval with reason: ", e.args)
        if connection:
            connection.rollback()
            print("> Rollback has been completed due to an error during table count retrieval")
        return False
    return True

    
# *** Handled by Airflow in Docker ***
"""
    result_table_row_count = get_TableRowCount()
"""