import pyodbc

from ..utils.db import get_sql_connection


def load_to_gold():

    connection = None

    try:

        connection = get_sql_connection()              # connection to db
        cursor = connection.cursor()
        print("Connection to SQL Server successful")

        cursor.execute("""EXEC DataWarehouse.gold.load_gold""")        # Stored procedure call with folder_path as parameter

        connection.commit()
        print("> Commit completed - Views for Dimensions and Fact have been created in gold layer")

    except pyodbc.Error as e:
        print("Error while performing the view creation in gold layer with reason: ", e.args)
        if connection:
            connection.rollback()
            print("> Rollback has been completed due to an error during view creation in gold layer")
    

result_load_to_gold = load_to_gold()
