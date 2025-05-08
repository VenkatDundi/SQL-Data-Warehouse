import pyodbc

# Connection details
server = 'Venkat'
database = 'DataWarehouse'
driver = 'ODBC Driver 18 for SQL Server' # ODBC Driver 18

# Connection string (using Windows Authentication)
connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;TrustServerCertificate=yes;'

def get_sql_connection():
    conn = pyodbc.connect(connection_string)
    return conn