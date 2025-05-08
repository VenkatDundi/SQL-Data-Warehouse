import requests
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import os
import pyodbc

from ..utils.db import get_sql_connection

url = "https://venkatdundi.github.io/Web-Scrapping/Github-Pages-Scraping/index.html"            # Github Static Webpage


def scrape_currencydata(url):

    try:
        req = requests.get(url)                         # Request the URL
        data, status = req.content, req.status_code     # Extract content, status code

        if status == 200:                               # Response - "OK"
            
            soup = bs(data, 'lxml')                     # lxml parser for formatting tree structure   
            
            table_data = soup.find('table', {'class' : 'ui celled striped inverted grey table'})    # Capture the Table using 'Class Name'
        
            header_data = table_data.find('thead').find_all('th')           # Extract all header cells
            headers = [i.text for i in header_data]                         
            
            row_data = table_data.find('tbody').find_all('tr')                      # Extract all rows in the table body
            cells_data = [[] for i in range(len(row_data[0].find_all('td')))]       # Create Empty lists to hold values of each column

            for i in range(len(row_data)):                                          # Traverse through the Number of rows in table body
                for j in range(len(row_data[i].find_all('td'))):                    # Traverse through the <td> cells for each row
                    cells_data[j].append(row_data[i].find_all('td')[j].text)        # Extract the text from each cell and save it in separate list
            
            Data_dictionary = {}                                                    # Dict with Headers as keys and corresponding columns as Values

            for i in range(len(headers)):
                Data_dictionary[headers[i]] = cells_data[i]                         

            df = pd.DataFrame(Data_dictionary)                                      # Dictionary to Data Frame

            df['Currency Symbol'] = df['Currency Symbol'].astype(str)
            #print(df)
            #print(df.info())
            csv_file = 'CurrencyData.csv'
            df.to_csv(csv_file, index=False, encoding='utf-8-sig')        # Encoding helps in holding the formatting for currency symbols

            
            for r, dirs,file in os.walk(os.getcwd()):
                for f in file:
                    if f == csv_file:
                        return os.path.join(r, f)
            
        else:
            return ("Error due to: {}", status)
    
    except requests.exceptions.RequestException as e:
        print("Error in data extraction from a Web Page:")
        print(type(e).__name__, "→", str(e))



def ingest_web_category(web_data):

    connection = None
    try:
        
        connection = get_sql_connection()          # Establish the connection
        cursor = connection.cursor()

        web_data = f"'{web_data}'"


        print("Connection to SQL Server successful")
        cursor.execute("TRUNCATE TABLE DataWarehouse.bronze.fin_CurrencyData;")          # Truncate the existing table
        print("Truncated Table - fin_CurrencyData")

        # Bulk Insert the data extract from web page to the SQL server table
        bulk_insert_query = f"""BULK INSERT bronze.fin_CurrencyData
                    FROM {web_data}
                    WITH (
                        FORMAT = 'CSV',
                        FIRSTROW = 2,
                        FIELDTERMINATOR = ',',
                        ROWTERMINATOR = '\\n',
                        CODEPAGE = '65001',
                        KEEPNULLS
                    );
                    """
        cursor.execute(bulk_insert_query)
        connection.commit()
        print("Commit completed - Ingestion of web data - category")
    
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Error connecting to SQL Server: {sqlstate}")
    except Exception as e:
        if connection:
            connection.rollback()
            print(f"Performed Rollback!!, Error Reason: {e.args[0]}")

    finally:
        if connection:
            connection.close()
            print("Connection closed")




web_data = scrape_currencydata(url)         # complete file path of the generated csv file --- Required for bulk insert to table

if web_data.endswith('CurrencyData.csv'):
    result = ingest_web_category(web_data)
else:
    print("Failed to extract data from API")