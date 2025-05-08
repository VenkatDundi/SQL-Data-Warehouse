import requests
from bs4 import BeautifulSoup as bs
import pandas as pd

url = "https://venkatdundi.github.io/Web-Scrapping/Github-Pages-Scraping/index.html"    # Github Page Static Webpage

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
    df.to_csv('CurrencyData.csv', index=False, encoding='utf-8-sig')        # Encoding helps in holding the formatting for symbols

else:
    print("Error due to: {}", status)
    
