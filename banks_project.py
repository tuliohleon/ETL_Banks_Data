from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

# Code for ETL operations on Country-GDP data

# Importing the required libraries

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    with open('code_log.txt', 'a') as f:
        f.write(f'{datetime.now()}: {message}\n')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    rows_to_add = []
    for row in rows:
         cells = row.find_all('td')
         if len(cells) > 2 and cells[1].find('a'):
          bank = cells[1].text.strip()
          mc_usd_billions = float(cells[2].text.strip())
          entry = {
                 table_attribs[0]: bank,
                 table_attribs[1]: mc_usd_billions if mc_usd_billions else None
                 }
          rows_to_add.append(entry)
    df = pd.concat([df, pd.DataFrame(rows_to_add)], ignore_index=True)
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    exchange_rate_df=pd.read_csv(csv_path)
    exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    query_result  = pd.read_sql_query(query_statement, sql_connection)
    print(query_statement)
    print(query_result)

#Declaring known values
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './exchange_rate.csv'
output_path = './Largest_banks_data.csv'

log_progress('Preliminaries complete. Initiating ETL process')

#Call extract() function
df=extract(url, table_attribs)
print(df)
log_progress('Data extraction complete. Initiating Transformation process')

#Call transform() function
df=transform(df, csv_path)
print(df)
log_progress('Data transformation complete. Initiating Loading process')

#Call load_to_csv() function
load_to_csv(df, output_path)
log_progress('Data saved to CSV file')

#Initiate SQLite3 connection
sql_connection = sqlite3.connect(db_name)
log_progress('	SQL Connection initiated')

#Call load_to_db() function
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as a table, Executing queries')

#Call run_query() function
query_statement = 'SELECT * FROM Largest_banks'
run_query(query_statement, sql_connection)
query_statement = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
run_query(query_statement, sql_connection)
query_statement = 'SELECT Name from Largest_banks LIMIT 5'
run_query(query_statement, sql_connection)
log_progress('Process Complete')

#Close SQLite3 connection
log_progress('Server Connection closed')