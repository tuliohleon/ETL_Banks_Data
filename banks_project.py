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

    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

#Declaring known values
log_progress('Preliminaries complete. Initiating ETL process')

#Call extract() function
log_progress('Data extraction complete. Initiating Transformation process')

#Call transform() function
log_progress('Data transformation complete. Initiating Loading process')

#Call load_to_csv() function
log_progress('Data saved to CSV file')

#Initiate SQLite3 connection
log_progress('	SQL Connection initiated')

#Call load_to_db() function
log_progress('Data loaded to Database as a table, Executing queries')

#Call run_query() function
log_progress('Process Complete')

#Close SQLite3 connection
log_progress('Server Connection closed')