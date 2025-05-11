import os
import requests
import pandas as pd 
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
target_file = os.path.join(os.getcwd(), 'Largest_banks_data.csv')
exchange_rates_file = os.path.join(os.getcwd(), 'exchange_rate.csv')
log_file = 'code_log.txt'
sql_db_name = 'Banks.db'
sql_table_name = 'Largest_banks'

exchange_rates_df = pd.read_csv(exchange_rates_file)
GBP_ER = exchange_rates_df[exchange_rates_df['Currency'] == 'GBP'].iloc[0, 1]
EUR_ER = exchange_rates_df[exchange_rates_df['Currency'] == 'EUR'].iloc[0, 1]
INR_ER = exchange_rates_df[exchange_rates_df['Currency'] == 'INR'].iloc[0, 1]

#-LOGGING--------------------
def log_progress(log_message):
    timestamp_format = '%Y-%b-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + log_message + '\n')
        print('\n' + log_message + '\n')

#-EXTRACTING-----------------

def extract():
    try:
        df_banks = pd.DataFrame(columns = ["Name", "MC_USD_Billion"])

        r = requests.get(url)
        if r.status_code == 200:
            html = BeautifulSoup(r.text, 'html.parser')
            table_html = html.find_all('tbody')[0]
            rows_in_table_html = table_html.find_all('tr')
            for row in rows_in_table_html:
                data = row.find_all('td')
                if len(data)!=0:
                    Name = data[1].find_all('a')[1].string
                    MC_USD_Billion = float(data[2].string)
                    new_df_entry = pd.DataFrame([{'Name': Name, 'MC_USD_Billion': MC_USD_Billion}])
                    df_banks = pd.concat([df_banks, new_df_entry], ignore_index=True)

            return df_banks

    except requests.exceptions.RequestException as errex:
        print("Request Exception Occurred.")

#-TRANSFORMING---------------

def transform(dataframe):
    try:
        dataframe['MC_GBP_Billion'] = round(dataframe['MC_USD_Billion'] * GBP_ER, 2)
        dataframe['MC_EUR_Billion'] = round(dataframe['MC_USD_Billion'] * EUR_ER, 2)
        dataframe['MC_INR_Billion'] = round(dataframe['MC_USD_Billion'] * INR_ER, 2)

        return dataframe

    except KeyError:
        print(f'Something went wrong. Please check that the following columns exist in your dataframe: MC_USD_Billion .')
    except TypeError:
        print(f'Something went wrong. Please check the data types of the columns in your dataframe.')

#-LOADING--------------------

def load_to_csv(target, data):
    data.to_csv(target)

def load_to_db(db_name, table_name, dataframe):
    conn = sqlite3.connect(db_name)
    dataframe.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

#-QUERYING-------------------
def query_for_london_office(db_name, table_name):
    conn = sqlite3.connect(db_name)
    query_for_london = f'SELECT Name, MC_GBP_Billion FROM {table_name}'
    london_data = pd.read_sql(query_for_london, conn)
    conn.close()

    return london_data

def query_for_berlin_office(db_name, table_name):
    conn = sqlite3.connect(db_name)
    query_for_berlin = f'SELECT Name, MC_EUR_Billion FROM {table_name}'
    berlin_data = pd.read_sql(query_for_berlin, conn)
    conn.close()

    return berlin_data

def query_for_newdelhi_office(db_name, table_name):
    conn = sqlite3.connect(db_name)
    query_for_newdelhi = f'SELECT Name, MC_INR_Billion FROM {table_name}'
    newdelhi_data = pd.read_sql(query_for_newdelhi, conn)
    conn.close()

    return newdelhi_data

#----------------------------
#-ETL PROCESS----------------

log_progress("Starting ETL Process.")

log_progress("Starting Extraction Process.")
df_banks = extract()
log_progress("Extraction Process Completed.")

log_progress("Data Transformation Started.")
df_banks = transform(df_banks)
log_progress("Data Transformation Completed.")

log_progress("Loading Data to .csv .")
load_to_csv(target_file, df_banks)
log_progress("Loading Data to .csv Completed.")

log_progress("Loading Data to SQL Database.")
load_to_db(sql_db_name, sql_table_name, df_banks)
log_progress("Loading Data to SQL Database Completed.")

log_progress("Querying Data for London Office.")
print(query_for_london_office(sql_db_name, sql_table_name))
log_progress("Data Retrieved for London Office.")

log_progress("Querying Data for Berlin Office.")
print(query_for_berlin_office(sql_db_name, sql_table_name))
log_progress("Data Retrieved for Berlin Office.")

log_progress("Querying Data for New Delhi Office.")
print(query_for_newdelhi_office(sql_db_name, sql_table_name))
log_progress("Data Retrieved for New Delhi Office.")

log_progress("ETL Process Completed.")

#print(df_banks)
#df_banks = transform(df_banks['Name']) #Test KeyError Exc in transform()
#df_banks["MC_USD_Billion"] = df_banks["MC_USD_Billion"].astype(str) #Test TypeError Exc in transform()