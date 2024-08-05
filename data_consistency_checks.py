#!/usr/bin/python3
from sqlalchemy import create_engine, text
import os
import mysql.connector
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from pandas.io.sql import DatabaseError

import warnings
warnings.filterwarnings('ignore')

# define custom exception
class TableNotFoundError(Exception):
    def __init__(self, database_name, table_name):
        super().__init__(f"Table '{table_name}' not found in database '{database_name}'.")
        self.database_name = database_name
        self.table_name = table_name

def connect_to_database(hostname, username, password, database, port=3306):
    return mysql.connector.connect(
        host=hostname,
        user=username,
        password=password,
        database=database,
        port=port
    )

load_dotenv()
hostname = os.getenv('DB_HOSTNAME')
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')

print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + f": scanning openmrs schemas on {hostname} mysql instance")
try:
    connection = connect_to_database(hostname, username, password, None)
    cursor = connection.cursor()

    db_list_query = "SHOW DATABASES"
    databases_df = pd.read_sql_query(db_list_query, connection)
    rds_databases_df = databases_df[databases_df['Database'].str.startswith('openmrs_')]

    loading_status_query = """
        SELECT 
            (SELECT property_value FROM global_property gp WHERE property = 'current_health_center_id') AS facility_id,
            (SELECT DISTINCT name FROM location l WHERE location_id = (SELECT DISTINCT property_value FROM global_property WHERE property='current_health_center_id')) AS facility_name,
            'obs' AS table_name,
            COALESCE(COUNT(*), 0) AS record_count,
            MAX(DATE(obs_datetime)) AS max_date,
            QUARTER(NOW())-1 AS reporting_quarter 
        FROM obs o 
        WHERE obs_datetime < NOW()
        
        UNION ALL 
        
        SELECT 
            (SELECT property_value FROM global_property gp WHERE property = 'current_health_center_id') AS facility_id,
            (SELECT DISTINCT name FROM location l WHERE location_id = (SELECT DISTINCT property_value FROM global_property WHERE property='current_health_center_id')) AS facility_name,
            'encounter' AS table_name,
            COALESCE(COUNT(*), 0) AS record_count,
            MAX(DATE(encounter_datetime)) AS max_date,
            QUARTER(NOW())-1 AS reporting_quarter 
        FROM encounter e 
        WHERE encounter_datetime < NOW()
        
        UNION ALL 
        
        SELECT 
            (SELECT property_value FROM global_property gp WHERE property = 'current_health_center_id') AS facility_id,
            (SELECT DISTINCT name FROM location l WHERE location_id = (SELECT DISTINCT property_value FROM global_property WHERE property='current_health_center_id')) AS facility_name,
            'orders' AS table_name,
            COALESCE(COUNT(*), 0) AS record_count,
            MAX(DATE(start_date)) AS max_date,
            QUARTER(NOW())-1 AS reporting_quarter 
        FROM orders r
        WHERE start_date < NOW()
    """

    column_names = ['facility_id', 'facility_name', 'table_name', 'record_count', 'max_date', 'reporting_quarter']
    loading_status_df = pd.DataFrame(columns=column_names)

    xcountr = int(rds_databases_df.shape[0])
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ':', str(xcountr) + ' openmrs schemas selected for processing')

    for index, database in rds_databases_df.iterrows():
        database_name = database['Database']
        connection = None
        try:
            connection = connect_to_database(hostname, username, password, database_name)
            try:
                loading_status = pd.read_sql_query(loading_status_query, connection)
                loading_status_df = pd.concat([loading_status_df, loading_status], ignore_index=True)
            except DatabaseError as err:
                if "1146 (42S02)" in str(err):
                    # Check which table is missing by inspecting the SQL query and error message
                    if 'obs' in str(err):
                        missing_table = 'obs'
                    elif 'encounter' in str(err):
                        missing_table = 'encounter'
                    elif 'orders' in str(err):
                        missing_table = 'orders'
                    else:
                        missing_table = 'unknown'

                    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: Skipping database '{database_name}' - Table '{missing_table}' does not exist.")
                else:
                    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: Error querying database '{database_name}' - {err}")
                    raise  # Re-raise any other database-related errors

        except mysql.connector.Error as err:
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: Error connecting to database '{database_name}' - {err}")
            raise  # Re-raise connection errors

        finally:
            if connection:
                connection.close()

    schema_stat = str(loading_status_df.shape)
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ': dataframe shape ' + schema_stat)

except mysql.connector.Error as err:
    print(f"Failed to connect to MySQL server: {err}")

dqa_df = pd.DataFrame(loading_status_df, columns=['facility_id', 'facility_name', 'table_name', 'max_date'])
pivot_df = dqa_df.pivot_table(index=['facility_id', 'facility_name'], columns='table_name', values='max_date', aggfunc='max')
pivot_df = pivot_df.reset_index()

# rename columns to add '_max_date' suffix
new_column_names = {col: col + '_max_date' if col not in ['facility_id', 'facility_name'] else col for col in pivot_df.columns}
pivot_df.rename(columns=new_column_names, inplace=True)

pivot_df['encounter_max_datex'] = pd.to_datetime(pivot_df['encounter_max_date'])
pivot_df['obs_max_datex'] = pd.to_datetime(pivot_df['obs_max_date'])
pivot_df['orders_max_datex'] = pd.to_datetime(pivot_df['orders_max_date'])

pivot_df['date1_ordinal'] = pivot_df['encounter_max_datex'].map(pd.Timestamp.toordinal)
pivot_df['date2_ordinal'] = pivot_df['obs_max_datex'].map(pd.Timestamp.toordinal)
pivot_df['date3_ordinal'] = pivot_df['orders_max_datex'].map(pd.Timestamp.toordinal)

pivot_df['std_dev'] = pivot_df[['date1_ordinal', 'date2_ordinal', 'date3_ordinal']].std(axis=1)
pivot_df['std_dev'] = pivot_df['std_dev'].round()

pivot_df = pivot_df.reset_index()
pivot_df = pivot_df[['facility_id', 'facility_name', 'encounter_max_date', 'obs_max_date', 'orders_max_date', 'std_dev']]

ycountr = int(pivot_df.shape[0])
print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ':', str(ycountr) + ' out of', str(xcountr), 'openmrs schemas processed successfully')

pivot_df['date_created'] = datetime.now().date()

load_dotenv()

db_type = os.getenv('DB_TYPE')
user = os.getenv('DB_USER')
port = os.getenv('DB_PORT')
host = os.getenv('DB_HOSTNAME')
database = os.getenv('DB_DATABASE')
token = os.getenv('DB_TOKEN')

connection_string = f"{db_type}://{user}:{token}@{host}:{port}/{database}"
engine = create_engine(connection_string)

with engine.begin() as conn:
    conn.execute("DROP TABLE IF EXISTS temp_dqa_openmrs_schemas")
    pivot_df.to_sql('temp_dqa_openmrs_schemas', engine, if_exists='replace', index=False)
    result = conn.execute(text("SELECT COUNT(*) FROM temp_dqa_openmrs_schemas"))
    record_count = result.scalar()
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: {record_count} records inserted into 'temp_dqa_openmrs_schemas'.")
    
print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ': openmrs schema analysis complete')
