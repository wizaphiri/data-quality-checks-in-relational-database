#!/usr/bin/python3
from sqlalchemy import create_engine
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

print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + f": Attempting to connect to MySQL server at {hostname}...")
print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ': pre and post ETL analysis started')

try:
    connection = connect_to_database(hostname, username, password, None)
    cursor = connection.cursor()

    db_list_query = "SHOW DATABASES"
    databases_df = pd.read_sql_query(db_list_query, connection)
    rds_databases_df = databases_df[databases_df['Database'].str.startswith('openmrs_')]

    schemas_stats_query = """
    select 
    (select property_value from global_property gp where property = 'current_health_center_id')*1 as site_id,
    (select distinct name from location l where location_id = (select distinct property_value from global_property where property='current_health_center_id')) site_name,
    'obs' table_name,
    coalesce(count(*),0) record_count
    from obs 
    where voided = 0
    group by 
    (select property_value from global_property gp where property = 'current_health_center_id')*1 ,
    (select distinct name from location l where location_id = (select distinct property_value from global_property where property='current_health_center_id'))
union all
    select 
    (select property_value from global_property gp where property = 'current_health_center_id')*1 as site_id,
    (select distinct name from location l where location_id = (select distinct property_value from global_property where property='current_health_center_id')) site_name,
    'encounter' table_name,
    coalesce(count(*),0) record_count
    from encounter e
    where voided = 0
    group by 
    (select property_value from global_property gp where property = 'current_health_center_id')*1,
    (select distinct name from location l where location_id = (select distinct property_value from global_property where property='current_health_center_id'))
union all
    select 
    (select property_value from global_property gp where property = 'current_health_center_id')*1 as site_id,
    (select name from location l where location_id = (select distinct property_value from global_property where property='current_health_center_id')) site_name,
    'orders' table_name,
    coalesce(count(*),0) record_count
    from orders
    where voided = 0  
    group by 
    (select property_value from global_property gp where property = 'current_health_center_id')*1,
    (select name from location l where location_id = (select distinct property_value from global_property where property='current_health_center_id')) 
union all
    select
    (select property_value from global_property gp where property = 'current_health_center_id')*1 as site_id,
    (select name from location l where location_id = (select distinct property_value from global_property where property='current_health_center_id')) site_name,
    'person' table_name,
    coalesce(count(*),0) record_count 
    from person
    where voided = 0
    group by 
    (select property_value from global_property gp where property = 'current_health_center_id')*1,
    (select name from location l where location_id = (select distinct property_value from global_property where property='current_health_center_id')) 
union all
    select
    (select property_value from global_property gp where property = 'current_health_center_id')*1 as site_id,
    (select name from location l where location_id = (select distinct property_value from global_property where property='current_health_center_id')) site_name,
    'patient' table_name,
    coalesce(count(*),0) record_count 
    from patient 
    where voided = 0
    group by 
    (select property_value from global_property gp where property = 'current_health_center_id')*1,
    (select name from location l where location_id = (select distinct property_value from global_property where property='current_health_center_id')) 
union all
    select
    (select property_value from global_property gp where property = 'current_health_center_id')*1 as site_id,
    (select name from location l where location_id = (select distinct property_value from global_property where property='current_health_center_id')) site_name,
    'patient_state' table_name,
    coalesce(count(*),0) record_count 
    -- where voided = 0
    from patient_state 
    group by 
    (select property_value from global_property gp where property = 'current_health_center_id')*1,
    (select name from location l where location_id = (select distinct property_value from global_property where property='current_health_center_id'))"""

    column_names = ['table_name', 'record_count']
    site_schemas_df = pd.DataFrame(columns=column_names)

    for index, database in rds_databases_df.iterrows():
        database_name = database['Database']
        connection = None
        try:
            connection = connect_to_database(hostname, username, password, database_name)
            try:
                site_schemas_df_temp = pd.read_sql_query(schemas_stats_query, connection)
                site_schemas_df = site_schemas_df.append(site_schemas_df_temp, ignore_index=True)
            except DatabaseError as err:
                if "1146 (42S02)" in str(err):
                    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: Skipping database '{database_name}' - Table 'patient' does not exist.")
                else:
                    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: Error querying database '{database_name}' - {err}")
                    raise  # Re-raise any other database-related errors

        except mysql.connector.Error as err:
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: Error connecting to database '{database_name}' - {err}")
            raise  # Re-raise connection errors

        finally:
            if connection:
                connection.close()

    schema_stat = str(site_schemas_df.shape)
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ': source dataframe shape ' + schema_stat)
    
except mysql.connector.Error as err:
    print(f"Failed to connect to MySQL server: {err}")

#### Destination database

load_dotenv()

db_type = os.getenv('DB_TYPE')
user = os.getenv('DB_USER')
port = os.getenv('DB_PORT')
host = os.getenv('DB_HOSTNAME')
database = os.getenv('DB_DATABASE')
token = os.getenv('DB_TOKEN')

connection_string = f"{db_type}://{user}:{token}@{host}:{port}/{database}"
engine = create_engine(connection_string)

# breakdown queries to optimize
with engine.begin() as conn:
    ohdl_query1 = conn.exec_driver_sql(
    """
    select 
    site_id,
    'obs' table_name,
    coalesce(count(*),0) record_count
    from obs 
    where voided = 0 
    group by site_id""")
    ohdl_df1 = pd.DataFrame(ohdl_query1.fetchall(), columns=ohdl_query1.keys())

with engine.begin() as conn:
    ohdl_query2 = conn.exec_driver_sql(
    """
    select 
    site_id,
    'encounter' table_name,
    coalesce(count(*),0) record_count
    from encounter e 
    where voided = 0 
    group by site_id""")
    ohdl_df2 = pd.DataFrame(ohdl_query2.fetchall(), columns=ohdl_query2.keys())

with engine.begin() as conn:
    ohdl_query3 = conn.exec_driver_sql(
    """
    select 
    site_id,
    'orders' table_name,
    coalesce(count(*),0) record_count
    from orders 
    where voided = 0 
    group by site_id""")
    ohdl_df3 = pd.DataFrame(ohdl_query3.fetchall(), columns=ohdl_query3.keys())

with engine.begin() as conn:
    ohdl_query4 = conn.exec_driver_sql(
    """
    select 
    site_id,
    'person' table_name,
    coalesce(count(*),0) record_count
    from person  
    where voided = 0 
    group by site_id
union all
    select 
    site_id,
    'patient' table_name,
    coalesce(count(*),0) record_count
    from patient  
    where voided = 0 
    group by site_id
union all
    select 
    site_id,
    'patient_state' table_name,
    coalesce(count(*),0) record_count
    from patient_state 
    where site_id = 20
    group by site_id
    """)
    ohdl_df4 = pd.DataFrame(ohdl_query4.fetchall(), columns=ohdl_query4.keys())

ohdl_df = pd.concat([ohdl_df1, ohdl_df2, ohdl_df3, ohdl_df4], ignore_index=True)
print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ': ohdl dataframe shape ' + str(ohdl_df.shape))

common_columns = site_schemas_df.columns.intersection(ohdl_df.columns).tolist()

# merge dataframes on site_id & tab name
unique_identifiers = ['site_id', 'table_name']
merged_df = pd.merge(
    site_schemas_df, 
    ohdl_df, 
    on=unique_identifiers, 
    suffixes=('_source', '_ohdl'), 
    how='outer'
)

merged_df['variance'] = merged_df['record_count_source'] - merged_df['record_count_ohdl']
merged_df['date_created'] = datetime.now().date()

merged_df_order = ['site_id','site_name','table_name','record_count_source','record_count_ohdl','variance','date_created']
merged_df = merged_df[merged_df_order]

with engine.begin() as conn:
    conn.exec_driver_sql("DROP TABLE IF EXISTS temp_dqa_openmrs_etl")
    merged_df.to_sql('temp_dqa_openmrs_etl', engine, if_exists='append', index=False)

print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ': pre and post ETL analysis complete')
