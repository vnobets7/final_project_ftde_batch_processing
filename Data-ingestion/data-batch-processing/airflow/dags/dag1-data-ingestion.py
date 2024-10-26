from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin

import os
import pandas as pd

from pandas import DataFrame
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Column, Integer, String, Table

load_dotenv()

### Load environment variables from .env file
# Postgres configuration
pg_username = os.getenv('POSTGRES_STAGING_USER')
pg_password = os.getenv('POSTGRES_STAGING_PASSWORD')
pg_host = os.getenv('POSTGRES_STAGING_HOST')
pg_database = os.getenv('POSTGRES_STAGING_DB')
pg_port = os.getenv('POSTGRES_STAGING_PORT')

# Mysql configuration
mysql_username = os.getenv('MYSQL_USER')
mysql_root_password = os.getenv('MYSQL_ROOT_PASSWORD')
mysql_host = os.getenv('MYSQL_HOST')
mysql_port = os.getenv('MYSQL_PORT')
mysql_database = os.getenv('MYSQL_DB')

CUR_DIR = os.path.dirname(os.path.abspath(__file__))

# Function for relationalized object
def sqlalchemy_data_type_check(dtype):
    if "int" in dtype.name:
        return Integer
    elif "object" in dtype.name:
        return String(255)
    else:
        return String(255)

def insert_data_to_postgresql(df, table_name, db_url):
    try:
        engine = create_engine(db_url)

        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"******** Data telah dimasukkan ke tabel {table_name}.******** ")
    except Exception as e:
        print(f"Terjadi kesalahan: {e}")

def extract_and_load_to_staging(target_connection_name: str) -> DataFrame:
    """
    description:
    
    """
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    # initialize database connection
    if target_connection_name == 'postgresql':
        try:
            # read csv file and save into dataFrame
            data1_dir = CUR_DIR + "/data_dump/data_management_payroll_update.csv"
            data2_dir = CUR_DIR + "/data_dump/data_performance_management_update.csv"
            management_payroll_df = pd.read_csv(data1_dir)
            performance_management_df = pd.read_csv(data2_dir)
            
            # PostgreSQL connection setup
            postgres_db_url = f"postgresql://{pg_username}:{pg_password}@{pg_host}:{pg_port}/{pg_database}"
            postgres_engine = create_engine(postgres_db_url)
            
            # Initialize table name
            table_name_1 = "data_management_payroll"
            table_name_2 = "data_performance_management"
            
            # Reflect existing tables (you can also define the table)
            metadata = MetaData()
            
            #--- Create data_management_payroll table in postgreSQL ---
            postres_columns_1 = [Column(name, sqlalchemy_data_type_check(dtype)) for name, dtype in management_payroll_df.dtypes.items()]
            postgres_table_dmp = Table(table_name_1, metadata, 
                                      *postres_columns_1
            )
            postgres_table_dmp.create(postgres_engine)
            print(f"******** Success create {table_name_1} table in PostgreSQL. ******** \n")
            
            # Insert data into db_dump on postgreSQL
            insert_data_to_postgresql(management_payroll_df, table_name_1, postgres_db_url)
            
            
            #--- Create data_performance_management table in postgreSQL ---
            postres_columns_2 = [Column(name, sqlalchemy_data_type_check(dtype)) for name, dtype in performance_management_df.dtypes.items()]
            postgres_table_dpm = Table(table_name_2, metadata, 
                           *postres_columns_2
            )
            postgres_table_dpm.create(postgres_engine)
            print(f"******** Success create {table_name_2} table in PostgreSQL. ******** \n")
            
            # Insert data into db_dump on postgreSQL
            insert_data_to_postgresql(performance_management_df, table_name_2, postgres_db_url)
            
        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")
            raise
    elif target_connection_name == 'mysql':
        try:
            # read csv file and save into dataFrame
            data_dir = CUR_DIR + "/data_dump/data_training_development_update.csv"
            training_development_df = pd.read_csv(data_dir)
            
            # MySQL connection setup
            mysql_db_url = f"mysql://{mysql_username}:{mysql_root_password}@{mysql_host}:{mysql_port}/{mysql_database}"
            mysql_engine = create_engine(mysql_db_url)
            
            # Initialize table name
            table_name = "data_training_development"
            
            # Reflect existing tables (you can also define the table)
            metadata = MetaData()
            
            #--- Create data_training_development table in mySQL ---
            mysql_columns = [Column(name, sqlalchemy_data_type_check(dtype)) for name, dtype in training_development_df.dtypes.items()]
            mysql_table_dtd = Table(table_name, metadata, 
                                   *mysql_columns
            )
            mysql_table_dtd.create(mysql_engine)
            print(f"******** Success create {table_name} table in MySQL. ******** \n")
            # Insert data into db_dump on postgreSQL
            insert_data_to_postgresql(training_development_df, table_name, mysql_db_url)
            
        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")
            raise


with DAG(
    dag_id='dag1-data-ingestion',
    start_date=datetime(2024, 10, 9),
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:
    start_task = EmptyOperator(
        task_id='start'
    )
    
    op_extract_employee_data_to_postgres = PythonOperator(
        task_id='fun_extract_employee_data_to_postgres',        
        python_callable=extract_and_load_to_staging,
        op_kwargs={'target_connection_name': 'postgresql'}
    )
    
    op_extract_employee_data_to_mysql = PythonOperator(
        task_id='fun_extract_employee_data_to_mysql',        
        python_callable=extract_and_load_to_staging,
        op_kwargs={'target_connection_name': 'mysql'}
    )

    end_task = EmptyOperator(
        task_id='end'
    )
    
    start_task >> op_extract_employee_data_to_postgres >> end_task
    start_task >> op_extract_employee_data_to_mysql >> end_task