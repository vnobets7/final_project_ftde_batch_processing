from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin

from datetime import datetime
from sqlalchemy import create_engine, MetaData, Column, Integer, String, Table
from dotenv import load_dotenv

import os
import psycopg2
import pandas as pd

load_dotenv()
CUR_DIR = os.path.dirname(os.path.abspath(__file__))

### Load environment variables from .env file
# staging db configuration
staging_pg_username = os.getenv('POSTGRES_STAGING_USER')
staging_pg_password = os.getenv('POSTGRES_STAGING_PASSWORD')
staging_pg_host = os.getenv('POSTGRES_STAGING_HOST')
staging_pg_database = os.getenv('POSTGRES_STAGING_DB')
staging_pg_port = os.getenv('POSTGRES_STAGING_PORT')

# Data warehouse configuration
dwh_pg_username = os.getenv('POSTGRES_DWH_USER')
dwh_pg_password = os.getenv('POSTGRES_DWH_PASSWORD')
dwh_pg_host = os.getenv('POSTGRES_DWH_HOST')
dwh_pg_database = os.getenv('POSTGRES_DWH_DB')
dwh_pg_port = os.getenv('POSTGRES_DWH_PORT')

def read_sql_file(file_path):
    """
    Description: Read SQL file and return the query as a string.
    """
    with open(file_path, 'r') as file:
        return file.read()

def sqlalchemy_data_type_check(dtype):
    """
    Description: Function for relationalized object.
    """
    if "int" in dtype.name:
        return Integer
    elif "object" in dtype.name:
        return String(255)
    else:
        return String(255)

def insert_data_to_postgresql(df, table_name, db_url):
    """
    Description: Function for Create table, and insert data from dataFrame into table.
    """
    try:
        engine = create_engine(db_url)

        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"******** Success insert data into {table_name}!******** ")
    except Exception as e:
        print(f"An error occurred: {e}")


def extract_data_employee_staging(query_file):
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    staging_conn = psycopg2.connect(
            database = staging_pg_database,
            user = staging_pg_username,
            password = staging_pg_password,
            host = staging_pg_host,
            port = staging_pg_port
    )
    # read .sql file on 'query' folder
    emp_query = read_sql_file(query_file)
    df_emp = pd.read_sql(emp_query, staging_conn)
    
    # Create new directory structure, but don't raise an error if it already exists
    output_file_name = "output_data_employee.csv"
    output_path_name = "output_data_staging"
    new_output_path = os.makedirs("/airflow/dags/" + output_path_name, exist_ok=True)
    
    # Save the DataFrame to a CSV file within the new folder
    csv_file_path = os.path.join(new_output_path, output_file_name)
    df_emp.to_csv(csv_file_path, index=False)
    
    # Close staging_db connection
    staging_conn.close()
    print(f"Employee data has been export into {csv_file_path}!")
    
def extract_data_job_salary_staging(query_file):
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    staging_conn = psycopg2.connect(
            database = staging_pg_database,
            user = staging_pg_username,
            password = staging_pg_password,
            host = staging_pg_host,
            port = staging_pg_port
    )
    # read .sql file on 'query' folder
    emp_query = read_sql_file(query_file)
    df_emp = pd.read_sql(emp_query, staging_conn)
    
    # Create new directory structure, but don't raise an error if it already exists
    output_file_name = "output_data_job_salary.csv"
    output_path_name = "output_data_staging"
    new_output_path = os.makedirs("/airflow/dags/" + output_path_name, exist_ok=True)
    
    # Save the DataFrame to a CSV file within the new folder
    csv_file_path = os.path.join(new_output_path, output_file_name)
    df_emp.to_csv(csv_file_path, index=False)
    
    # Close staging_db connection
    staging_conn.close()
    print(f"Job salary data has been export into {csv_file_path}!")

def extract_data_employee_salary_staging(query_file):
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    staging_conn = psycopg2.connect(
            database = staging_pg_database,
            user = staging_pg_username,
            password = staging_pg_password,
            host = staging_pg_host,
            port = staging_pg_port
    )
    # read .sql file on 'query' folder
    emp_query = read_sql_file(query_file)
    df_emp = pd.read_sql(emp_query, staging_conn)
    
    # Create new directory structure, but don't raise an error if it already exists
    output_file_name = "output_data_emp_salary.csv"
    output_path_name = "output_data_staging"
    new_output_path = os.makedirs("/airflow/dags/" + output_path_name, exist_ok=True)
    
    # Save the DataFrame to a CSV file within the new folder
    csv_file_path = os.path.join(new_output_path, output_file_name)
    df_emp.to_csv(csv_file_path, index=False)
    
    # Close staging_db connection
    staging_conn.close()
    print(f"Employee salary data has been export into {csv_file_path}!")

def extract_data_training_dev_staging(query_file):
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    staging_conn = psycopg2.connect(
            database = staging_pg_database,
            user = staging_pg_username,
            password = staging_pg_password,
            host = staging_pg_host,
            port = staging_pg_port
    )
    # read .sql file on 'query' folder
    emp_query = read_sql_file(query_file)
    df_emp = pd.read_sql(emp_query, staging_conn)
    
    # Create new directory structure, but don't raise an error if it already exists
    output_file_name = "output_data_training_dev.csv"
    output_path_name = "output_data_staging"
    new_output_path = os.makedirs("/airflow/dags/" + output_path_name, exist_ok=True)
    
    # Save the DataFrame to a CSV file within the new folder
    csv_file_path = os.path.join(new_output_path, output_file_name)
    df_emp.to_csv(csv_file_path, index=False)
    
    # Close staging_db connection
    staging_conn.close()
    print(f"Training development data has been export into {csv_file_path}!")

def extract_data_employee_training_dev_staging(query_file):
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    staging_conn = psycopg2.connect(
            database = staging_pg_database,
            user = staging_pg_username,
            password = staging_pg_password,
            host = staging_pg_host,
            port = staging_pg_port
    )
    # read .sql file on 'query' folder
    emp_query = read_sql_file(query_file)
    df_emp = pd.read_sql(emp_query, staging_conn)
    
    # Create new directory structure, but don't raise an error if it already exists
    output_file_name = "output_data_employee_training_dev.csv"
    output_path_name = "output_data_staging"
    new_output_path = os.makedirs("/airflow/dags/" + output_path_name, exist_ok=True)
    
    # Save the DataFrame to a CSV file within the new folder
    csv_file_path = os.path.join(new_output_path, output_file_name)
    df_emp.to_csv(csv_file_path, index=False)
    
    # Close staging_db connection
    staging_conn.close()
    print(f"Employee training development data has been export into {csv_file_path}!")
    
def extract_data_employee_training_performance_staging(query_file):
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    staging_conn = psycopg2.connect(
            database = staging_pg_database,
            user = staging_pg_username,
            password = staging_pg_password,
            host = staging_pg_host,
            port = staging_pg_port
    )
    # read .sql file on 'query' folder
    emp_query = read_sql_file(query_file)
    df_emp = pd.read_sql(emp_query, staging_conn)
    
    # Create new directory structure, but don't raise an error if it already exists
    output_file_name = "output_data_employee_training_performance.csv"
    output_path_name = "output_data_staging"
    new_output_path = os.makedirs("/airflow/dags/" + output_path_name, exist_ok=True)
    
    # Save the DataFrame to a CSV file within the new folder
    csv_file_path = os.path.join(new_output_path, output_file_name)
    df_emp.to_csv(csv_file_path, index=False)
    
    # Close staging_db connection
    staging_conn.close()
    print(f"Employee training performance data has been export into {csv_file_path}!")

def extract_data_employee_training_performance_review_staging(query_file):
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    staging_conn = psycopg2.connect(
            database = staging_pg_database,
            user = staging_pg_username,
            password = staging_pg_password,
            host = staging_pg_host,
            port = staging_pg_port
    )
    # read .sql file on 'query' folder
    emp_query = read_sql_file(query_file)
    df_emp = pd.read_sql(emp_query, staging_conn)
    
    # Create new directory structure, but don't raise an error if it already exists
    output_file_name = "output_data_employee_training_performance_review.csv"
    output_path_name = "output_data_staging"
    new_output_path = os.makedirs("/airflow/dags/" + output_path_name, exist_ok=True)
    
    # Save the DataFrame to a CSV file within the new folder
    csv_file_path = os.path.join(new_output_path, output_file_name)
    df_emp.to_csv(csv_file_path, index=False)
    
    # Close staging_db connection
    staging_conn.close()
    print(f"Employee training performance review data has been export into {csv_file_path}!")

def load_data_employee_dim(csv_file):
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    
    # read csv file and save into dataFrame
    data_dir = csv_file
    employee_df = pd.read_csv(data_dir)
            
    # PostgreSQL connection setup
    postgres_db_url = f"postgresql://{dwh_pg_username}:{dwh_pg_password}@{dwh_pg_host}:{dwh_pg_port}/{dwh_pg_database}"
    try:
            # Start dwh connection
            postgres_engine = create_engine(postgres_db_url)
            
            # Initialize table name
            table_name = "dim_tbl_emp"
            
            # Reflect existing tables (you can also define the table)
            metadata = MetaData()
            
            #--- Create data_employee dim table on dwh postgreSQL ---
            postres_columns = [Column(name, sqlalchemy_data_type_check(dtype)) for name, dtype in employee_df.dtypes.items()]
            postgres_table_dmp = Table(table_name, metadata, 
                                      *postres_columns
            )
            postgres_table_dmp.create(postgres_engine)
            print(f"******** Success create {table_name} table in PostgreSQL! ******** \n")
            
            # Insert data into data warehouse on postgreSQL
            insert_data_to_postgresql(employee_df, table_name, postgres_db_url)
    except Exception as e:
            logger.error(f"An error occurred: {str(e)}")
            raise

def load_data_job_salary_dim(csv_file):
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    
    # read csv file and save into dataFrame
    data_dir = csv_file
    job_salary_df = pd.read_csv(data_dir)
            
    # PostgreSQL connection setup
    postgres_db_url = f"postgresql://{dwh_pg_username}:{dwh_pg_password}@{dwh_pg_host}:{dwh_pg_port}/{dwh_pg_database}"
    try:
            # Start dwh connection
            postgres_engine = create_engine(postgres_db_url)
            
            # Initialize table name
            table_name = "dim_tbl_job_salary"
            
            # Reflect existing tables (you can also define the table)
            metadata = MetaData()
            
            #--- Create data_employee_training_dev dim table on dwh postgreSQL ---
            postres_columns = [Column(name, sqlalchemy_data_type_check(dtype)) for name, dtype in job_salary_df.dtypes.items()]
            postgres_table_dmp = Table(table_name, metadata, 
                                      *postres_columns
            )
            postgres_table_dmp.create(postgres_engine)
            print(f"******** Success create {table_name} table in PostgreSQL! ******** \n")
            
            # Insert data into data warehouse on postgreSQL
            insert_data_to_postgresql(job_salary_df, table_name, postgres_db_url)
    except Exception as e:
            logger.error(f"An error occurred: {str(e)}")
            raise
        
def load_data_employee_salary_dim(csv_file):
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    
    # read csv file and save into dataFrame
    data_dir = csv_file
    emp_salary_df = pd.read_csv(data_dir)
            
    # PostgreSQL connection setup
    postgres_db_url = f"postgresql://{dwh_pg_username}:{dwh_pg_password}@{dwh_pg_host}:{dwh_pg_port}/{dwh_pg_database}"
    try:
            # Start dwh connection
            postgres_engine = create_engine(postgres_db_url)
            
            # Initialize table name
            table_name = "dim_tbl_emp_salary"
            
            # Reflect existing tables (you can also define the table)
            metadata = MetaData()
            
            #--- Create data_employee_training_dev dim table on dwh postgreSQL ---
            postres_columns = [Column(name, sqlalchemy_data_type_check(dtype)) for name, dtype in emp_salary_df.dtypes.items()]
            postgres_table_dmp = Table(table_name, metadata, 
                                      *postres_columns
            )
            postgres_table_dmp.create(postgres_engine)
            print(f"******** Success create {table_name} table in PostgreSQL! ******** \n")
            
            # Insert data into data warehouse on postgreSQL
            insert_data_to_postgresql(emp_salary_df, table_name, postgres_db_url)
    except Exception as e:
            logger.error(f"An error occurred: {str(e)}")
            raise

def load_data_training_dev_dim(csv_file):
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    
    # read csv file and save into dataFrame
    data_dir = csv_file
    training_dev_df = pd.read_csv(data_dir)
            
    # PostgreSQL connection setup
    postgres_db_url = f"postgresql://{dwh_pg_username}:{dwh_pg_password}@{dwh_pg_host}:{dwh_pg_port}/{dwh_pg_database}"
    try:
            # Start dwh connection
            postgres_engine = create_engine(postgres_db_url)
            
            # Initialize table name
            table_name = "dim_tbl_training_dev"
            
            # Reflect existing tables (you can also define the table)
            metadata = MetaData()
            
            #--- Create data_employee_training_dev dim table on dwh postgreSQL ---
            postres_columns = [Column(name, sqlalchemy_data_type_check(dtype)) for name, dtype in training_dev_df.dtypes.items()]
            postgres_table_dmp = Table(table_name, metadata, 
                                      *postres_columns
            )
            postgres_table_dmp.create(postgres_engine)
            print(f"******** Success create {table_name} table in PostgreSQL! ******** \n")
            
            # Insert data into data warehouse on postgreSQL
            insert_data_to_postgresql(training_dev_df, table_name, postgres_db_url)
    except Exception as e:
            logger.error(f"An error occurred: {str(e)}")
            raise
        
def load_data_employee_training_dev_dim(csv_file):
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    
    # read csv file and save into dataFrame
    data_dir = csv_file
    employee_training_dev_df = pd.read_csv(data_dir)
            
    # PostgreSQL connection setup
    postgres_db_url = f"postgresql://{dwh_pg_username}:{dwh_pg_password}@{dwh_pg_host}:{dwh_pg_port}/{dwh_pg_database}"
    try:
            # Start dwh connection
            postgres_engine = create_engine(postgres_db_url)
            
            # Initialize table name
            table_name = "dim_tbl_emp_training_dev"
            
            # Reflect existing tables (you can also define the table)
            metadata = MetaData()
            
            #--- Create data_employee_training_dev dim table on dwh postgreSQL ---
            postres_columns = [Column(name, sqlalchemy_data_type_check(dtype)) for name, dtype in employee_training_dev_df.dtypes.items()]
            postgres_table_dmp = Table(table_name, metadata, 
                                      *postres_columns
            )
            postgres_table_dmp.create(postgres_engine)
            print(f"******** Success create {table_name} table in PostgreSQL! ******** \n")
            
            # Insert data into data warehouse on postgreSQL
            insert_data_to_postgresql(employee_training_dev_df, table_name, postgres_db_url)
    except Exception as e:
            logger.error(f"An error occurred: {str(e)}")
            raise
        
def load_data_employee_training_performance_dim(csv_file):
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    
    # read csv file and save into dataFrame
    data_dir = csv_file
    employee_training_performance_df = pd.read_csv(data_dir)
            
    # PostgreSQL connection setup
    postgres_db_url = f"postgresql://{dwh_pg_username}:{dwh_pg_password}@{dwh_pg_host}:{dwh_pg_port}/{dwh_pg_database}"
    try:
            # Start dwh connection
            postgres_engine = create_engine(postgres_db_url)
            
            # Initialize table name
            table_name = "dim_tbl_emp_training_performance"
            
            # Reflect existing tables (you can also define the table)
            metadata = MetaData()
            
            #--- Create data_employee_training_dev dim table on dwh postgreSQL ---
            postres_columns = [Column(name, sqlalchemy_data_type_check(dtype)) for name, dtype in employee_training_performance_df.dtypes.items()]
            postgres_table_dmp = Table(table_name, metadata, 
                                      *postres_columns
            )
            postgres_table_dmp.create(postgres_engine)
            print(f"******** Success create {table_name} table in PostgreSQL! ******** \n")
            
            # Insert data into data warehouse on postgreSQL
            insert_data_to_postgresql(employee_training_performance_df, table_name, postgres_db_url)
    except Exception as e:
            logger.error(f"An error occurred: {str(e)}")
            raise
        
def load_data_employee_training_performance_review_dim(csv_file):
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    
    # read csv file and save into dataFrame
    data_dir = csv_file
    employee_training_performance_review_df = pd.read_csv(data_dir)
            
    # PostgreSQL connection setup
    postgres_db_url = f"postgresql://{dwh_pg_username}:{dwh_pg_password}@{dwh_pg_host}:{dwh_pg_port}/{dwh_pg_database}"
    try:
            # Start dwh connection
            postgres_engine = create_engine(postgres_db_url)
            
            # Initialize table name
            table_name = "dim_tbl_employee_training_performance_review"
            
            # Reflect existing tables (you can also define the table)
            metadata = MetaData()
            
            #--- Create data_employee_training_dev dim table on dwh postgreSQL ---
            postres_columns = [Column(name, sqlalchemy_data_type_check(dtype)) for name, dtype in employee_training_performance_review_df.dtypes.items()]
            postgres_table_dmp = Table(table_name, metadata, 
                                      *postres_columns
            )
            postgres_table_dmp.create(postgres_engine)
            print(f"******** Success create {table_name} table in PostgreSQL! ******** \n")
            
            # Insert data into data warehouse on postgreSQL
            insert_data_to_postgresql(employee_training_performance_review_df, table_name, postgres_db_url)
    except Exception as e:
            logger.error(f"An error occurred: {str(e)}")
            raise

with DAG(
    dag_id='dag2-data-ingestion-dwh',
    start_date=datetime(2024, 10, 9),
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:
    start_task = EmptyOperator(
        task_id='start'
    )
    
    op_extract_data_employee_staging = PythonOperator(
        task_id='fun_extract_data_employee_staging',        
        python_callable=extract_data_employee_staging,
        op_kwargs={'query_file': CUR_DIR + '/query/payroll_1_query.sql'}
    )
    
    op_extract_data_job_salary_staging = PythonOperator(
        task_id='fun_extract_data_job_salary_staging',        
        python_callable=extract_data_job_salary_staging,
        op_kwargs={'query_file': CUR_DIR + '/query/payroll_2_query.sql'}
    )
    
    op_extract_data_employee_salary_staging = PythonOperator(
        task_id='fun_extract_data_emp_salary_staging',        
        python_callable=extract_data_employee_salary_staging,
        op_kwargs={'query_file': CUR_DIR + '/query/payroll_3_query.sql'}
    )
    
    op_extract_data_training_dev_staging = PythonOperator(
        task_id='fun_extract_data_training_dev_staging',        
        python_callable=extract_data_training_dev_staging,
        op_kwargs={'query_file': CUR_DIR + '/query/training_1_query.sql'}
    )
    
    op_extract_data_employee_training_dev_staging = PythonOperator(
        task_id='fun_extract_data_employee_training_dev_staging',        
        python_callable=extract_data_employee_training_dev_staging,
        op_kwargs={'query_file': CUR_DIR + '/query/training_2_query.sql'}
    )
    
    op_extract_data_employee_training_performance_staging = PythonOperator(
        task_id='fun_extract_data_employee_training_performance_staging',        
        python_callable=extract_data_employee_training_performance_staging,
        op_kwargs={'query_file': CUR_DIR + '/query/training_3_query.sql'}
    )
    
    op_extract_data_employee_training_performance_review_staging = PythonOperator(
        task_id='fun_extract_data_employee_training_performance_review_staging',        
        python_callable=extract_data_employee_training_performance_review_staging,
        op_kwargs={'query_file': CUR_DIR + '/query/training_4_query.sql'}
    )
    
    op_load_dim_employee_dwh = PythonOperator(
        task_id='fun_load_dim_employee_dwh',
        python_callable=load_data_employee_dim,
        op_kwargs={'csv_file': CUR_DIR + '/output_data_staging/output_data_employee.csv'}
    )
    
    op_load_dim_job_salary_dwh = PythonOperator(
        task_id='fun_load_dim_job_salary_dwh',
        python_callable=load_data_job_salary_dim,
        op_kwargs={'csv_file': CUR_DIR + '/output_data_staging/output_data_job_salary.csv'}
    )
    
    op_load_dim_employee_salary_dwh = PythonOperator(
        task_id='fun_load_dim_emp_salary_dwh',
        python_callable=load_data_employee_salary_dim,
        op_kwargs={'csv_file': CUR_DIR + '/output_data_staging/output_data_emp_salary.csv'}
    )
    
    op_load_dim_training_dev_dwh = PythonOperator(
        task_id='fun_load_dim_training_dev_dwh',
        python_callable=load_data_training_dev_dim,
        op_kwargs={'csv_file': CUR_DIR + '/output_data_staging/output_data_training_dev.csv'}
    )
    
    op_load_dim_employee_training_dev_dwh = PythonOperator(
        task_id='fun_load_dim_employee_training_dev_dwh',        
        python_callable=load_data_employee_training_dev_dim,
        op_kwargs={'query_file': CUR_DIR + '/output_data_staging/output_data_employee_training_dev.csv'}
    )
    
    op_load_dim_employee_training_performance_dwh = PythonOperator(
        task_id='fun_load_dim_employee_training_performance_dwh',        
        python_callable=load_data_employee_training_performance_dim,
        op_kwargs={'query_file': CUR_DIR + '/output_data_staging/output_data_employee_training_performance.csv'}
    )
    
    op_load_dim_employee_training_performance_review_dwh = PythonOperator(
        task_id='fun_load_dim_employee_training_performance_review_dwh',        
        python_callable=load_data_employee_training_performance_review_dim,
        op_kwargs={'query_file': CUR_DIR + '/output_data_staging/output_data_employee_training_performance_review.csv'}
    )
        
    end_task = EmptyOperator(
        task_id='end'
    )
    
    start_task >> op_extract_data_employee_staging >> op_load_dim_employee_dwh >> end_task
    start_task >> op_extract_data_job_salary_staging >> op_load_dim_job_salary_dwh >> end_task
    start_task >> op_extract_data_employee_salary_staging >> op_load_dim_employee_salary_dwh >> end_task
    start_task >> op_extract_data_training_dev_staging >> op_load_dim_training_dev_dwh >> end_task
    start_task >> op_extract_data_employee_training_dev_staging >> op_load_dim_employee_training_dev_dwh >> end_task
    start_task >> op_extract_data_employee_training_performance_staging >> op_load_dim_employee_training_performance_dwh >> end_task
    start_task >> op_extract_data_employee_training_performance_review_staging >> op_load_dim_employee_training_performance_review_dwh >> end_task