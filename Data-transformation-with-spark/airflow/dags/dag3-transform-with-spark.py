from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin

import pandas as pd
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import max
import pyspark.sql.functions as func
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Load environment variables for data warehouse configuration
pg_dwh_username = os.getenv('POSTGRES_USER_DATA_WAREHOUSE')
pg_dwh_password = os.getenv('POSTGRE_PASSWORD_DATA_WAREHOUSE')
pg_dwh_host = os.getenv('POSTGRE_HOST_DATA_WAREHOUSE')
pg_dwh_database_1 = os.getenv('POSTGRE_DATABASE_1_DATA_WAREHOUSE')
pg_dwh_database_2 = os.getenv('POSTGRE_DATABASE_2_DATA_WAREHOUSE')

# Load environment variables for data mart configuration
pg_mart_username = os.getenv('POSTGRES_USER_DATA_MART')
pg_mart_password = os.getenv('POSTGRES_PASSWORD_DATA_MART')
pg_mart_host = os.getenv('POSTGRES_HOST_DATA_MART')
pg_mart_port = os.getenv('POSTGRES_PORT_DATA_MART')
pg_mart_database = os.getenv('POSTGRES_DB_DATA_MART')

# ----------------------------------------- Task for employee data -----------------------------------------
def employee_get_all_transform(**kwargs):
    """
    Set up spark connection and use spark to transform data from tabular to parquet data
    """
    # create a logger object and log an informational message saying that the orchestration task
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    
    try:
        # Spark session as the entry point for working with Spark
        spark = SparkSession.builder \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
            .master("local") \
            .appName('EmployeeData') \
            .getOrCreate()
        jdbc_url = f"jdbc:postgresql://{pg_dwh_host}/{pg_dwh_database_1}?user={pg_dwh_username}&password={pg_dwh_password}"
        connection_properties = {
           "driver": "org.postgresql.Driver"
        }
        # Reading data from your PostgreSQL database using the jdbc format
        spark_df_emp = spark.read.jdbc(url=jdbc_url, table="fact_tbl_hr", properties=connection_properties)
        # Creates a temporary view named "employee" that represents the DataFrame
        spark_df_emp.createOrReplaceTempView("employee")
        # Run SQL query inside spark sql func to create a new DataFrame
        spark_df_emp_result = spark.sql('''
            SELECT COUNT(*) AS Total_company_emp,
                   current_date() AS date
            FROM employee
        ''')
        # Write the data in variable to a storage location
        spark_df_emp_result.write.mode('append').partitionBy('date') \
            .option("compression", "snappy") \
            .save("emp_data_result_task_1")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

def employee_get_data_based_on_age_transform(**kwargs):
    """
    Set up spark connection and use spark to transform data from tabular to parquet data
    """
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    
    try:
        spark = SparkSession.builder \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
            .master("local") \
            .appName('EmployeeData') \
            .getOrCreate()
        jdbc_url = f"jdbc:postgresql://{pg_dwh_host}/{pg_dwh_database_1}?user={pg_dwh_username}&password={pg_dwh_password}"
        connection_properties = {
           "driver": "org.postgresql.Driver"
        }
        spark_df_emp = spark.read.jdbc(url=jdbc_url, table="fact_tbl_hr", properties=connection_properties)
        spark_df_emp.createOrReplaceTempView("employee")
        
        spark_df_emp_based_on_age_result = spark.sql('''
            SELECT age,
                   COUNT(age) AS Total_emp,
                   current_date() AS date
            FROM employee
            GROUP BY age
            ORDER BY age
        ''')
        
        spark_df_emp_based_age_avg_result = spark.sql('''
                SELECT ROUND(avg(age)) AS avg_age,
                       current_date() AS date
                FROM employee
        ''')
        
        spark_df_emp_result = spark.sql('''
                SELECT *,
                       current_date() AS date
                FROM employee
        ''')
        
        # Convert Spark DataFrame to Pandas DataFrame
        pandas_df_emp_result = spark_df_emp_result.toPandas()
        pandas_df_emp_temp = pandas_df_emp_result
        pandas_df_emp_result_temp = pandas_df_emp_temp
        
        # Create indexing using .loc function (pandas) and create new column 'age_group'
        pandas_df_emp_result_temp.loc[pandas_df_emp_result_temp['age']>=39, 'age_group'] = 'older_adult'
        pandas_df_emp_result_temp.loc[pandas_df_emp_result_temp['age'].between(25,39), 'age_group'] = 'adult'
        pandas_df_emp_result_temp.loc[pandas_df_emp_result_temp['age'].between(20,24), 'age_group'] = 'young_adult'
        pandas_df_emp_result_temp.loc[pandas_df_emp_result_temp['age'].between(0,19), 'age_group'] = 'teenage'
        
        # Move dataFrame to temporary variable 
        pandas_df_emp_result_temp_1 = pandas_df_emp_result_temp
        
        # Create indexing using .loc function (pandas) and create new column 'age_range'
        pandas_df_emp_result_temp_1.loc[pandas_df_emp_result_temp['age_group']=='older_adult', 'age_range'] = '>=39'
        pandas_df_emp_result_temp_1.loc[pandas_df_emp_result_temp['age_group']=='adult', 'age_range'] = '25-38'
        pandas_df_emp_result_temp_1.loc[pandas_df_emp_result_temp['age_group']=='young_adult', 'age_range'] = '20-24'
        pandas_df_emp_result_temp_1.loc[pandas_df_emp_result_temp['age_group']=='teenage', 'age_range'] = '1-19'
        
        # create Pyspark DataFrame from Pandas DataFrame
        spark_df_emp_result_based_group_age = spark.createDataFrame(pandas_df_emp_result_temp_1)
        spark_df_emp_result_based_group_age_result = spark_df_emp_result_based_group_age.groupby('age_range', 'gender', 'date').agg((func.count('age_group')).alias('count_employee_age'))
        
        spark_df_emp_based_on_age_result.write.mode('append').partitionBy('date') \
            .option("compression", "snappy") \
            .save("emp_data_result_task_2")
        
        spark_df_emp_based_age_avg_result.write.mode('append').partitionBy('date') \
            .option("compression", "snappy") \
            .save("emp_data_result_task_2_1")
        
        spark_df_emp_result_based_group_age_result.write.mode('append').partitionBy('date') \
            .option("compression", "snappy") \
            .save("emp_data_result_task_2_2")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise


def employee_get_data_gender_transform(**kwargs):
    """
    Set up spark connection and use spark to transform data from tabular to parquet data
    """
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    
    try:
        spark = SparkSession.builder \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
            .master("local") \
            .appName('EmployeeData') \
            .getOrCreate()
        jdbc_url = f"jdbc:postgresql://{pg_dwh_host}/{pg_dwh_database_1}?user={pg_dwh_username}&password={pg_dwh_password}"
        connection_properties = {
           "driver": "org.postgresql.Driver"
        }
        spark_df_emp = spark.read.jdbc(url=jdbc_url, table="fact_tbl_hr", properties=connection_properties)
        spark_df_emp.createOrReplaceTempView("employee")
    
        spark_df_emp_based_on_gender_result = spark.sql('''
                SELECT gender,
                       COUNT(gender) AS Total_employee_per_department,
                       ROUND(COUNT(*) / (SELECT COUNT(*) FROM employee), 3) AS Gender_ratio,
                       current_date() AS date
                       FROM employee
                GROUP BY gender
        ''')
        
        spark_df_emp_based_on_gender_result.write.mode('append').partitionBy('date') \
            .option("compression", "snappy") \
            .save("emp_data_result_task_3")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

def employee_get_data_based_department_transform(**kwargs):
    """
    Set up spark connection and use spark to transform data from tabular to parquet data
    """
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    
    try:
        spark = SparkSession.builder \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
            .master("local") \
            .appName('EmployeeData') \
            .getOrCreate()
        jdbc_url = f"jdbc:postgresql://{pg_dwh_host}/{pg_dwh_database_1}?user={pg_dwh_username}&password={pg_dwh_password}"
        connection_properties = {
           "driver": "org.postgresql.Driver"
        }
        spark_df_emp = spark.read.jdbc(url=jdbc_url, table="fact_tbl_hr", properties=connection_properties)
        spark_df_emp.createOrReplaceTempView("employee")
    
        spark_df_emp_based_department_result = spark.sql('''
                SELECT department,
                       ROUND(COUNT(*) / (SELECT COUNT(*) FROM employee), 2) AS Department_ratio,
                       current_date() AS date
                FROM employee
                GROUP BY department
        ''')
        
        spark_df_emp_based_department_result.write.mode('append').partitionBy('date') \
            .option("compression", "snappy") \
            .save("emp_data_result_task_4")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

def employee_get_data_based_job_title_transform(**kwargs):
    """
    Set up spark connection and use spark to transform data from tabular to parquet data
    """
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    
    try:
        spark = SparkSession.builder \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
            .master("local") \
            .appName('EmployeeData') \
            .getOrCreate()
        jdbc_url = f"jdbc:postgresql://{pg_dwh_host}/{pg_dwh_database_1}?user={pg_dwh_username}&password={pg_dwh_password}"
        connection_properties = {
           "driver": "org.postgresql.Driver"
        }
        spark_df_emp = spark.read.jdbc(url=jdbc_url, table="fact_tbl_hr", properties=connection_properties)
        spark_df_emp.createOrReplaceTempView("employee")
    
        spark_df_employee_top_salary_result = spark.sql('''
                SELECT department,
                       title,
                       employeesalary,
                       bonusovertime,
                       current_date() AS date
                FROM employee
                GROUP BY department,
                         title,
                         employeesalary,
                         bonusovertime
                ORDER BY employeesalary ASC
        ''')
        
        spark_df_employee_top_salary_result.groupby('department', 'title', 'date').agg(max('employeesalary').alias("High_salary")).orderBy(func.desc("High_salary"))
        spark_df_employee_top_salary_result.write.mode('append').partitionBy('date') \
            .option("compression", "snappy") \
            .save("emp_data_result_task_5")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

def employee_get_data_based_job_performance_transform(**kwargs):
    """
    Set up spark connection and use spark to transform data from tabular to parquet data
    """
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    
    try:
        spark = SparkSession.builder \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
            .master("local") \
            .appName('EmployeeData') \
            .getOrCreate()
        jdbc_url = f"jdbc:postgresql://{pg_dwh_host}/{pg_dwh_database_1}?user={pg_dwh_username}&password={pg_dwh_password}"
        connection_properties = {
           "driver": "org.postgresql.Driver"
        }
        spark_df_emp = spark.read.jdbc(url=jdbc_url, table="fact_tbl_hr", properties=connection_properties)
        spark_df_emp.createOrReplaceTempView("employee")
    
        spark_df_emp_performance = spark.sql('''
                SELECT department,
                       title,
                       trainingreview,
                       trainingrating,
                       current_date() AS date
                FROM employee
                GROUP BY department,
                         title,
                         trainingreview,
                         trainingrating
                ORDER BY trainingreview
        ''')
        
        spark_df_emp_performance.write.mode('append').partitionBy('date') \
            .option("compression", "snappy") \
            .save("emp_data_result_task_6")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

def employee_load_all_transform(**kwargs):
    """
    Set up data mart connection and use spark to save the transform resutl to postgresql
    """
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    
    try:
        # Read Parquet files to obtained data from previous tasks
        df_data_mart_1 = pd.read_parquet('emp_data_result_task_1')
        # Start postgres engine
        db_url = f"postgresql://{pg_mart_username}:{pg_mart_password}@{pg_mart_host}:{pg_mart_port}/{pg_mart_database}"
        engine = create_engine(db_url, echo=False)
        # Write their contents into PostgreSQL database
        df_data_mart_1.to_sql(name='total_emp', con=engine)
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

def employee_load_data_based_on_age_transform(**kwargs):
    """
    Set up data mart connection and use spark to save the transform resutl to postgresql
    """
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    
    try:
        df_data_mart_2 = pd.read_parquet('emp_data_result_task_2')
        df_data_mart_2_1 = pd.read_parquet('emp_data_result_task_2_1')
        df_data_mart_2_2 = pd.read_parquet('emp_data_result_task_2_2')
        db_url = f"postgresql://{pg_mart_username}:{pg_mart_password}@{pg_mart_host}:{pg_mart_port}/{pg_mart_database}"
        engine = create_engine(db_url, echo=False)
        df_data_mart_2.to_sql(name='total_emp_based_on_age', con=engine)
        df_data_mart_2_1.to_sql(name='avg_emp_age', con=engine)
        df_data_mart_2_2.to_sql(name='total_emp_top_salary', con=engine)
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

def employee_load_data_based_gender_transform(**kwargs):
    """
    Set up data mart connection and use spark to save the transform resutl to postgresql
    """
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    
    try:
        df_data_mart_3 = pd.read_parquet('emp_data_result_task_3')
        db_url = f"postgresql://{pg_mart_username}:{pg_mart_password}@{pg_mart_host}:{pg_mart_port}/{pg_mart_database}"
        engine = create_engine(db_url, echo=False)
        df_data_mart_3.to_sql(name='total_emp_gender_ratio', con=engine)
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

def employee_load_data_based_department_transform(**kwargs):
    """
    Set up data mart connection and use spark to save the transform resutl to postgresql
    """
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    
    try:
        df_data_mart_4 = pd.read_parquet('emp_data_result_task_4')
        db_url = f"postgresql://{pg_mart_username}:{pg_mart_password}@{pg_mart_host}:{pg_mart_port}/{pg_mart_database}"
        engine = create_engine(db_url, echo=False)
        df_data_mart_4.to_sql(name='total_dept', con=engine)
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise
    
def employee_load_data_based_job_title_transform(**kwargs):
    """
    Set up data mart connection and use spark to save the transform resutl to postgresql
    """
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    
    try:
        df_data_mart_5 = pd.read_parquet('emp_data_result_task_5')
        db_url = f"postgresql://{pg_mart_username}:{pg_mart_password}@{pg_mart_host}:{pg_mart_port}/{pg_mart_database}"
        engine = create_engine(db_url, echo=False)
        df_data_mart_5.to_sql(name='total_emp_based_on_job_title', con=engine)
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

def employee_load_data_based_job_performance_transform(**kwargs):
    """
    Set up data mart connection and use spark to save the transform resutl to postgresql
    """
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
    
    try:
        df_data_mart_6 = pd.read_parquet('emp_data_result_task_6')
        db_url = f"postgresql://{pg_mart_username}:{pg_mart_password}@{pg_mart_host}:{pg_mart_port}/{pg_mart_database}"
        engine = create_engine(db_url, echo=False)
        df_data_mart_6.to_sql(name='emp_performance_review', con=engine)
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

# ----------------------------------------- Task for candidate data -----------------------------------------
def candidate_get_data_all_transform(**kwargs):
    """
    Set up spark connection and use spark to transform data from tabular to parquet data
    """
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
        
    try:
        spark = SparkSession.builder \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
            .master("local") \
            .appName('EmployeeData') \
            .getOrCreate()
        jdbc_url = f"jdbc:postgresql://{pg_dwh_host}/{pg_dwh_database_1}?user={pg_dwh_username}&password={pg_dwh_password}"
        connection_properties = {
            "driver": "org.postgresql.Driver"
        }
        spark_df_candidate = spark.read.jdbc(url=jdbc_url, table="data_recruitment_selection_update", properties=connection_properties)
        spark_df_candidate.createOrReplaceTempView("candidate")
    
        spark_df_candidate_result = spark.sql('''
            SELECT COUNT(*) AS Total_interview_candidate,
                   current_date() AS date
            FROM candidate
        ''')
    
        spark_df_candidate_result.write.mode('append').partitionBy('date') \
            .option("compression", "snappy") \
            .save("candidate_data_result_task_1")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

def candidate_get_data_based_gender_transform(**kwargs):
    """
    Set up spark connection and use spark to transform data from tabular to parquet data
    """
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
        
    try:
        spark = SparkSession.builder \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
            .master("local") \
            .appName('EmployeeData') \
            .getOrCreate()
        jdbc_url = f"jdbc:postgresql://{pg_dwh_host}/{pg_dwh_database_1}?user={pg_dwh_username}&password={pg_dwh_password}"
        connection_properties = {
            "driver": "org.postgresql.Driver"
        }
        spark_df_candidate = spark.read.jdbc(url=jdbc_url, table="data_recruitment_selection_update", properties=connection_properties)
        spark_df_candidate.createOrReplaceTempView("candidate")
    
        spark_df_candidate_based_on_gender_result = spark.sql('''
            SELECT gender,
                   COUNT(gender) AS Total_interview_candidate,
                   current_date() AS date
            FROM candidate
            GROUP BY gender
        ''')
    
        spark_df_candidate_based_on_gender_result.write.mode('append').partitionBy('date') \
            .option("compression", "snappy") \
            .save("candidate_data_result_task_2")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

def candidate_get_data_based_age_transform(**kwargs):
    """
    Set up spark connection and use spark to transform data from tabular to parquet data
    """
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
        
    try:
        spark = SparkSession.builder \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
            .master("local") \
            .appName('EmployeeData') \
            .getOrCreate()
        jdbc_url = f"jdbc:postgresql://{pg_dwh_host}/{pg_dwh_database_1}?user={pg_dwh_username}&password={pg_dwh_password}"
        connection_properties = {
            "driver": "org.postgresql.Driver"
        }
        spark_df_candidate = spark.read.jdbc(url=jdbc_url, table="data_recruitment_selection_update", properties=connection_properties)
        spark_df_candidate.createOrReplaceTempView("candidate")
    
        spark_df_candidate_based_on_age_result = spark.sql('''
            SELECT age,
                   COUNT(age) AS Total_candidate,
                   current_date() AS date
            FROM candidate
            GROUP BY age
            ORDER BY age
        ''')
    
        spark_df_candidate_based_on_age_result.write.mode('append').partitionBy('date') \
            .option("compression", "snappy") \
            .save("candidate_data_result_task_3")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

def candidate_load_data_all_transform(**kwargs):
    """
    Set up data mart connection and use spark to save the transform resutl to postgresql
    """
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
        
    try:
        df_data_mart_5 = pd.read_parquet('candidate_data_result_task_1')
        db_url = f"postgresql://{pg_mart_username}:{pg_mart_password}@{pg_mart_host}:{pg_mart_port}/{pg_mart_database}"
        engine = create_engine(db_url, echo=False)
        df_data_mart_5.to_sql(name='total_intv_candidate', con=engine)
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

def candidate_load_data_based_age_transform(**kwargs):
    """
    Set up data mart connection and use spark to save the transform resutl to postgresql
    """
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
        
    try:
        df_data_mart_6 = pd.read_parquet('candidate_data_result_task_2')
        db_url = f"postgresql://{pg_mart_username}:{pg_mart_password}@{pg_mart_host}:{pg_mart_port}/{pg_mart_database}"
        engine = create_engine(db_url, echo=False)
        df_data_mart_6.to_sql(name='total_intv_candidate_based_on_gender', con=engine)
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

def candidate_load_data_based_gender_transform(**kwargs):
    """
    Set up data mart connection and use spark to save the transform resutl to postgresql
    """
    logger = LoggingMixin().log
    logger.info("Starting orchestration task...")
        
    try:
        df_data_mart_7 = pd.read_parquet('candidate_data_result_task_3')
        db_url = f"postgresql://{pg_mart_username}:{pg_mart_password}@{pg_mart_host}:{pg_mart_port}/{pg_mart_database}"
        engine = create_engine(db_url, echo=False)
        df_data_mart_7.to_sql(name='total_intv_candidate_based_on_age', con=engine)
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise


with DAG(
    dag_id='dag1-transform-spark',
    start_date=datetime(2024, 10, 9),
    schedule_interval='0 0 * * 1', # The DAG is scheduled to run every Monday at midnight (00:00)
    catchup=False # Prevents the DAG from running historical backfills
) as dag:
    # This is the start point of the DAG
    start_task = EmptyOperator(
        task_id='start'
    )
# ----------------------------------------- Task for employee data -----------------------------------------
    # DAG task to fetches employee data from a source
    op_employee_demography_get_data_all = PythonOperator(
        task_id='fun_employee_demography_get_data_all',        
        python_callable=employee_get_all_transform                                                                                                                                              
    )
    
    op_employee_demography_get_data_age = PythonOperator(
        task_id='fun_employee_demography_get_data_age',        
        python_callable=employee_get_data_based_on_age_transform                                                                                                                                              
    )
    
    op_employee_demography_get_data_gender = PythonOperator(
        task_id='fun_employee_demography_get_data_gender',
        python_callable=employee_get_data_gender_transform                                                                                                                                              
    )
    
    op_employee_demography_get_data_department = PythonOperator(
        task_id='fun_employee_demography_get_data_department',        
        python_callable=employee_get_data_based_department_transform                                                                                                                                              
    )
    
    op_employee_demography_get_data_job_title = PythonOperator(
        task_id='fun_employee_demography_get_data_job_title',        
        python_callable=employee_get_data_based_job_title_transform                                                                                                                                              
    )
    
    op_employee_demography_get_data_job_performance = PythonOperator(
        task_id='fun_employee_demography_get_data_job_performance',        
        python_callable=employee_get_data_based_job_performance_transform                                                                                                                                              
    )
    # DAG task to loads the transformed employee data into target
    op_employee_demography_load_data_all = PythonOperator(
        task_id='fun_employee_demography_load_data_all',        
        python_callable=employee_load_all_transform
    )
    
    op_employee_demography_load_data_all_age = PythonOperator(
        task_id='fun_employee_demography_load_data_age',        
        python_callable=employee_load_data_based_on_age_transform
    )
    
    op_employee_demography_load_data_gender = PythonOperator(
        task_id='fun_employee_demography_load_data_gender',        
        python_callable=employee_load_data_based_gender_transform
    )
    
    op_employee_demography_load_data_department = PythonOperator(
        task_id='fun_employee_demography_load_data_department',        
        python_callable=employee_load_data_based_department_transform
    )
    
    op_employee_demography_load_data_job_title = PythonOperator(
        task_id='fun_employee_demography_load_data_job_title',        
        python_callable=employee_load_data_based_job_title_transform
    )
    
    op_employee_demography_load_data_job_performance = PythonOperator(
        task_id='fun_employee_demography_load_data_job_performance',        
        python_callable=employee_load_data_based_job_performance_transform
    )
# ----------------------------------------- Task for candidate data -----------------------------------------
    op_candidate_demography_get_data_all = PythonOperator(
        task_id='fun_candidate_demography_get_data_all',        
        python_callable=candidate_get_data_all_transform                                                                                                                                              
    )
    
    op_candidate_demography_get_data_gender = PythonOperator(
        task_id='fun_candidate_demography_get_data_gender',        
        python_callable=candidate_get_data_based_gender_transform                                                                                                                                              
    )
    
    op_candidate_demography_get_data_age = PythonOperator(
        task_id='fun_candidate_demography_get_data_age',        
        python_callable=candidate_get_data_based_age_transform                                                                                                                                              
    )

    op_candidate_demography_load_data_all = PythonOperator(
        task_id='fun_candidate_demography_load_data_all',        
        python_callable=candidate_load_data_all_transform
    )
    
    op_candidate_demography_load_data_gender = PythonOperator(
        task_id='fun_candidate_demography_load_data_gender',        
        python_callable=candidate_load_data_based_gender_transform
    )
    
    op_candidate_demography_load_data_age = PythonOperator(
        task_id='fun_candidate_demography_load_data_age',        
        python_callable=candidate_load_data_based_age_transform
    )
    # This is the end point of the DAG
    end_task = EmptyOperator(
        task_id='end'
    )

    start_task >> op_employee_demography_get_data_all >> op_employee_demography_get_data_age >> op_employee_demography_get_data_gender >> op_employee_demography_get_data_department >> op_employee_demography_get_data_job_title >> op_employee_demography_get_data_job_performance >> end_task
    start_task >> op_employee_demography_load_data_all >> op_employee_demography_load_data_all_age >> op_employee_demography_load_data_gender >> op_employee_demography_load_data_department >> op_employee_demography_load_data_job_title >> op_employee_demography_load_data_job_performance >> end_task
    start_task >> op_candidate_demography_get_data_all >> op_candidate_demography_get_data_gender >> op_candidate_demography_get_data_age >> end_task
    start_task >> op_candidate_demography_load_data_all >> op_candidate_demography_load_data_gender >> op_candidate_demography_load_data_age >> end_task