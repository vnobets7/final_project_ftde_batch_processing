import os
import pandas as pd
import psycopg2
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Data warehouse configuration
pg_username = "DWH_HR_ricky"
pg_password = "DWH_HR_test"
      
# Load environment variables
pg_mart_username = "Data_mart_ricky"
pg_mart_password = "Data_mart_test"
pg_mart_host = "172.18.0.2"
pg_mart_port = "5432"
pg_mart_database = "Data_mart"

def employee_get_data_transform():
    """
    Set up spark connection and use spark to transform data from tabular to parquet data
    """
    # Data warehouse configuration
    pg_username = "DWH_HR_ricky"
    pg_password = "DWH_HR_test"
        
    spark = SparkSession.builder \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
        .master("local") \
        .appName('EmployeeData') \
        .getOrCreate()

    jdbc_url = "jdbc:postgresql://172.18.0.3:5432/DWH_HR"
    connection_properties = {
        "user":pg_username,
        "password":pg_password,
        "driver": "org.postgresql.Driver"
    }
    spark_df_emp = spark.read.jdbc(url=jdbc_url, table="public.fact_tbl_hr", properties=connection_properties)
    spark_df_emp.createOrReplaceTempView("employee")
    
    spark_df_emp_based_on_age_result = spark.sql('''
            SELECT age,
                   COUNT(age) AS Total_emp,
                   current_date() AS date
            FROM employee
            GROUP BY age
            ORDER BY age
    ''')
    
    spark_df_emp_based_on_age_result.write.mode('append').partitionBy('date') \
        .option("compression", "snappy") \
        .save("data_result_task_2")

def employee_load_data_transform():
    """
    Set up data mart connection and use spark to save the transform resutl to postgresql
    """
    # Load environment variables
    pg_mart_username = "Data_mart_ricky"
    pg_mart_password = "Data_mart_test"
    pg_mart_host = "172.18.0.2"
    pg_mart_port = "5432"
    pg_mart_database = "Data_mart"
    
    df_data_mart_2 = pd.read_parquet('data_result_task_2')
    db_url = f"postgresql://{pg_mart_username}:{pg_mart_password}@{pg_mart_host}:{pg_mart_port}/{pg_mart_database}"
    engine = create_engine(db_url, echo=False)
    df_data_mart_2.to_sql(name='total_emp_based_on_age', con=engine)
