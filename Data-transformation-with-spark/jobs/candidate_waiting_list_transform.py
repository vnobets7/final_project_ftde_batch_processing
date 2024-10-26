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

def candidate_get_data_transform():
    """
    Set up spark connection and use spark to transform data from tabular to parquet data
    """
  
    spark = SparkSession.builder \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
        .master("local") \
        .appName('EmployeeData') \
        .getOrCreate()

    jdbc_url = "jdbc:postgresql://172.18.0.3:5432/staging_db"
    connection_properties = {
        "user":pg_username,
        "password":pg_password,
        "driver": "org.postgresql.Driver"
    }
    spark_df_candidate = spark.read.jdbc(url=jdbc_url, table="public.data_recruitment_selection_update", properties=connection_properties)
    spark_df_candidate.createOrReplaceTempView("candidate")
    
    spark_df_candidate_based_on_gender_result = spark.sql('''
       SELECT
            gender,
            COUNT(gender) AS Total_interview_candidate,
            current_date() AS date
       FROM candidate
       GROUP BY gender
    ''')
    
    spark_df_candidate_based_on_gender_result.write.mode('append').partitionBy('date') \
        .option("compression", "snappy") \
        .save("data_result_task_4")

def candidate_load_data_transform():
    """
    Set up data mart connection and use spark to save the transform resutl to postgresql
    """    
    df_data_mart = pd.read_parquet('data_result_task_4')
    db_url = f"postgresql://{pg_mart_username}:{pg_mart_password}@{pg_mart_host}:{pg_mart_port}/{pg_mart_database}"
    engine = create_engine(db_url, echo=False)
    df_data_mart.to_sql(name='total_intv_candidate_based_on_gender', con=engine)