from python_functions.mysql_connection import mysql_connection
from pyspark.sql import SparkSession
from airflow.utils.log.logging_mixin import LoggingMixin

def Load_male_students(**kwargs):
    logger = LoggingMixin().log
    con_class = mysql_connection()
    cred = con_class.credentials()
    mysqlurl = con_class.mysqlurl("student_data")
    
    spark = SparkSession.builder.appName("loadmalestudent") \
    .config("spark.jars" , "/Users/krvigne/downloads/mysql-connector-j-9.1.0/mysql-connector-j-9.1.0.jar") \
    .getOrCreate()
    ti = kwargs['ti']
    temp_path = ti.xcom_pull(task_ids = "male_student" , key = 'temp_data_path')
    df = spark.read.parquet(temp_path)
    df.write \
      .mode("append") \
          .jdbc(mysqlurl,table="male_student",properties = cred)
    
    logger.info("Load Job completed successfully")
              
    
    
    
