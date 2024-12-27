from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def student_data_conn(db_tablename , dbname):
    spark = SparkSession.builder \
        .appName("MYSQL connection") \
        .config("spark.jars","/Users/krvigne/downloads/mysql-connector-j-9.1.0/mysql-connector-j-9.1.0.jar") \
        .getOrCreate()
        
    mysql_properties = {
        "url":f"jdbc:mysql://localhost:3306/{dbname}",
        "driver":"com.mysql.cj.jdbc.Driver",
        "user":"root",
        "password":"vtechnosoft@123A"
    }
    
    df = spark.read \
        .format("jdbc") \
        .option("url",mysql_properties["url"])\
        .option("driver",mysql_properties["driver"])\
        .option("user",mysql_properties["user"])\
        .option("password",mysql_properties["password"])\
        .option("dbtable",db_tablename)\
        .option("numPartitions",10)\
        .load()
    return df
    