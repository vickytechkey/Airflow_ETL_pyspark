from sparkfiles.sparksql import student_data_conn
from pyspark.sql.functions import *

def checking_connection():
    
    '''
    checking connection for student data
    '''
    df = student_data_conn(db_tablename="male_student",dbname="student_data")

def reading_student_data():
    
    '''
    Reading the student data and saving as parquet file
    '''
    df = student_data_conn(db_tablename="male_student",dbname="student_data")
    df.write.parquet('./datasets/tmp/student_data',mode='overwrite',compression='snappy')
    
    