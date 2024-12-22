from pyspark.sql import SparkSession

def run_spark(**kwargs):
    spark = SparkSession.builder.appName("male_students").getOrCreate()
    folder_path = "./datasets/"
    df = spark.read.csv(path=f'{folder_path}StudentsPerformance.csv' , header=True , inferSchema = True)
    filter_df = df.filter(df.gender == 'male')
    filter_df.write.parquet(f'{folder_path}male_students',mode="overwrite")
    path = f'{folder_path}male_students'
    ti = kwargs['ti']
    ti.xcom_push(key='temp_data_path',value = path)
    spark.stop()
    
if __name__ == '__main__':
    run_spark()
    
    
    