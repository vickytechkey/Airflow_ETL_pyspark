from airflow import DAG
from datetime import datetime , timedelta
from airflow.operators.python import PythonOperator
from sparkfiles.male_student import run_spark
from sparkfiles.loading_male_students import Load_male_students
from python_functions.delete_folder import delete_folder

default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2023,1,1),
    'email_on_failure':False,
    'email_on_retry':False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5)
}

dag = DAG(
    'simple_dag',
    description='Tested by vignesh for study purpose',
    schedule_interval=timedelta(days=1),
    default_args=default_args
)

def calling_delete():
    folder_path = "./datasets/male_student"
    delete_folder(folder_path)
    

task_1 = PythonOperator(
    task_id = "delete_data",
    python_callable= calling_delete,
    dag = dag,
)
    
task_2 = PythonOperator(
    task_id = 'male_student',
    python_callable= run_spark,
    dag = dag ,
    provide_context = True 
)

task_3  = PythonOperator(
    task_id = "Load_male_students",
    python_callable=Load_male_students,
    dag = dag,
    provide_context = True
    
)


 
task_1 >> task_2 >> task_3
