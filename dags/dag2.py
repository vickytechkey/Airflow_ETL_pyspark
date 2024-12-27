from airflow import DAG
from datetime  import datetime , timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


default_args = {
    'owner':'apache_airflow',
    'depends_on_past' : False,
    'start_date':datetime(2024,12,24),
    'email_on_failure':False,
    'email_on_retry':False,
    'retires':1,
    'retry_delay':timedelta(minutes=5)
    
}

dag = DAG(
    'dag2',
    description = "owndag",
    schedule_interval=timedelta(days=1),
    default_args=default_args
)

bash_task = BashOperator(
    task_id = 'task1',
    bash_command="echo 'hello world'",
    dag = dag
)

bash_task
