
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import timedelta

with DAG(dag_id='group1',schedule = timedelta(days=1),start_date=pendulum.datetime(2024,12,26),tags=['learning']) as dag:
    with TaskGroup("Section1" , tooltip="Task for section 1") as secition1:
        task1 = EmptyOperator(task_id='start')
        task2 = EmptyOperator(task_id="end")
        task1 >> task2
    with TaskGroup('seciton2') as section2:
        task3 = EmptyOperator(task_id = "start")
        task4 = EmptyOperator(task_id="End")
        task3 >> task4
secition1 >> section2
