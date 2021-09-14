from airflow.models import DAG
from datetime import datetime
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

default_args = {'start_date': datetime(2021, 1, 1)}

with DAG('dynamic_task_groups_independent', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    for g_id in range(1, 3):
        with TaskGroup(group_id=f'group{g_id}') as tg1:
            t1 = DummyOperator(task_id='task1')
            t2 = DummyOperator(task_id='task2')

            t1 >> t2
