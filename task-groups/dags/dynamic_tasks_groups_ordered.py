from airflow.models import DAG
from datetime import datetime
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

default_args = {'start_date': datetime(2021, 1, 1)}

with DAG('dynamic_task_groups_ordered', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    t0 = DummyOperator(task_id='start')

    groups = []
    for g_id in range(1, 4):
        tg_id = f'group{g_id}'
        with TaskGroup(group_id=tg_id) as tg1:
            t1 = DummyOperator(task_id='task1')
            t2 = DummyOperator(task_id='task2')

            t1 >> t2

            # Add an additional task to group1 based on group_id to demonstrate ability to add variation to dynamic tasks
            if tg_id == 'group1':
                t3 = DummyOperator(task_id='task3')
                t1 >> t3

            groups.append(tg1)

    t3 = DummyOperator(task_id='end')

    t0 >> [groups[0], groups[1]] >> groups[2] >> t3