from airflow.models import DAG
from datetime import datetime
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

default_args = {'start_date': datetime(2021, 1, 1)}

with DAG('nested_task_groups', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    t0 = DummyOperator(task_id='start')

    groups = []
    for g_id in range(1, 3):
        with TaskGroup(group_id=f'group{g_id}') as tg1:
            t1 = DummyOperator(task_id='task1')
            t2 = DummyOperator(task_id='task2')

            sub_groups = []
            for s_id in range(1, 3):
                with TaskGroup(group_id=f'sub_group{s_id}') as tg2:
                    st1 = DummyOperator(task_id='task1')
                    st2 = DummyOperator(task_id='task2')

                    st1 >> st2
                    sub_groups.append(tg2)

            t1 >> sub_groups >> t2
            groups.append(tg1)

    t3 = DummyOperator(task_id='end')

    t0 >> groups[0] >> groups[1] >> t3