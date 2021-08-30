import logging
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

def create_python_op(task_number):
    return PythonOperator(
        task_id=f'dynamic_task_{task_number}',
        python_callable=_my_python_task,
        op_kwargs={
            'task_number': task_number
        }
    )

def _my_python_task(task_number):
    log = logging.getLogger('Dependency Demo')
    log.info(f'Running dynamic_task_{task_number}')

def hour_rounder(t):
    t_updated = (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
                 + timedelta(hours=-3 + (t.minute // 30)))
    return t_updated.isoformat()

with DAG(dag_id='1-trigger-downstream-dag',
         start_date=datetime(2021, 1, 1),
         max_active_runs=1,
         schedule_interval=None,
         catchup=False
         ) as dag:

    #DummyOperators
    start = DummyOperator(task_id='start')
    finish = DummyOperator(task_id='finish')

    #parallel_tasks_task_groups
    with TaskGroup(group_id='group1') as tg1:
        #create a few working tasks
        for i in range(1, 5):
            create_python_op(i)

    #trigger downstream dags
    trigger_downstream_dag = TriggerDagRunOperator(
        task_id='trigger_downstream_dag',
        trigger_dag_id='2-triggered-by-upstream-dag',
        execution_date=hour_rounder(datetime.now())
    )

    trigger_downstream_dag_2 = TriggerDagRunOperator(
        task_id='trigger_downstream_dag_2',
        trigger_dag_id='3-triggered-by-sensor',
        execution_date=hour_rounder(datetime.now())
    )

    #parallel_tasks_regular
    parallel_tasks = []
    for i in range(6, 10):
        t = create_python_op(i)
        parallel_tasks.append(t)

    start >> tg1 >> parallel_tasks >> finish >> [trigger_downstream_dag, trigger_downstream_dag_2] #you can add task groups, lists of tasks, or individual tasks to these dependency trees