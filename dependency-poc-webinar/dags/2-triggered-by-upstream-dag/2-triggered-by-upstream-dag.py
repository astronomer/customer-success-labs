import random
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule

def return_branch(**kwargs):
    branches = ['branch_0', 'branch_1', 'branch_2', 'branch_3', 'branch_4']
    return random.choice(branches)

with DAG(dag_id='2-triggered-by-upstream-dag',
         start_date=datetime(2021, 1, 1),
         max_active_runs=1,
         schedule_interval=None,
         catchup=False
         ) as dag:

    #DummyOperators
    start = DummyOperator(task_id='start')
    finish = DummyOperator(
        task_id='finish',
        trigger_rule=TriggerRule.ALL_SUCCESS #default
        # trigger_rule=TriggerRule.ALL_FAILED
        # trigger_rule=TriggerRule.ALL_DONE
        # trigger_rule=TriggerRule.ONE_FAILED
        # trigger_rule=TriggerRule.ONE_SUCCESS
        # trigger_rule=TriggerRule.DUMMY
    )

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=return_branch,
        provide_context=True
    )

    start >> branching

    for i in range(0, 5):
        d = DummyOperator(task_id='branch_{0}'.format(i))
        for j in range(0, 3):
            m = DummyOperator(task_id='branch_{0}_{1}'.format(i, j))
            d >> m >> finish
        branching >> d