from datetime import datetime
from airflow import DAG
from airflow.sensors.sql_sensor import SqlSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.python_sensor import PythonSensor

def deployment_1_check(schema, table, column, ds):
    sql = f"""
    select
      {column}
    from {schema}.{table}
    where {column} = '{ds}'
    """
    return sql

def my_func(result):
    return result

'''
A couple of notes:
Reschedule v. Poke - Reschedule releases worker and utilizes resources on deployment where poke return sensor results
on a set interval and doesn't free up resources. Reschedule is great for long running tasks especially when you are
unsure of when upstream deployment will finish
'''

with DAG(dag_id='start_of_deployment_2', start_date=datetime(2021, 8, 9), schedule_interval=None, max_active_runs=1, catchup=False) as DAG:

    start = DummyOperator(task_id='start')
    finish = DummyOperator(task_id='finish')

    value_check = SqlSensor(
        task_id='check_using_sql_sensor',
        sql=deployment_1_check('public', 'deployment_1', 'date_key', '{{ds}}'),
        conn_id='my_data_warehouse',
        mode='reschedule' #see notes above
        # poke_interval=60,  # (seconds); #see notes above
        # timeout=60 * 60 * 12  # timeout in 12 hours
        )

    value_check_2 = PythonSensor(
        task_id=f"check_using_python_sensor",
        python_callable=my_func,
        op_kwargs={
            'result': True
        },
        mode='reschedule' #see notes above
        # poke_interval=3 * 60,  # checks transfer status every three minutes
        # timeout=60 * 60  # timeout in 1 hour
    )

    start >> value_check >> value_check_2 >> finish