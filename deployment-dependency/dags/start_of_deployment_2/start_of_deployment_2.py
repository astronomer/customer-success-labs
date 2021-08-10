from datetime import datetime
from airflow import DAG
from airflow.sensors.sql_sensor import SqlSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.python_sensor import PythonSensor

def extract_ds_check(schema, table, column, ds):
    sql = f"""
    select
      {column}
    from {schema}.{table}
    where {column} = '{ds}'
    """
    return sql

# def my_func():
#     return True

with DAG(dag_id='start_of_deployment_2', start_date=datetime(2021, 8, 9), schedule_interval=None, max_active_runs=1, catchup=False) as DAG:

    start = DummyOperator(task_id='start')
    finish = DummyOperator(task_id='finish')

    value_check = SqlSensor(
        task_id='check_using_sql_sensor',
        sql=extract_ds_check('public', 'extract_status', 'date_key', '{{ds}}'),
        conn_id='redshift_warehouse',
        poke_interval=15 * 60,  # (seconds); checking file every fifteen minutes
        timeout=60 * 60 * 12  # timeout in 12 hours
        )

    # bq_task_status = PythonSensor(
    #     task_id=f"check_using_python_sensor",
    #     python_callable=my_func,
    #     # op_kwargs={
    #     #     'table_name': ''
    #     # },
    #     poke_interval=3 * 60,  # checks transfer status every three minutes
    #     timeout=60 * 60  # timeout in 1 hour
    # )

    start >> value_check >> finish