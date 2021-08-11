import pathlib
import logging

from datetime import datetime
from psycopg2.extensions import AsIs

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.latest_only import LatestOnlyOperator

from util.aws_tools.s3_automation import S3Tools
from util.sql_tools.rds_postgres import RDS
from util.sql_tools.query_builder import successful_tasks_month_agg, all_logs, all_taskinstances

wd = pathlib.Path(__file__).parent.resolve()

def _upload_dynamic_query_to_s3(query, ds):
    S3 = S3Tools()
    rds_tool = RDS()
    conn = rds_tool._db_connect("postgres")
    # db_list = rds_tool._get_databases()
    db_list = ['boreal_declination_7615_airflow', 'desolate_spaceship_9383_airflow']
    sql = """"""
    counter = 0
    for db in db_list:
        if counter == 0:
            sql += str(query(db, ds))
        else:
            sql += "union" + str(query(db, ds))
        counter = counter+1
    S3._upload_sql_to_s3(
        ds=ds,
        db_conn=conn,
        sql=sql,
        key=f"{query.__name__}/{ds}/out.csv"
    )

def _fdw_task():
    rds_tool = RDS()
    log = logging.getLogger('psycopg2 logger:')
    con_params = BaseHook.get_connection("rds_instance")
    connection = rds_tool._db_connect("postgres")
    connection.autocommit = False
    cursor = connection.cursor()
    fd = open(f"{wd}/sql/create_fdw.sql")
    sql = fd.read()
    fd.close()
    # db_list = rds_tool._get_databases()
    db_list = ['boreal_declination_7615_airflow', 'desolate_spaceship_9383_airflow']

    for db in db_list:
        cursor.execute(sql, {
            "host": con_params.host,
            "dbname": db,
            "db_shortname": AsIs(str(db).replace('_airflow', '')),
            "port": con_params.port,
            "user": con_params.login,
            "user_as_is": AsIs(con_params.login),
            "password": con_params.password
        })
        log.info(f"Cursor statusmessage: {cursor.statusmessage}")
        connection.commit()
    cursor.close()

with DAG(
    "airflow_db_fdw_to_s3",
    start_date=datetime(2021, 8, 3),
    max_active_runs=1,
    schedule_interval="@daily",
    catchup=False
) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    latest_only = LatestOnlyOperator(task_id='latest_only')

    create_fdw = PythonOperator(
        task_id='create_fdw',
        python_callable=_fdw_task,
        provide_context=True
    )

    t1 = PythonOperator(
        task_id='all_logs',
        python_callable=_upload_dynamic_query_to_s3,
        op_kwargs={
            'query': all_logs,
            'ds': '{{ds}}'
        }
    )

    t2 = PythonOperator(
        task_id='all_task_instances',
        python_callable=_upload_dynamic_query_to_s3,
        op_kwargs={
            'query': all_taskinstances,
            'ds': '{{ds}}'
        }
    )

    latest_only >> start >> create_fdw >> t1 >> t2 >> end