import pathlib
import logging

from datetime import datetime
from psycopg2.extensions import AsIs

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.latest_only import LatestOnlyOperator

from lib.sql_tools.rds_postgres import RDS
from lib.sql_tools.query_builder import all_logs, all_taskinstances

wd = pathlib.Path(__file__).parent.resolve()

def _create_dynamic_view(view_name, query, ds):
    rds_tool = RDS()
    db_list = rds_tool._get_databases()
    rds_tool._drop_view(view_name=view_name)
    sql = """"""
    sql += "begin transaction;\n"
    sql += f"create view public.{view_name} as ("
    counter = 0
    for db in db_list:
        if counter == 0:
            sql += str(f"\n{query(db, ds)}")
        else:
            sql += "\nunion all\n" + str(query(db, ds))
        counter = counter+1
    sql += ");\n"
    sql += "end transaction;"
    # print(sql)
    rds_tool._execute_sql(sql=sql)

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
    db_list = rds_tool._get_databases()

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
    "airflow_db_fdw_to_view",
    start_date=datetime(2021, 8, 3),
    max_active_runs=1,
    schedule_interval="@daily"
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
        task_id='create_all_logs_vw',
        python_callable=_create_dynamic_view,
        op_kwargs={
            'view_name': 'all_logs_vw',
            'query': all_logs,
            'ds': '{{ds}}'
        }
    )

    t2 = PythonOperator(
        task_id='create_all_taskinstances_vw',
        python_callable=_create_dynamic_view,
        op_kwargs={
            'view_name': 'all_taskinstances_vw',
            'query': all_taskinstances,
            'ds': '{{ds}}'
        }
    )

    latest_only >> start >> create_fdw >> t1 >> t2 >> end