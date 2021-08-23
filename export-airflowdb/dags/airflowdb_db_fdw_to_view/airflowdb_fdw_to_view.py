import pathlib
import logging

from datetime import datetime
from psycopg2.extensions import AsIs

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.latest_only import LatestOnlyOperator

from util.sql_tools.rds_postgres import RDS
from util.sql_tools.query_builder import all_logs, all_taskinstances

wd = pathlib.Path(__file__).parent.resolve()

def _create_master_metadb_if_not_exists(db_name):
    rds_tool = RDS()
    exists = rds_tool._check_if_db_exists(db_name)
    sql = f"CREATE DATABASE {db_name} WITH OWNER postgres;"
    if exists is True:
        print("Database already exists...skipping.")
    else:
        print("Database doesn't exist...creating.")
        rds_tool._execute_sql(sql)

def _create_dynamic_view(view_name, query, ds):
    rds_tool = RDS()
    db_list = rds_tool._get_databases()
    rds_tool._drop_view(view_name=view_name)
    sql = """"""
    sql += "begin transaction;\n"
    sql += "create schema if not exists metadata;\n"
    sql += f"create view metadata.{view_name} as ("
    counter = 0
    for db in db_list:
        if counter == 0:
            sql += str(f"\n{query(db, ds)}")
        else:
            sql += "\nunion all\n" + str(query(db, ds))
        counter = counter+1
    sql += ");\n"
    sql += "end transaction;"
    print(sql)
    rds_tool._execute_sql(sql=sql, dbname="airflow_master_metadb")

def _fdw_task():
    rds_tool = RDS()
    log = logging.getLogger('psycopg2 logger:')
    con_params = BaseHook.get_connection("rds_instance")
    connection = rds_tool._db_connect("airflow_master_metadb")
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
    schedule_interval="@daily",
    catchup=False
) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    latest_only = LatestOnlyOperator(task_id='latest_only')

    create_master_metadb = PythonOperator(
        task_id='create_master_metadb',
        python_callable=_create_master_metadb_if_not_exists,
        op_kwargs={
            'db_name': 'airflow_master_metadb'
        }
    )

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

    latest_only >> start >> create_master_metadb >> create_fdw >> t1 >> t2 >> end