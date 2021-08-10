import pathlib
import glob
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.latest_only import LatestOnlyOperator
from datetime import datetime

from lib.aws_tools.s3_automation import S3Tools
from lib.sql_tools.rds_postgres import RDS

wd = pathlib.Path(__file__).parent.resolve()
sql_files = [str(os.path.basename(f)).replace('.sql', '') for f in glob.glob(f"{wd}/sql/*.sql")]

def _upload_sql_to_s3_task(ds, sql_file):
    rds_tool = RDS()
    S3 = S3Tools()
    db_list = rds_tool._get_databases()
    moyr_str = datetime.strptime(ds, '%Y-%m-%d').strftime('%m%Y')

    fd = open(f"{wd}/sql/{sql_file}.sql")
    sql = fd.read()
    fd.close()
    for database in db_list:
        conn = rds_tool._db_connect(database)
        S3._upload_sql_to_s3(
            ds=ds,
            db_conn=conn,
            sql=sql,
            key=f"{database}/{moyr_str}/{str(sql_file).replace('.sql', '')}.csv"
        )
        conn = None

with DAG(
    "airflow_db_iterative_sql",
    start_date=datetime(2021, 7, 20),
    max_active_runs=1,
    schedule_interval="@daily"
) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    latest_only = LatestOnlyOperator(task_id='latest_only')

    latest_only >> start

    for file in sql_files:
        t = PythonOperator(
            task_id=f"{file}_to_s3",
            python_callable=_upload_sql_to_s3_task,
            op_kwargs={
                'ds': '{{ds}}',
                'sql_file': file
            },
            pool='metadb_sql_query',
            task_concurrency=1
        )
        start >> t >> end