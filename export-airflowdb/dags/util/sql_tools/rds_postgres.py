import psycopg2
from airflow.hooks.base_hook import BaseHook

class RDS:
    def __init__(self):
        self.root = "postgres"

    def _check_if_db_exists(self, db_name):
        sql = f"""select datname = '{db_name}' as db_exists from pg_database where datname = '{db_name}';"""
        connection = self._db_connect(database_name='postgres')
        cursor = connection.cursor()
        cursor.execute(sql)
        try:
            result = cursor.fetchone()[0]
        except:
            result = False
        cursor.close()
        return result

    def _drop_view(self, view_name):
        sql = f"""drop view if exists {view_name};"""
        self._execute_sql(sql=sql)

    def _get_databases(self):
        connection = self._db_connect(self.root)
        cursor = connection.cursor()
        sql = """select datname from pg_database where datname like '%%_airflow';"""
        cursor.execute(sql)
        records = cursor.fetchall()
        databases = []
        for record in records:
            if record[0] not in databases:
                databases.append(record[0])
        cursor.close()
        return databases

    def _execute_sql(self, sql, dbname="postgres"):
        connection = self._db_connect(dbname)
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute(sql)
        print(f"CURSOR MESSAGE {cursor.statusmessage}")
        cursor.close()

    def _db_connect(self, database_name):
        conn = BaseHook.get_connection("rds_instance")
        connection = psycopg2.connect(
            database=database_name,
            user=conn.login,
            password=conn.password,
            host=conn.host,
            port=conn.port
        )
        return connection