import json
from airflow.hooks.base import BaseHook
from airflow.providers.http.operators.http import SimpleHttpOperator

class AirflowStableAPI():
    def __init__(self, conn_id):
        self.conn_id = conn_id
        '''
        host: https://<AIRFLOW-DOMAIN>/<DEPLOYMENT-RELEASE-NAME>
        password: api_key from service account on deployment
        '''
        self.headers = {
            'Cache-Control': 'no-cache',
            'Content-type': 'application/json',
            'Authorization': BaseHook.get_connection(f"{self.conn_id}").password
        }

    def trigger_dag(self, dag_id):
        return SimpleHttpOperator(
            task_id=f"api_trigger_{dag_id}",
            http_conn_id=self.conn_id,
            endpoint=f'/airflow/api/v1/dags/{dag_id}/dagRuns',
            method='POST',
            headers=self.headers,
            data=json.dumps({})
        )

    def change_pause_status(self, dag_id, pause):
        action = 'pause' if pause else 'unpause'
        return SimpleHttpOperator(
            task_id=f"{action}_dag_{dag_id}",
            http_conn_id=self.conn_id,
            endpoint=f'/airflow/api/v1/dags/{dag_id}',
            method='PATCH',
            headers=self.headers,
            data=json.dumps({
                "is_paused": pause
            })
        )
