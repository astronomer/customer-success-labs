from distutils import util
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.sensors.python import PythonSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from datetime import datetime

def my_func(var_key):
    return bool(util.strtobool(Variable.get(var_key)))

with DAG(dag_id='3-triggered-by-sensor',
         start_date=datetime(2021, 1, 1),
         max_active_runs=1,
         schedule_interval=None,
         catchup=False
         ) as dag:

    #DummyOperators
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    t = DummyOperator(task_id='t')

    #for a list of Sensors: https://registry.astronomer.io/modules?types=Sensors

    upstream_dag = ExternalTaskSensor(
        task_id="wait-for-2-triggered-by-upstream-dag",
        external_dag_id="2-triggered-by-upstream-dag",
        external_task_id="finish",
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode="reschedule"
    )

    value_check_2 = PythonSensor(
        task_id=f"check_using_python_sensor",
        python_callable=my_func,
        op_kwargs={
            'var_key': 'pass_sensor_check'
        },
        mode='reschedule'
        # poke_interval=3 * 60,  # checks transfer status every three minutes
        # timeout=60 * 60  # timeout in 1 hour
    )

    upstream_dag >> value_check_2 >> start >> t >> end