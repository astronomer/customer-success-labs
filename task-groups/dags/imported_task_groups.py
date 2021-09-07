from airflow.models import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from task_group_functions import training_groups

default_args = {'start_date': datetime(2021, 1, 1)}

with DAG('imported_task_groups', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo 'start'"
    )

    # From the imported training_groups module, assign and call the function containing the task groups
    group_training_tasks = training_groups()

    end = BashOperator(
        task_id="end",
        bash_command="echo 'end'"
    )

    start >> group_training_tasks >> end