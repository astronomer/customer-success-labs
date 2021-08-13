import requests
import json
from util.notion import NotionClient
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def get_office_hours(source_id='', target_id=''):
    print('test')
    # DEaaS (Office Hours)--
    # https://www.notion.so/astronomerio/195541b7cef24e2fab6ec3c936ad46bc?v=70d2cf5afcdc44588fc9fd4e1e597f1c
    # source_database_id = '195541b7cef24e2fab6ec3c936ad46bc'
    source_db = NotionClient(source_id)
    # Customer Task List - Customer Success
    # https://www.notion.so/astronomerio/964eb0aa7713477e8c19fd8cf3d1959b?v=9cafc0f6e49f41d6bcb170eacdc1a739
    # target_db_id='964eb0aa7713477e8c19fd8cf3d1959b'
    target_db = NotionClient(target_id)

    source = source_db._query_notion_database()
    target = target_db._query_notion_database()
    results = []
    for object in source['results']:
        try:
            try:
                day_left = object['properties']['Days to Renewal']['rollup']['array'][0]['formula']['number']
            except KeyError:
                day_left = 0
            customer = object['properties']['Customer']['title'][0]['text']['content']
            hours_committed = object['properties']['Hours Committed']['number']

            try:
                hours_delivered = object['properties']['Hours Delivered']['rollup']['number']
            except Exception as e:
                hours_delivered = 0
            if hours_committed == 0:
                percent_complete = 1
            else:
                percent_complete = round(hours_delivered / hours_committed, 2)
            if percent_complete < 1:
                results.append({'customer': customer,
                                'day_left': day_left,
                                'hours_committed': hours_committed,
                                'hours_delivered': hours_delivered,
                                'percent_complete': percent_complete,
                                'source_id': object['id']})
        except Exception as e:
            print(e)
    existing_ids = []

    for object in target['results']:
        try:
            existing_ids.append(object['properties']['Source Id']['rich_text'][0]['text']['content'])
        except KeyError:
            pass
        except IndexError:
            pass
    add_cnt = 0
    sorted_results = sorted(results, key=lambda k: k['day_left'])

    for result in sorted_results:
        if add_cnt >= 3:
            break
        elif result['source_id'] not in existing_ids:
            target_db.create_notion_page(f'{result["customer"]} has '
                                         f'{result["hours_committed"] - result["hours_delivered"]} '
                                         f'DEaaS Hours Remaining, they expire in {result["day_left"]} days',
                                         None, None, source_id=result['source_id'])
            add_cnt += 1

    # Default settings applied to all tasks


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('add_office_hours_expiration_ticket',
         start_date=datetime(2021, 8, 1),
         max_active_runs=3,
         schedule_interval='@weekly',
         default_args=default_args,
         catchup=False
         ) as dag:
    start = DummyOperator(
        task_id='start'
    )

    t1 = PythonOperator(
        task_id='move_tickets',
        python_callable=get_office_hours,
        op_kwargs={
            'source_id': '195541b7cef24e2fab6ec3c936ad46bc',
            'target_id': '964eb0aa7713477e8c19fd8cf3d1959b'
        }
    )

    finish = DummyOperator(
        task_id='finish'
    )

    start >> t1 >> finish
