select
  current_database(),
  date_trunc('month', ti.start_date::date) as start_date,
  count(distinct log.id) as successful_run
from airflow.log
inner join airflow.task_instance ti on ti.dag_id = log.dag_id
  and ti.operator != 'DummyOperator'
  and ti.task_id = log.task_id
  and ti.execution_date = log.execution_date
  and log.event = 'success'
group by 1, 2
order by 2 desc
;