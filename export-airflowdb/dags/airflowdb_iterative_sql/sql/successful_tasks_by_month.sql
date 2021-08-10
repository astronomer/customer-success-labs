with operator_types as (
  select
    date_key,
    dag_id,
    task_id,
    operator
  from (
    select
      start_date::date as date_key,
      dag_id,
      task_id,
      operator,
      row_number() over (partition by start_date::date, dag_id, task_id order by start_date desc) as rn
    from airflow.task_instance
  ) t
  where t.rn = 1
)

select
  current_database(),
  l.dttm::date as date_key,
  l.dag_id,
  l.task_id,
  ot.operator,
  1 as successful_run,
  case
    when ot.operator in ('LatestOnlyOperator', 'DummyOperator') then 0
    else 1 end as billable,
  l.dttm as execution_datetime
from airflow.log l
left join operator_types ot on ot.date_key = l.dttm::date
  and ot.dag_id = l.dag_id
  and ot.task_id = l.task_id
where l.event = 'success'
  and date_trunc('month', l.dttm::date) = date_trunc('month', %(ds)s::date)
;
