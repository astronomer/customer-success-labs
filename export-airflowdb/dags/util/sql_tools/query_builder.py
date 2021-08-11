def successful_tasks_month_agg(dbname, ds):
    return f"""
    select
      '{dbname}' as current_database,
      date_trunc('month', log.execution_date::date) as start_date,
      count(distinct log.id) as successful_run
    from {dbname}_log log
    inner join {dbname}_ti ti on ti.dag_id = log.dag_id
      and ti.operator != 'DummyOperator'
      and ti.task_id = log.task_id
      and ti.execution_date = log.execution_date
      and log.event = 'success'
    group by 1, 2
    """

def all_logs(dbname, ds):
    return f"""
    select '{dbname}' as db_name, * from {dbname}_log where dttm::date = '{ds}'::date
    """

def all_taskinstances(dbname, ds):
    return f"""
    select '{dbname}' as db_name, * from {dbname}_ti where start_date::date = '{ds}'::date
    """