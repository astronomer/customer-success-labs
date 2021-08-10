from operators.ms_teams_webhook_operator import MSTeamsWebhookOperator
from hooks.ms_teams_webhook_hook import MSTeamsWebhookHook
from airflow.operators.python import get_current_context


def dag_triggered_callback(context, **kwargs):
    log_url = context.get("task_instance").log_url
    teams_msg = f"""
            DAG has been triggered.
            Task: {context.get('task_instance').task_id}  
            Dag: {context.get('task_instance').dag_id} 
            Execution Time: {context.get('execution_date')}  
            """
    teams_notification = MSTeamsWebhookOperator(
        task_id="ms_teams_callback",
        trigger_rule="all_done",
        message=teams_msg,
        button_text="View log",
        button_url=log_url,
        theme_color="FF0000",
        http_conn_id=kwargs["http_conn_id"],
    )
    return teams_notification.execute(context)


def dag_success_callback(context, **kwargs):
    log_url = context.get("task_instance").log_url
    teams_msg = f"""
            DAG has succeeded.
            Task: {context.get('task_instance').task_id}  
            Dag: {context.get('task_instance').dag_id} 
            Execution Time: {context.get('execution_date')}  
            """
    teams_notification = MSTeamsWebhookOperator(
        task_id="ms_teams_callback",
        trigger_rule="all_done",
        message=teams_msg,
        button_text="View log",
        button_url=log_url,
        theme_color="FF0000",
        http_conn_id=kwargs["http_conn_id"],
    )
    return teams_notification.execute(context)


def success_callback(context, **kwargs):
    log_url = context.get("task_instance").log_url
    teams_msg = f"""
            Task has succeeded. 
            Task: {context.get('task_instance').task_id}  
            Dag: {context.get('task_instance').dag_id} 
            Execution Time: {context.get('execution_date')}  
            """
    teams_notification = MSTeamsWebhookOperator(
        task_id="ms_teams_callback",
        trigger_rule="all_done",
        message=teams_msg,
        button_text="View log",
        button_url=log_url,
        theme_color="FF0000",
        http_conn_id=kwargs["http_conn_id"],
    )
    return teams_notification.execute(context)


def failure_callback(context, **kwargs):
    log_url = context.get("task_instance").log_url
    teams_msg = f"""
            Task has failed. 
            Task: {context.get('task_instance').task_id}  
            Dag: {context.get('task_instance').dag_id} 
            Execution Time: {context.get('execution_date')}
            Exception: {context.get('exception')}
            """
    teams_notification = MSTeamsWebhookOperator(
        task_id="ms_teams_callback",
        trigger_rule="all_done",
        message=teams_msg,
        button_text="View log",
        button_url=log_url,
        theme_color="FF0000",
        http_conn_id=kwargs["http_conn_id"],
    )
    return teams_notification.execute(context)


def retry_callback(context, **kwargs):
    log_url = context.get("task_instance").log_url
    teams_msg = f"""
            Task is retrying. 
            Task: {context.get('task_instance').task_id}
            Try number: {context.get('task_instance').try_number - 1} out of {context.get('task_instance').max_tries + 1}. 
            Dag: {context.get('task_instance').dag_id} 
            Execution Time: {context.get('execution_date')}  
            Exception: {context.get('exception')}
            """
    teams_notification = MSTeamsWebhookOperator(
        task_id="ms_teams_callback",
        trigger_rule="all_done",
        message=teams_msg,
        button_text="View log",
        button_url=log_url,
        theme_color="FF0000",
        http_conn_id=kwargs["http_conn_id"],
    )
    return teams_notification.execute(context)


def python_operator_callback(**kwargs):
    context = get_current_context()
    log_url = context.get("task_instance").log_url
    teams_msg = f"""
            This is a test for sending a MS Teams message via a PythonOperator.
            Task: {context.get('task_instance').task_id}  
            Dag: {context.get('task_instance').dag_id} 
            Execution Time: {context.get('execution_date')}  
            """
    teams_notification = MSTeamsWebhookOperator(
        task_id="ms_teams_callback",
        trigger_rule="all_done",
        message=teams_msg,
        button_text="View log",
        button_url=log_url,
        theme_color="FF0000",
        http_conn_id=kwargs["http_conn_id"],
    )
    return teams_notification.execute(context)


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis, *args, **kwargs):
    dag_id = slas[0].dag_id
    task_id = slas[0].task_id
    execution_date = slas[0].execution_date.isoformat()
    teams_msg = f"""
            SLA has been missed.
            Task: {task_id}  
            Dag: {dag_id} 
            Execution Time: {execution_date}  
            """
    hook = MSTeamsWebhookHook(
        message=teams_msg,
        theme_color="FF0000",
        http_conn_id=kwargs['http_conn_id'])
    hook.execute()
