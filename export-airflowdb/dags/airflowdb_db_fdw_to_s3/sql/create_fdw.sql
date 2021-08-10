begin transaction;

create extension if not exists postgres_fdw;

drop server if exists %(db_shortname)s cascade;
create server %(db_shortname)s
  foreign data wrapper postgres_fdw
  options (host %(host)s, port '%(port)s', dbname %(dbname)s);

drop user mapping if exists for %(user_as_is)s server %(db_shortname)s;
create user mapping for %(user_as_is)s server %(db_shortname)s
  options (user %(user)s, password %(password)s);

drop foreign table if exists %(db_shortname)s_airflow_log;
create foreign table %(db_shortname)s_airflow_log (
  id int,
  dttm timestamp with time zone,
  dag_id varchar(250),
  task_id varchar(250),
  event varchar(30),
  execution_date timestamp with time zone,
  owner varchar(500),
  extra text
)
  server %(db_shortname)s
  options (schema_name 'airflow', table_name 'log');

drop foreign table if exists %(db_shortname)s_airflow_ti;
create foreign table %(db_shortname)s_airflow_ti (
  task_id varchar(250),
  dag_id varchar(250),
  execution_date timestamp with time zone,
  start_date timestamp with time zone,
  end_date timestamp with time zone,
  duration double precision,
  state varchar(20),
  try_number integer,
  hostname varchar(1000),
  unixname varchar(1000),
  job_id integer,
  pool varchar(256),
  queue varchar(256),
  priority_weight integer,
  operator varchar(1000),
  queued_dttm timestamp with time zone,
  pid integer,
  max_tries integer,
  executor_config bytea,
  pool_slots integer,
  queued_by_job_id integer,
  external_executor_id varchar(250)
)
  server %(db_shortname)s
  options (schema_name 'airflow', table_name 'task_instance');

end transaction;