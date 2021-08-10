drop table if exists public.deployment_1;

create table public.deployment_1 as (
select
  'deployment_1_complete' as description,
  '{{ds}}'::date as date_key
--   '2021-08-09'::date as date_key --for demonstration put day before ds
);