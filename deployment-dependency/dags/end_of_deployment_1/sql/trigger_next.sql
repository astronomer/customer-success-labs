drop table if exists public.deployment_1;

create table public.deployment_1 as (
select
  'deployment_1_complete' as description,
  '{{ds}}'::date as date_key
);

grant select on public.deployment_1 to group analysts;