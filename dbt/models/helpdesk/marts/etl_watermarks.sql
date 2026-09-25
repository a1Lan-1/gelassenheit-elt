{{
  config(
    materialized='table',
    engine=ch_engine_merge_tree(),
    order_by='(domain, entity)',
    tags=['helpdesk', 'meta'],
  )
}}

select
  cast('helpdesk' as String) as domain,
  cast('' as String) as entity,
  cast(toDateTime('1970-01-01 00:00:00', 'UTC') as DateTime64(3, 'UTC')) as watermark_at,
  cast(now64(3) as DateTime64(3, 'UTC')) as updated_at
where 1 = 0
