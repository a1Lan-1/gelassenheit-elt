{{
  config(
    materialized='view',
    alias='cur_deals_v',
    tags=['cur_deals', 'current_view'],
  )
}}

{# Prefer FINAL row fields, but if tags were wiped to empty by a bad extract
   while an older version still has tags, keep the last non-empty tags.
   Extractors also re-fetch by id when list returns empty tags. #}
with latest as (
  select *
  from {{ ref('int_crm__deals_current') }}
  final
),
best_tags as (
  select
    id,
    argMax(tags, (length(toString(tags)) > 15, updated_at)) as tags
  from {{ ref('int_crm__deals_current') }}
  group by id
)
select
  latest.* replace (
    if(
      length(toString(latest.tags)) > 15,
      latest.tags,
      best_tags.tags
    ) as tags
  )
from latest
left join best_tags on latest.id = best_tags.id
