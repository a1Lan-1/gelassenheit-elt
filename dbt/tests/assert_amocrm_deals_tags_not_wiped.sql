-- Fails when a newer deal version empties tags that existed on an older version.
-- Catches silent overwrites from flaky Amo list payloads / bad JSON casts.
-- Scoped to recent updates so historical one-offs don't permanently red the suite.
select
  id,
  latest_at,
  latest_tags,
  prev_tags
from (
  select
    id,
    max(updated_at) as latest_at,
    argMax(toString(tags), updated_at) as latest_tags,
    argMaxIf(toString(tags), updated_at, length(toString(tags)) > 30) as prev_tags
  from {{ ref('int_crm__deals_current') }}
  group by id
)
where length(latest_tags) <= 15
  and length(prev_tags) > 30
  and latest_at >= today() - 3
