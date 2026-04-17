-- Uniqueness of grain after ReplacingMergeTree (FINAL), not raw SELECT.
select cashdesk_id
from {{ ref('int_devices__device_mark_events') }} final
group by cashdesk_id
having count() > 1
