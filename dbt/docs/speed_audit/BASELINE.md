# Speed audit baseline (14d)

Generated: 2026-09-30T15:21:49.148672+00:00
DBs: gel_helpdesk, gel_pbx, gel_dialer, gel_crm, gel_workforce

## Top-40 models by p95 (regular runs <600s)

| p95_s | p50_s | max_s | n | db | model | dag |
|------:|------:|------:|--:|----|-------|-----|
| 125.31 | 30.31 | 251.01 | 309 | gel_helpdesk | `int_devices__device_mark_events` | (none) |
| 94.32 | 0.84 | 117.37 | 130 | gel_helpdesk | `int_helpdesk__config_items_current` | (none) |
| 68.05 | 0.79 | 89.53 | 135 | gel_helpdesk | `int_helpdesk__tickets_current` | (none) |
| 54.34 | 0.85 | 94.13 | 125 | gel_helpdesk | `int_helpdesk__comments_current` | (none) |
| 54.01 | 0.77 | 70.92 | 124 | gel_helpdesk | `int_helpdesk__task_status_current` | (none) |
| 52.88 | 0.81 | 71.14 | 117 | gel_helpdesk | `int_helpdesk__tasks_current` | (none) |
| 50.74 | 0.82 | 65.49 | 135 | gel_helpdesk | `int_helpdesk__places_current` | (none) |
| 48 | 19.24 | 91.67 | 306 | gel_helpdesk | `int_devices__install_report_current` | (none) |
| 47.52 | 0.84 | 90.15 | 133 | gel_helpdesk | `int_helpdesk__appeals_current` | (none) |
| 46.57 | 0.67 | 77.06 | 123 | gel_helpdesk | `int_helpdesk__ticket_status_current` | (none) |
| 46.08 | 0.75 | 55.33 | 118 | gel_helpdesk | `int_helpdesk__task_config_item_current` | (none) |
| 44.49 | 0.82 | 61.32 | 120 | gel_helpdesk | `int_helpdesk__persons_current` | (none) |
| 44.47 | 16.53 | 96.88 | 567 | gel_helpdesk | `int_helpdesk__ticket_lifecycle_events` | (none) |
| 44.3 | 0.71 | 59.16 | 138 | gel_helpdesk | `int_helpdesk__ticket_categories_current` | (none) |
| 42.08 | 0.74 | 70.88 | 132 | gel_helpdesk | `int_helpdesk__user_groups_current` | (none) |
| 42.04 | 0.81 | 76.55 | 127 | gel_helpdesk | `int_helpdesk__entities_current` | (none) |
| 40.75 | 0.77 | 58.7 | 129 | gel_helpdesk | `int_helpdesk__types_current` | (none) |
| 38.5 | 0.83 | 61.84 | 144 | gel_helpdesk | `int_helpdesk__sku_current` | (none) |
| 38.03 | 0.71 | 54.04 | 137 | gel_helpdesk | `int_helpdesk__ticket_config_item_current` | (none) |
| 36.95 | 0.85 | 49.83 | 134 | gel_helpdesk | `int_helpdesk__user_groups_mapping_current` | (none) |
| 36.78 | 0.81 | 59.35 | 114 | gel_helpdesk | `int_helpdesk__ticket_cases_current` | (none) |
| 35.77 | 0.8 | 51.67 | 133 | gel_helpdesk | `int_helpdesk__statuses_current` | (none) |
| 34.21 | 0.88 | 44.98 | 144 | gel_helpdesk | `int_helpdesk__user_types_current` | (none) |
| 32.79 | 0.86 | 53.21 | 126 | gel_helpdesk | `int_helpdesk__services_current` | (none) |
| 31.93 | 0.7 | 55.1 | 117 | gel_helpdesk | `int_helpdesk__ticket_channels_current` | (none) |
| 30.8 | 12.47 | 30.8 | 9 | gel_helpdesk | `int_helpdesk__ticket_first_status` | (none) |
| 30.36 | 15.5 | 102.3 | 621 | gel_pbx | `int_pbx__calls_detail` | (none) |
| 29.78 | 9.25 | 58.06 | 555 | gel_helpdesk | `int_helpdesk__ticket_sla_current` | (none) |
| 27.12 | 8.43 | 50.69 | 564 | gel_helpdesk | `int_helpdesk__ticket_queue_episodes` | (none) |
| 26.15 | 0.95 | 50.47 | 155 | gel_helpdesk | `int_helpdesk__users_current` | (none) |
| 24.59 | 15.53 | 73.06 | 329 | gel_workforce | `mart_workforce__employee_daily_totals` | (none) |
| 23.59 | 6.89 | 45.53 | 567 | gel_helpdesk | `int_helpdesk__config_items_cashdesk` | (none) |
| 19.09 | 9.4 | 34.12 | 567 | gel_helpdesk | `int_helpdesk__tasks_enriched_current` | (none) |
| 17.72 | 17.58 | 17.72 | 3 | gel_helpdesk | `int_devices__task_device_resolved` | (none) |
| 17.05 | 10.44 | 38.26 | 566 | gel_helpdesk | `int_helpdesk__tickets_enriched_current` | (none) |
| 14.28 | 5.26 | 109.78 | 576 | gel_helpdesk | `int_helpdesk__history_action_extract` | (none) |
| 14.12 | 4.59 | 29.25 | 575 | gel_helpdesk | `stg_helpdesk__employee_work_history_events` | (none) |
| 13.42 | 7.37 | 13.42 | 12 | gel_helpdesk | `mart_devices__kkt_info` | (none) |
| 13 | 8.42 | 17.47 | 567 | gel_helpdesk | `int_helpdesk__task_ci_primary` | (none) |
| 11.21 | 5.71 | 27.9 | 306 | gel_helpdesk | `mart_devices__install_report` | (none) |

## Approx wall-clock by DAG (sum of model times per hour)

| dag | n_hours | wall_sum p50 | wall_sum p95 | max_model p50 | max_model p95 |
|-----|--------:|-------------:|-------------:|--------------:|--------------:|
