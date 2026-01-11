# ClickHouse schemas — Gelassenheit ELT

| Database | Domain | Typical content |
|----------|--------|-----------------|
| `gel_helpdesk` | Service desk | tickets, tasks, history, lifecycle events, ticket SLA, queue episodes, operational load, enriched |
| `gel_devices` | Product / devices | install helpdesk joins, device mark events, product ops marts |
| `gel_bot` | Support AI bot | bot sessions, answer dialogs, usage, coverage/theme marts |
| `gel_crm` | CRM | leads/companies/contacts current layers |
| `gel_pbx` | Cloud PBX | calls detail, legs, main metrics |
| `gel_dialer` | Outbound dialer | calls detail, FCR, dialer marts |
| `gel_workforce` | Workforce | employee daily totals, activity |

## Mapping from mixed legacy layout

Previously several domains shared one database. In this portfolio build they are split so ownership and access controls are obvious.
