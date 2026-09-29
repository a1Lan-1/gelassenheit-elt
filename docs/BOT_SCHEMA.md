# Support Bot source schema (anonymized)

Demo bot tables live in Postgres schema `bot` (see `fixtures/sql/init_postgres.sql`)
and ClickHouse database `gel_bot`.

Local demo does not require SSH bastion; Airflow ingest modules keep the
interface for bronze extraction but point at local Postgres in compose.
