"""Hardcoded DWH constants for Gelassenheit ELT demo (no secrets)."""

CH_HOST = "clickhouse"
CH_PORT = 8123

CH_DATABASE_HELPDESK = "gel_helpdesk"
CH_DATABASE_DEVICES = "gel_devices"
CH_DATABASE_BOT = "gel_bot"
CH_DATABASE_CRM = "gel_crm"
CH_DATABASE_PBX = "gel_pbx"
CH_DATABASE_DIALER = "gel_dialer"
CH_DATABASE_WORKFORCE = "gel_workforce"

CH_DATABASES = {
    "helpdesk": CH_DATABASE_HELPDESK,
    "devices": CH_DATABASE_DEVICES,
    "bot": CH_DATABASE_BOT,
    "crm": CH_DATABASE_CRM,
    "pbx": CH_DATABASE_PBX,
    "dialer": CH_DATABASE_DIALER,
    "workforce": CH_DATABASE_WORKFORCE,
}

GS_CONN_ID = "workforce_conn"

S3_BUCKET = "gel-lake"
S3_ENDPOINT = "http://minio:9000"
S3_REGION = "us-east-1"
DBT_PROJECT_PATH = "/opt/dbt"
DBT_TARGET = "dev"
DBT_REPO_BRANCH = "main"

SSH_CONN_DBT = "gel-dbt"
DBT_SSH_POOL = "dbt_host"
SSH_DBT_CMD_TIMEOUT = 3600
SSH_DBT_YEARLY_FULL_REFRESH_TIMEOUT = 28800
S3_CONN = "gel-s3"

BACKLOG_APPEND_ONLY_EXCLUDE = "tag:backlog_append_only"

SOURCE_CONN_HELPDESK = "helpdesk_pg"
SOURCE_CONN_BOT = "bot_pg"
SOURCE_CONN_BOT_SSH = "bot_ssh"

API_EXPORT_RETRIES = 3
API_EXPORT_RETRY_DELAY_MIN = 2
API_EXPORT_MAX_RETRY_DELAY_MIN = 15
