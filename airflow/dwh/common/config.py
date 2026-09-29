"""Runtime config: Airflow Variables with hardcoded fallbacks from constants."""

from __future__ import annotations

from airflow.models import Variable

from dwh.common.constants import (
    DBT_PROJECT_PATH,
    DBT_TARGET,
    S3_BUCKET,
    S3_ENDPOINT,
)


def get_var(key: str, default: str) -> str:
    return Variable.get(key, default_var=default)


def s3_bucket() -> str:
    return get_var("dwh_s3_bucket", S3_BUCKET)


def s3_endpoint() -> str:
    return get_var("dwh_s3_endpoint", S3_ENDPOINT)


def dbt_project_path() -> str:
    return get_var("dwh_dbt_project_path", DBT_PROJECT_PATH)


def dbt_target() -> str:
    return get_var("dwh_dbt_target", DBT_TARGET)


def dbt_docs_path() -> str:
    return get_var("dwh_dbt_docs_path", "/home/dbt-user/dbt-docs")
