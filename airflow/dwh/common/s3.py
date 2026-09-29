"""Garage/S3 helpers for direct bronze uploads from Airflow workers."""

from __future__ import annotations

from urllib.parse import urlparse

import boto3
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from botocore.config import Config

from dwh.common.config import s3_endpoint
from dwh.common.constants import S3_CONN, S3_REGION


def _bool_extra(value, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _optional_var(key: str) -> str | None:
    value = Variable.get(key, default_var=None)
    return value or None


def _optional_connection():
    try:
        return BaseHook.get_connection(S3_CONN)
    except Exception:
        return None


def s3_client():
    conn = _optional_connection()
    extra = conn.extra_dejson if conn else {}
    conn_host = conn.host if conn and str(conn.host or "").startswith(("http://", "https://")) else None
    endpoint = extra.get("endpoint_url") or conn_host or s3_endpoint()
    region = extra.get("region_name") or extra.get("region") or S3_REGION
    verify = _bool_extra(extra.get("verify"), False)
    access_key = _optional_var("s3_key_id") or (conn.login if conn else None) or extra.get("aws_access_key_id")
    secret_key = _optional_var("s3_secret_key") or (conn.password if conn else None) or extra.get("aws_secret_access_key")

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        verify=verify,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def upload_file_to_s3(local_path: str, s3_uri: str) -> None:
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    s3_client().upload_file(local_path, parsed.netloc, parsed.path.lstrip("/"))
