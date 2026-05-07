"""SSH helpers for gel-dbt and gel-s3 connections."""

from __future__ import annotations

import base64
from urllib.parse import urlparse

import paramiko
from airflow.sdk.bases.hook import BaseHook
from paramiko.ssh_exception import AuthenticationException, BadAuthenticationType

try:
    from airflow.utils.log.secrets_masker import mask_secret
except ImportError:  # pragma: no cover - depends on Airflow version
    def mask_secret(secret: str) -> None:
        return None

from dwh.common.config import dbt_docs_path, dbt_project_path, dbt_target
from dwh.common.constants import DBT_REPO_BRANCH, DBT_REPO_URL, GIT_CONN_DBT


def get_ssh_client(conn_id: str) -> paramiko.SSHClient:
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson or {}
    hostname, port = _ssh_host_port(conn.host, conn.port)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    password = conn.password or None
    try:
        client.connect(
            hostname=hostname,
            port=port,
            username=conn.login,
            password=password,
            key_filename=extra.get("key_file") or extra.get("private_key"),
            look_for_keys=False,
            allow_agent=False,
            timeout=30,
        )
    except (AuthenticationException, BadAuthenticationType):
        if not password:
            raise
        client.close()
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        transport = paramiko.Transport((hostname, port))
        transport.start_client(timeout=30)
        transport.auth_interactive(
            conn.login,
            lambda _title, _instructions, prompts: [password for _prompt, _echo in prompts],
        )
        if not transport.is_authenticated():
            raise AuthenticationException(f"Authentication failed for conn_id={conn_id} host={hostname}")
        client._transport = transport
    return client


def _ssh_host_port(raw_host: str, raw_port: int | None) -> tuple[str, int]:
    parsed = urlparse(raw_host or "")
    if parsed.scheme and parsed.hostname:
        return parsed.hostname, raw_port or parsed.port or 22
    return raw_host, raw_port or 22


def exec_ssh(conn_id: str, command: str, timeout: int = 600) -> tuple[int, str, str]:
    client = get_ssh_client(conn_id)
    try:
        _, stdout, stderr = client.exec_command(command, timeout=timeout)
        out = stdout.read().decode("utf-8", errors="replace")
        err = stderr.read().decode("utf-8", errors="replace")
        code = stdout.channel.recv_exit_status()
        return code, out, err
    finally:
        client.close()


def dbt_run_cmd(
    select: str,
    *,
    bronze_dt: str | None = None,
    bronze_run_id: str | None = None,
    backlog_snapshot_hour: str | None = None,
    full_refresh: bool = False,
    with_current_view: bool = False,
    downstream_depth: int | None = None,
    exclude: str | None = None,
    threads: int = 4,
) -> str:
    import json
    import shlex

    target = dbt_target()
    vars_dict: dict[str, str] = {}
    if bronze_dt:
        vars_dict["bronze_dt"] = bronze_dt
    if bronze_run_id:
        vars_dict["bronze_run_id"] = bronze_run_id
    if backlog_snapshot_hour:
        vars_dict["backlog_snapshot_hour"] = backlog_snapshot_hour
    vars_part = f" --vars '{json.dumps(vars_dict)}'" if vars_dict else ""
    refresh_part = " --full-refresh" if full_refresh else ""
    if with_current_view:
        select_expr = f"{select} {select}_v"
    elif downstream_depth is not None:
        # Parentheses required: int_helpdesk__*_current+1 parses as "current+1", not glob+children.
        select_expr = f"({select})+{downstream_depth}"
    else:
        select_expr = select
    exclude_part = f" --exclude '{exclude}'" if exclude else ""
    threads_part = f" --threads {threads}"
    run = (
        f"{_dbt_preflight_cmd()}dbt run --select '{select_expr}' "
        f"--target {target}{vars_part}{refresh_part}{exclude_part}{threads_part}"
    )
# Throw dag/run into audit_dbt_runs (log_dbt_run_stats on dbt-host).
    af_export = ""
    try:
        try:
            from airflow.sdk import get_current_context
        except ImportError:
            from airflow.operators.python import get_current_context

        ctx = get_current_context()
        dag_id = getattr(ctx.get("dag"), "dag_id", None) or ctx.get("dag_id")
        run_id = ctx.get("run_id")
        if dag_id:
            af_export = (
                f"export DWH_AIRFLOW_DAG_ID={shlex.quote(str(dag_id))}; "
                f"export DWH_AIRFLOW_RUN_ID={shlex.quote(str(run_id or ''))}; "
            )
    except Exception:
        af_export = ""
    return f"{_dbt_shell_prefix()}{af_export}{_dbt_run_with_stats(run)}"


def _dbt_shell_prefix() -> str:
    project = dbt_project_path()
    return (
        f"cd {project} && "
        "{ set -a; [ -f ~/.env ] && source ~/.env; set +a; } && "
        'DBT_VENV="${DBT_VENV_PATH:-}" && '
        '[ -z "$DBT_VENV" ] && [ -f venv/bin/activate ] && DBT_VENV="venv" || true && '
        '[ -z "$DBT_VENV" ] && [ -f .venv/bin/activate ] && DBT_VENV=".venv" || true && '
        '[ -z "$DBT_VENV" ] && [ -f ~/dbt-env/bin/activate ] && DBT_VENV="$HOME/dbt-env" || true && '
        '[ -n "$DBT_VENV" ] && source "$DBT_VENV/bin/activate" || true && '
    )


def _dbt_preflight_cmd() -> str:
    return (
        "test -f dbt_packages/dbt_utils/dbt_project.yml || "
        "{ echo 'FATAL: dbt_packages missing; run dwh_sync_dbt_repo first'; exit 2; } && "
    )


def _dbt_conditional_deps_cmd() -> str:
    return (
        "if [ ! -f dbt_packages/dbt_utils/dbt_project.yml ] "
        "|| [ packages.yml -nt dbt_packages/.deps_stamp ]; then "
        "rm -rf dbt_packages && dbt deps && touch dbt_packages/.deps_stamp; "
        "fi && "
        "git rev-parse HEAD > .dwh-sync-rev"
    )


def dbt_git_sync_cmd() -> str:
    project = dbt_project_path()
    conn = BaseHook.get_connection(GIT_CONN_DBT)
    extra = conn.extra_dejson or {}
    repo_url = conn.host or extra.get("repo_url") or DBT_REPO_URL
    branch = conn.schema or extra.get("branch") or DBT_REPO_BRANCH
    git_cmd = _git_cmd(conn)
    return (
        "set -euo pipefail && "
        f"if [ ! -d {project}/.git ]; then "
        f"rm -rf {project} && {git_cmd} clone -b {branch} {repo_url} {project}; "
        "else "
        f"{git_cmd} -C {project} fetch origin {branch} && "
        f"git -C {project} reset --hard origin/{branch}; "
        "fi"
    )


def _ensure_profiles_threads_cmd(threads: int = 4) -> str:
    """Ensure ~/.dbt/profiles.yml sets dbt parallelism for prod target."""
    return (
        "if [ -f ~/.dbt/profiles.yml ]; then "
        "if grep -q '^      threads:' ~/.dbt/profiles.yml; then "
        f"sed -i 's/^      threads:.*/      threads: {threads}/' ~/.dbt/profiles.yml; "
        "else "
        f"sed -i '/cluster_mode: true/a\\      threads: {threads}' ~/.dbt/profiles.yml; "
        "fi; "
        "fi && "
    )


def _strip_deprecated_env_cmd() -> str:
    """Ensure ~/.env has Replicated DB flag and a real DWH_CH_CLUSTER name (not placeholder 'default')."""
    return (
        "if [ -f ~/.env ]; then "
        "if grep -q '^DWH_CH_REPLICATED_DB=' ~/.env; then "
        "sed -i 's/^DWH_CH_REPLICATED_DB=.*/DWH_CH_REPLICATED_DB=true/' ~/.env; "
        "else echo 'DWH_CH_REPLICATED_DB=true' >> ~/.env; fi; "
        "set -a; source ~/.env; set +a; "
        'NEED_CLUSTER=0; '
        "grep -q '^DWH_CH_CLUSTER=default$' ~/.env 2>/dev/null && NEED_CLUSTER=1 || true; "
        "grep -q '^DWH_CH_CLUSTER=$' ~/.env 2>/dev/null && NEED_CLUSTER=1 || true; "
        "grep -q '^DWH_CH_CLUSTER=' ~/.env 2>/dev/null || NEED_CLUSTER=1; "
        'if [ "$NEED_CLUSTER" = "1" ] && [ -n "$DWH_CH_HOST" ] && [ -n "$DWH_CH_USER" ]; then '
        'DETECTED=$(curl -sS --max-time 10 '
        '"http://${DWH_CH_HOST}:${DWH_CH_PORT:-8123}/?user=${DWH_CH_USER}&password=${DWH_CH_PASSWORD}" '
        '--data-binary "SELECT cluster FROM system.clusters GROUP BY cluster ORDER BY cluster LIMIT 1 FORMAT TabSeparated" '
        "| tr -d '\\r\\n'); "
        'if [ -n "$DETECTED" ]; then '
        "if grep -q '^DWH_CH_CLUSTER=' ~/.env; then "
        'sed -i "s/^DWH_CH_CLUSTER=.*/DWH_CH_CLUSTER=${DETECTED}/" ~/.env; '
        "else echo \"DWH_CH_CLUSTER=${DETECTED}\" >> ~/.env; fi; "
        "fi; fi; "
        "fi && "
    )


def dbt_sync_and_deps_cmd() -> str:
    project = dbt_project_path()
    return (
        f"{dbt_git_sync_cmd()} && "
        f"{_strip_deprecated_env_cmd()}"
        f"{_ensure_profiles_threads_cmd()}"
        f"cd {project} && "
        "{ set -a; [ -f ~/.env ] && source ~/.env; set +a; } && "
        'DBT_VENV="${DBT_VENV_PATH:-}" && '
        '[ -z "$DBT_VENV" ] && [ -f venv/bin/activate ] && DBT_VENV="venv" || true && '
        '[ -z "$DBT_VENV" ] && [ -f .venv/bin/activate ] && DBT_VENV=".venv" || true && '
        '[ -z "$DBT_VENV" ] && [ -f ~/dbt-env/bin/activate ] && DBT_VENV="$HOME/dbt-env" || true && '
        '[ -n "$DBT_VENV" ] && source "$DBT_VENV/bin/activate" || true && '
        f"{_dbt_conditional_deps_cmd()}"
    )


def _git_cmd(conn) -> str:
    extra = conn.extra_dejson or {}
    access_token = conn.password or extra.get("access_token") or extra.get("token")
    if not access_token:
        return "git"

    mask_secret(access_token)
    username = conn.login or extra.get("username") or "oauth2"
    basic_auth = base64.b64encode(f"{username}:{access_token}".encode()).decode()
    mask_secret(basic_auth)
    return f"git -c http.extraHeader='Authorization: Basic {basic_auth}'"


def _dbt_run_with_stats(dbt_run_body: str) -> str:
    """Run dbt, then soft-fail log timings into sync_*.audit_dbt_runs; preserve dbt exit code."""
    return (
        "{ "
        f"{dbt_run_body}; "
        "DBT_RC=$?; "
        "python scripts/log_dbt_run_stats.py 2>/tmp/dwh_dbt_run_stats.err "
        "|| python3 scripts/log_dbt_run_stats.py 2>/tmp/dwh_dbt_run_stats.err "
        "|| true; "
        "exit $DBT_RC; "
        "}"
    )


def dbt_run_all_cmd(
    *,
    full_refresh: bool = False,
    exclude: str | None = None,
    threads: int = 4,
) -> str:
    """Run every dbt model in the project (no --select)."""
    target = dbt_target()
    refresh_part = " --full-refresh" if full_refresh else ""
    exclude_part = f" --exclude '{exclude}'" if exclude else ""
    threads_part = f" --threads {threads}"
    run = (
        f"{_dbt_preflight_cmd()}dbt run "
        f"--target {target}{refresh_part}{exclude_part}{threads_part}"
    )
    return f"{_dbt_shell_prefix()}{_dbt_run_with_stats(run)}"


def dbt_test_cmd(select: str, *, with_current_view: bool = False, threads: int = 4) -> str:
    target = dbt_target()
    select_expr = f"{select} {select}_v" if with_current_view else select
    return (
        f"{_dbt_shell_prefix()}{_dbt_preflight_cmd()}dbt test --select '{select_expr}' "
        f"--target {target} --threads {threads}"
    )


def dbt_docs_generate_cmd() -> str:
    import json

    target = dbt_target()
    docs_path = dbt_docs_path()
    # docs generate compiles every model; *_current macros require bronze_run_id at parse time
    vars_part = (
        " --vars '"
        + json.dumps({"bronze_run_id": "documentation", "bronze_dt": "1970-01-01"})
        + "'"
    )
    return (
        f"{_dbt_shell_prefix()}"
        f"{_dbt_preflight_cmd()}dbt docs generate --target {target}{vars_part} && "
        f"mkdir -p {docs_path} && "
        f"rsync -a --delete target/ {docs_path}/"
    )

