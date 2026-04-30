"""ClickHouse operational helpers (run on dbt-host via SSH shell snippet)."""

from __future__ import annotations

from dwh.common.constants import CH_DATABASES

# Leftover dbt-clickhouse swap/probe tables (see gelassenheit-dbt macros/dbt_cleanup.sql).
DBT_ORPHAN_TABLE_WHERE = (
    "name LIKE '%__dbt_tmp' "
    "OR name LIKE '%__dbt_backup' "
    "OR name LIKE '%__dbt_new_data%' "
    "OR startsWith(name, '__dbt_exchange_test')"
)


def ch_shell_prefix() -> str:
    return (
        "set -a && [ -f ~/.env ] && source ~/.env && set +a && "
        "python3 - <<'PY'\n"
    )


def ch_shell_suffix() -> str:
    return "\nPY"


def _drop_tables_matching_script(where_sql: str, label: str) -> str:
    return (
        ch_shell_prefix()
        + "import os, urllib.parse, urllib.request\n"
        + "host=os.environ.get('DWH_CH_HOST','clickhouse')\n"
        + "user=os.environ.get('DWH_CH_USER','airflow')\n"
        + "password=os.environ.get('DWH_CH_PASSWORD','')\n"
        + "dbs=["
        + ", ".join(repr(db) for db in CH_DATABASES.values())
        + "]\n"
        + "list_q=(\n"
        + "  'SELECT database, name FROM system.tables '\n"
        + "  f\"WHERE database IN ({', '.join(repr(d) for d in dbs)}) \"\n"
        + f'  "AND ({where_sql}) FORMAT TabSeparated"\n'
        + ")\n"
        + "def run(sql):\n"
        + "  url='http://'+host+':8123/?'+urllib.parse.urlencode({'user':user,'password':password})\n"
        + "  req=urllib.request.Request(url, data=sql.encode(), method='POST')\n"
        + "  return urllib.request.urlopen(req, timeout=120).read().decode()\n"
        + "rows=[ln for ln in run(list_q).strip().splitlines() if ln]\n"
        + "for ln in rows:\n"
        + "  db, name = ln.split('\\t', 1)\n"
        + "  sql=f'DROP TABLE IF EXISTS `{db}`.`{name}`'\n"
        + "  print(sql)\n"
        + "  run(sql)\n"
        + f"print(f'dropped {{len(rows)}} {label}')\n"
        + ch_shell_suffix()
    )


def drop_dbt_orphan_tables_script() -> str:
    """DROP all leftover dbt-clickhouse tables in sync_* databases."""
    return _drop_tables_matching_script(DBT_ORPHAN_TABLE_WHERE, "dbt orphan tables")


def drop_dbt_tmp_tables_script() -> str:
    """DROP *__dbt_tmp only. Prefer drop_dbt_orphan_tables_script()."""
    return _drop_tables_matching_script("name LIKE '%__dbt_tmp'", "__dbt_tmp tables")


def drop_dbt_exchange_test_tables_script() -> str:
    """DROP __dbt_exchange_test* only. Prefer drop_dbt_orphan_tables_script()."""
    return _drop_tables_matching_script(
        "startsWith(name, '__dbt_exchange_test')", "__dbt_exchange_test tables"
    )


def duplicate_keys_alert_script() -> str:
    """Query ops_cur_duplicate_keys row count; print ALERT if > 0."""
    return (
        ch_shell_prefix()
        + "import os, urllib.parse, urllib.request, json\n"
        + "host=os.environ.get('DWH_CH_HOST','clickhouse')\n"
        + "user=os.environ.get('DWH_CH_USER','airflow')\n"
        + "password=os.environ.get('DWH_CH_PASSWORD','')\n"
        + "q='SELECT count() AS c FROM gel_helpdesk.ops_cur_duplicate_keys FORMAT JSONEachRow'\n"
        + "url='http://'+host+':8123/?'+urllib.parse.urlencode({'user':user,'password':password})\n"
        + "r=urllib.request.urlopen(urllib.request.Request(url,data=q.encode()),timeout=120)\n"
        + "row=json.loads(r.read().decode())\n"
        + "c=int(row.get('c',0))\n"
        + "print(json.dumps({'duplicate_rows': c}))\n"
        + "if c>0: print('ALERT: duplicate primary keys in cur_* tables')\n"
        + ch_shell_suffix()
    )
