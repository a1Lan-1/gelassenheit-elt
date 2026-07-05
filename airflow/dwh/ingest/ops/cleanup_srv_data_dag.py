"""Remove legacy temp parquet files from host-mounted /srv/airflow/data (not in S3)."""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag
from airflow.operators.bash import BashOperator

# Host bind-mount (legacy etl_sd used /opt/airflow/data inside container → often same volume)
DATA_DIRS = "/srv/airflow/data /opt/airflow/data"


@dag(
    dag_id="dwh_ops_cleanup_srv_data",
    schedule="30 2 * * *",
    start_date=datetime(2026, 6, 3),
    catchup=False,
    tags=["dwh", "ops", "cleanup"],
    max_active_runs=1,
    doc_md="""
    Deletes stale `extract_*.parquet` / `*.parquet` left by legacy DAGs under
    **`/srv/airflow/data`** on the Airflow host (volume mount, not bronze S3).

    Manual run: trigger this DAG once after deploy.
    """,
)
def ops_cleanup_srv_data():
    BashOperator(
        task_id="cleanup_parquet_temp",
        bash_command=f"""
set -euo pipefail
for dir in {DATA_DIRS}; do
  if [[ ! -d "$dir" ]]; then
    echo "skip missing $dir"
    continue
  fi
  echo "=== $dir before ==="
  du -sh "$dir" 2>/dev/null || true
  n=$(find "$dir" -maxdepth 1 -type f \\
    \\( -name '*.parquet' -o -name 'extract_*.parquet' -o -name 'data.parquet' \\) 2>/dev/null | wc -l)
  echo "parquet files: $n"
  find "$dir" -maxdepth 1 -type f \\
    \\( -name '*.parquet' -o -name 'extract_*.parquet' -o -name 'data.parquet' \\) \\
    -delete 2>/dev/null || true
  echo "=== $dir after ==="
  du -sh "$dir" 2>/dev/null || true
done
""",
    )


ops_cleanup_srv_data()
