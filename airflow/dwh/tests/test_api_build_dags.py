from dwh.ingest.api.build_api_dags import _build_dag


def test_watermark_runs_after_empty_window_skip():
    dag = _build_dag("pbx", {"entity": "calls"}, "@hourly")

    watermark = dag.task_dict["watermark"]
    upstream = set(watermark.upstream_task_ids)

    assert watermark.trigger_rule == "none_failed_min_one_success"
    assert "dbt_test_int_pbx__calls_current" in upstream
    assert "wait_dbt_repo_sync" in upstream
