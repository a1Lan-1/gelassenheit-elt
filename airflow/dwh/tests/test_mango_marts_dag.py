from dwh.ingest.pbx.run_pbx_marts_dag import _MANGO_MARTS_SELECT, run_pbx_marts


def test_pbx_marts_select_includes_detail_and_marts():
    assert "int_pbx__calls_detail" in _MANGO_MARTS_SELECT
    assert "tag:mart_pbx" in _MANGO_MARTS_SELECT
    assert "call_legs" not in _MANGO_MARTS_SELECT
    assert "call_conversions" not in _MANGO_MARTS_SELECT


def test_pbx_marts_dag_id():
    dag = run_pbx_marts()
    assert dag.dag_id == "dwh_run_pbx_marts"
    assert "dbt_run_pbx_marts" in dag.task_dict
    assert "dbt_test_pbx_marts" in dag.task_dict
