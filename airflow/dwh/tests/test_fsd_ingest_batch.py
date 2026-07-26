from datetime import datetime

import pandas as pd

from dwh.common.fsd_ingest import (
    DEFAULT_BATCH_SIZE,
    _batch_cursor,
    _build_extract_sql,
    _order_by_clause,
    _watermark_predicate,
    batch_size,
)


def test_default_batch_size():
    assert batch_size({}) == DEFAULT_BATCH_SIZE
    assert batch_size({"batch_size": 50_000}) == 50_000


def test_order_by_includes_primary_key():
    entity_cfg = {"primary_key": ["id"], "watermark_column": "updated_at"}
    assert _order_by_clause(entity_cfg, "updated_at") == "updated_at, id"


def test_build_extract_sql_default_adds_order_and_limit():
    entity_cfg = {
        "source_table": "public.tickets",
        "primary_key": ["id"],
    }
    sql = _build_extract_sql(entity_cfg, watermark_column="updated_at", batch_limit=100_000)
    assert "FROM public.tickets" in sql
    assert "WHERE updated_at > %(watermark)s" in sql
    assert "ORDER BY updated_at, id" in sql
    assert "LIMIT %(limit)s" in sql


def test_build_extract_sql_custom_preserves_order_and_adds_limit():
    entity_cfg = {
        "extract_sql": (
            "SELECT id FROM public.history\n"
            "WHERE updated_at > %(watermark)s\n"
            "ORDER BY updated_at, id"
        ),
    }
    sql = _build_extract_sql(entity_cfg, watermark_column="updated_at", batch_limit=100_000)
    assert sql.endswith("LIMIT %(limit)s")
    assert "ORDER BY updated_at, id" in sql


def test_watermark_predicate_uses_keyset_after_first_batch():
    entity_cfg = {"primary_key": ["id"]}
    predicate = _watermark_predicate(
        entity_cfg,
        watermark_column="updated_at",
        cursor_after={"watermark_value": "2024-01-01", "pk": "uuid"},
    )
    assert "updated_at > %(cursor_wm)s" in predicate
    assert "id > %(cursor_pk)s" in predicate


def test_batch_cursor_uses_msk_iso_string():
    entity_cfg = {"primary_key": ["id"]}
    df = pd.DataFrame(
        {
            "id": ["a", "b"],
            "updated_at": [
                datetime(2024, 1, 1, 10, 0, 0),
                datetime(2024, 1, 1, 11, 0, 0),
            ],
        }
    )
    cursor = _batch_cursor(df, entity_cfg, "updated_at")
    assert isinstance(cursor["watermark_value"], str)
    assert "+03:00" in cursor["watermark_value"]
    assert cursor["pk"] == "b"
