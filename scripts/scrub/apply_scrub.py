#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""NDA scrub + Gelassenheit renames across the monorepo."""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# Order matters: longer / more specific first
REPLACEMENTS: list[tuple[str, str]] = [
    # hosts / infra
    (r"https://metabase\.esp\.local", "http://localhost:3000"),
    (r"metabase\.esp\.local", "localhost:3000"),
    (r"gitlab\.esp\.local", "gitlab.example.com"),
    (r"s-sd-anl-dbt00\.esp\.local", "dbt"),
    (r"sd-anl-dbt00\.esp\.local", "dbt"),
    (r"s-sd-anl-s3\.esp\.local", "minio:9000"),
    (r"sd-anl-s3\.esp\.local", "minio:9000"),
    (r"s-ch00\.esp\.local", "clickhouse"),
    (r"s-ch01\.esp\.local", "clickhouse"),
    (r"nornir\.esp\.local", "bastion"),
    (r"\bnornir\b", "bastion"),
    (r"sd-anl-dbt00", "dbt"),
    (r"sd-anl-s3", "minio"),
    (r"\.esp\.local", ".example.com"),
    (r"esp\.local", "example.com"),
    # CH databases / schemas
    (r"sync_amocrm", "gel_crm"),
    (r"sync_mango", "gel_pbx"),
    (r"sync_skorozvon", "gel_dialer"),
    (r"sync_gs", "gel_workforce"),
    (r"sync_fsd", "gel_helpdesk"),  # after more specific sync_* 
    (r"ch_db_amocrm", "ch_db_crm"),
    (r"ch_db_mango", "ch_db_pbx"),
    (r"ch_db_skorozvon", "ch_db_dialer"),
    (r"ch_db_gs", "ch_db_workforce"),
    (r"ch_db_fsd", "ch_db_helpdesk"),
    (r"ch_db_orchestrator", "ch_db_bot"),
    (r"CH_DATABASE_AMOCRM", "CH_DATABASE_CRM"),
    (r"CH_DATABASE_MANGO", "CH_DATABASE_PBX"),
    (r"CH_DATABASE_SKOROZVON", "CH_DATABASE_DIALER"),
    (r"CH_DATABASE_GS", "CH_DATABASE_WORKFORCE"),
    (r"CH_DATABASE_FSD", "CH_DATABASE_HELPDESK"),
    (r"CH_DATABASE_ORCHESTRATOR", "CH_DATABASE_BOT"),
    # product / company brands
    (r"ao-esp\.ru", "example.com"),
    (r"ao-esp", "gelassenheit"),
    (r"АО «ЕСП»", "Gelassenheit"),
    (r"АО ЕСП", "Gelassenheit"),
    (r"Честн(?:ым|ый|ого)? Знаком?", "Compliance Mark"),
    (r"ЦРПТ", "TraceReg"),
    (r"ПМСР", "ProductB"),
    (r"ЕСМ", "ProductA"),
    (r"ЛКП", "Partner Portal"),
    (r"ЛКК", "Client Portal"),
    (r"FSD Orchestrator", "Support Bot Orchestrator"),
    (r"fsd_orchestrator", "bot_orchestrator"),
    (r"ingest_orchestrator", "ingest_bot"),
    (r"ORCHESTRATOR_", "BOT_"),
    (r"orchestrator_bot", "support_bot"),
    (r"orchestrator", "bot"),  # late; careful with already renamed
    # telephony brands
    (r"Skorozvon", "Dialer"),
    (r"skorozvon", "dialer"),
    (r"Mango Office", "PBX Cloud"),
    (r"Mango", "PBX"),
    (r"mango", "pbx"),
    (r"AmoCRM", "CRM"),
    (r"amocrm", "crm"),
    # paths / buckets
    (r"dwh-lake", "gel-lake"),
    (r"dwh-airflow", "gelassenheit-airflow"),
    (r"dbt-etl", "gelassenheit-dbt"),
    (r"service-desk/analytics", "gelassenheit/analytics"),
    (r"dwh-dbt", "gel-dbt"),
    (r"dwh-s3", "gel-s3"),
    # people / secrets patterns (common hardcoded leftovers)
    (r"E9Df6b\$MG#4BJr", "CHANGEME_SSH_PASSWORD"),
    (r"JamesHetfield", "CHANGEME_SSH_PASSWORD"),
    (r"SerjTankian", "CHANGEME_PG_PASSWORD"),
    (r"d\.gorokhov@[^\"'\s]+", "analyst@example.com"),
    (r"dim\.gorohov[^\"'\s]*", "analyst@example.com"),
    (r"admin@ao-esp\.ru", "admin@example.com"),
    # FSD wording → Helpdesk (display; keep model path fsd/ for less churn where needed)
    (r"Overview \[TechSup\]", "Overview [Support]"),
    (r"TechSup", "Support"),
    (r"mart_fsd__", "mart_helpdesk__"),
    (r"int_fsd__", "int_helpdesk__"),
    (r"stg_fsd__", "stg_helpdesk__"),
    (r"int_esp__", "int_devices__"),
    (r"mart_esp__", "mart_devices__"),
    (r"int_mango__", "int_pbx__"),
    (r"mart_mango__", "mart_pbx__"),
    (r"int_skorozvon__", "int_dialer__"),
    (r"mart_skorozvon__", "mart_dialer__"),
    (r"int_amocrm__", "int_crm__"),
    (r"mart_amocrm__", "mart_crm__"),
    (r"int_gs__", "int_workforce__"),
    (r"mart_gs__", "mart_workforce__"),
    (r"int_orchestrator__", "int_bot__"),
    (r"mart_orchestrator__", "mart_bot__"),
    (r"tag:helpdesk", "tag:helpdesk"),
    (r"tag:mart_fsd", "tag:mart_helpdesk"),
    (r"tag:mart_mango", "tag:mart_pbx"),
    (r"'fsd'", "'helpdesk'"),
    (r'"fsd"', '"helpdesk"'),
]

SKIP_DIRS = {
    ".git",
    "__pycache__",
    "target",
    "dbt_packages",
    "logs",
    ".pytest_cache",
}
TEXT_EXT = {
    ".py",
    ".sql",
    ".yml",
    ".yaml",
    ".md",
    ".txt",
    ".toml",
    ".cfg",
    ".ini",
    ".json",
    ".example",
    ".gitignore",
    ".airflowignore",
    ".env",
    "Dockerfile",
    "Makefile",
}


def is_text(path: Path) -> bool:
    if path.name in TEXT_EXT or path.name.startswith("Dockerfile"):
        return True
    return path.suffix.lower() in TEXT_EXT


def scrub_text(text: str) -> str:
    out = text
    for pat, repl in REPLACEMENTS:
        out = re.sub(pat, repl, out)
    return out


def main() -> int:
    n_files = 0
    n_changed = 0
    for path in ROOT.rglob("*"):
        if not path.is_file():
            continue
        if any(p in SKIP_DIRS for p in path.parts):
            continue
        if path.name == "apply_scrub.py":
            continue
        if not is_text(path):
            continue
        raw = path.read_bytes()
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            try:
                text = raw.decode("cp1251")
            except UnicodeDecodeError:
                continue
        n_files += 1
        new = scrub_text(text)
        if new != text:
            path.write_text(new, encoding="utf-8", newline="\n")
            n_changed += 1
    print(f"scanned={n_files} changed={n_changed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
