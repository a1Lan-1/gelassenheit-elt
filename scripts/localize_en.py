#!/usr/bin/env python3
"""Portfolio repo: translate human-readable text and domain enums to English."""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CYR = re.compile(r"[\u0400-\u04FF\u0451\u0401]")

SKIP_PARTS = {".git", "target", "__pycache__", "dbt_packages", "seed_demo_data.sql"}
SKIP_FILES = {"scripts/scrub/apply_scrub.py"}
SKIP_GLOB: list[str] = []

# Longest first
TEXT_REPLACEMENTS: list[tuple[str, str]] = [
    # --- domain literals (source enums → English portfolio) ---
    ("'L1 Support'", "'L1 Support'"),
    ("'L2 Support'", "'L2 Support'"),
    ("'L1 inbound calls'", "'L1 inbound calls'"),
    ("'L2 technical support'", "'L2 technical support'"),
    ("'New ticket'", "'New ticket'"),
    ("'individual assignment'", "'individual assignment'"),
    ("'Idle'", "'Idle'"),
    ("'no theme'", "'no theme'"),
    ("'no status'", "'no status'"),
    ("'no outcome'", "'no outcome'"),
    ("'Issue resolved'", "'Issue resolved'"),
    ("'Issue not resolved'", "'Issue not resolved'"),
    ("'Day'", "'Day'"),
    ("'Week'", "'Week'"),
    ("'Month'", "'Month'"),
    ("'Terminated'", "'Terminated'"),
    ("'Active'", "'Active'"),
    ("'<1h'", "'<1h'"),
    ("'1-4h'", "'1-4h'"),
    ("'4-24h'", "'4-24h'"),
    ("'1-3d'", "'1-3d'"),
    ("'3-7d'", "'3-7d'"),
    ("'>7d'", "'>7d'"),
    ("'Spam / irrelevant request'", "'Spam / irrelevant request'"),
    # workforce bool synonyms
    ("IN ('true', '1', 'yes', 'y', 't')", "IN ('true', '1', 'yes', 'y', 't')"),
    ("IN ('false', '0', 'no', 'n', 'f')", "IN ('false', '0', 'no', 'n', 'f')"),
    ("{# delete+insert by activity_date; lookback 14d (full history on FR). #}", "{# delete+insert by activity_date; lookback 14d (full history on FR). #}"),
    ("{# false → *_sk = 0 (temporary run without dialer). #}", "{# false → *_sk = 0 (temporary run without dialer). #}"),
    ("{# BI facade; FINAL — one row per id before merge. #}", "{# BI facade; FINAL — one row per id before merge. #}"),
    ("{# Id-first: no FINAL on full tickets_current; dedup by batch id only. #}", "{# Id-first: no FINAL on full tickets_current; dedup by batch id only. #}"),
    ("{# Id-first + prune dims to batch keys only. #}", "{# Id-first + prune dims to batch keys only. #}"),
    # --- line comments ---
    ("-- Single pass: touch (all object_type) + dedup", "-- Single pass: touch (all object_type) + dedup"),
    ("-- Resolve statuses: narrow layer by ticket_id, no repeated JSON on hot path", "-- Resolve statuses: narrow layer by ticket_id, no repeated JSON on hot path"),
    ("-- First = first Resolved; Full = last Resolved (Closed not required).", "-- First = first Resolved; Full = last Resolved (Closed not required)."),
    ("-- Deferred pause: same intervals as queue episodes", "-- Deferred pause: same intervals as queue episodes"),
    ("-- Pause in [created_at, end_at] for each metric anchor", "-- Pause in [created_at, end_at] for each metric anchor"),
    ("-- Pause markers: deferred + terminators", "-- Pause markers: deferred + terminators"),
    ("-- Spam filter without mart FINAL: dedup via int enriched", "-- Spam filter without mart FINAL: dedup via int enriched"),
    ("-- spam service (fixed service_id)", "-- spam service (fixed service_id)"),
    ("-- No FINAL: PREWHERE + LIMIT 1 BY (ticket_id, …)", "-- No FINAL: PREWHERE + LIMIT 1 BY (ticket_id, …)"),
    ("-- Fallback JSON: old rows may have empty new_*/old_* before FR layer.", "-- Fallback JSON: old rows may have empty new_*/old_* before FR layer."),
    ("-- create / initial group assignment: empty old, non-empty new", "-- create / initial group assignment: empty old, non-empty new"),
    ("-- Before cutover dispatch maps to L1 Support (rename, not duplicate row)", "-- Before cutover dispatch maps to L1 Support (rename, not duplicate row)"),
    ("-- Entry on create group (empty old in user_group_change), not current enriched", "-- Entry on create group (empty old in user_group_change), not current enriched"),
    ("-- Moments ticket enters a group (for status as-of)", "-- Moments ticket enters a group (for status as-of)"),
    ("-- completed / reopen (incl. informal); deferred/resumed as separate rows below", "-- completed / reopen (incl. informal); deferred/resumed as separate rows below"),
    ("-- formal reopen", "-- formal reopen"),
    ("-- informal: left terminal to non-terminal", "-- informal: left terminal to non-terminal"),
    ("-- Resolved→Closed excluded (new is terminal)", "-- Resolved→Closed excluded (new is terminal)"),
    ("-- Dedup group change ±60s; exclude New ticket status", "-- Dedup group change ±60s; exclude New ticket status"),
    ("-- Active-time pause (optional BI: wall vs active)", "-- Active-time pause (optional BI: wall vs active)"),
    ("-- call_end in stg includes +2 min after outbound (dialer + PBX)", "-- call_end in stg includes +2 min after outbound (dialer + PBX)"),
    ("-- calltrafic temporarily excluded: Nextcloud 403 from Airflow egress", "-- calltrafic temporarily excluded: Nextcloud 403 from Airflow egress"),
    ("-- PBX: talk threshold (aht_talk); duration numerator for counted types only.", "-- PBX: talk threshold (aht_talk); duration numerator for counted types only."),
    ("-- Planned hours from all workforce sheets.", "-- Planned hours from all workforce sheets."),
    ("-- toJSONString(NULL) → NULL; CH concat nulls whole string — create uses new_group only", "-- toJSONString(NULL) → NULL; CH concat nulls whole string — create uses new_group only"),
    ("-- Step dialogs: one row per agent dialog (entry_id, dialog_seq).", "-- Step dialogs: one row per agent dialog (entry_id, dialog_seq)."),
    ("-- Incremental delete+insert by entry_id for lookback (late legs change dialog_seq).", "-- Incremental delete+insert by entry_id for lookback (late legs change dialog_seq)."),
    ("-- No FULL FINAL on history: lookback first, then dedup.", "-- No FULL FINAL on history: lookback first, then dedup."),
    ("-- Entry-level talk/duration for outbound_fallback (no answered user leg).", "-- Entry-level talk/duration for outbound_fallback (no answered user leg)."),
    (
        "   Grain: cashdesk_id + subscription_id (multiple licenses per device on renewal is OK).",
        "   Grain: cashdesk_id + subscription_id (multiple licenses per device on renewal is OK).",
    ),
    ("ReplacingMergeTree on _dbt_loaded_at; uniqueness via tests/assert_* + FINAL.", "ReplacingMergeTree on _dbt_loaded_at; uniqueness via tests/assert_* + FINAL."),
    ("Raw install report (Replacing-append).", "Raw install report (Replacing-append)."),
    ("Grain: cashdesk_id + subscription_id — multiple licenses per device allowed.", "Grain: cashdesk_id + subscription_id — multiple licenses per device allowed."),
    ("description: BI view over mart_ticket_sla_daily.", "description: BI view over mart_ticket_sla_daily."),
    ("description: BI view over mart_ticket_sla_daily.", "description: BI view over mart_ticket_sla_daily."),
    # py docstrings / comments fragments
    ('"""Bronze ingest for bot (SSH bastion → PG)."""', '"""Bronze ingest for bot (SSH bastion → PG)."""'),
    ('"""SSH bastion → local forward to PG host/port from Postgres conn."""', '"""SSH bastion → local forward to PG host/port from Postgres conn."""'),
    ('"""Daily cashdesk geo dictionary (address → city lat/lon)."""', '"""Daily cashdesk geo dictionary (address → city lat/lon)."""'),
    ("# Product DB — do not force gel_helpdesk as default database.", "# Product DB — do not force gel_helpdesk as default database."),
    ("# date_last_update can be null — still advance wall-clock so we don't loop forever", "# date_last_update can be null — still advance wall-clock so we don't loop forever"),
]


def should_skip(path: Path) -> bool:
    rel = path.relative_to(ROOT).as_posix()
    if rel in SKIP_FILES:
        return True
    for part in SKIP_PARTS:
        if part in path.parts:
            return True
    for pat in SKIP_GLOB:
        if path.match(pat):
            return True
    return False


def strip_cyrillic_from_geo_csv(content: str) -> str:
    """Keep geonames metadata ASCII-only in portfolio repo."""
    if "geonameid,name,asciiname" not in content:
        return content
    lines = []
    for i, line in enumerate(content.splitlines()):
        if i == 0 or not line.strip():
            lines.append(line)
            continue
        parts = line.split(",", 3)
        if len(parts) < 4:
            lines.append(line)
            continue
        head, alternates = parts[0], parts[3]
        # drop Cyrillic from alternatenames blob only
        alt_clean = re.sub(r"[\u0400-\u04FF\u0451\u0401]+", " ", alternates)
        alt_clean = re.sub(r"\s+", " ", alt_clean).strip()
        lines.append(f"{parts[0]},{parts[1]},{parts[2]},{alt_clean}")
    return "\n".join(lines) + ("\n" if content.endswith("\n") else "")


def transform(content: str, path: Path) -> str:
    rel = path.relative_to(ROOT).as_posix()
    if "airflow/dwh/data/geo/" in rel and rel.endswith(".csv"):
        content = strip_cyrillic_from_geo_csv(content)
    for old, new in sorted(TEXT_REPLACEMENTS, key=lambda x: -len(x[0])):
        if old:
            content = content.replace(old, new)
    # mojibake lines in yaml descriptions
    if False:  # mojibake guard (legacy)
        lines = []
        for line in content.splitlines():
            if False and "description:" in line:
                line = re.sub(r"description:.*", "description: BI view (see model SQL).", line)
            lines.append(line)
        content = "\n".join(lines) + ("\n" if content.endswith("\n") else "")
    return content


def main() -> int:
    import sys

    _scripts = Path(__file__).resolve().parent
    if str(_scripts) not in sys.path:
        sys.path.insert(0, str(_scripts))
    from _localize_pass import auto_translate_repo, run_static_pass

    run_static_pass()
    exts = {".py", ".sql", ".yml", ".yaml", ".md", ".json", ".sh", ".ps1", ".example", ".csv", ".jinja"}
    changed = 0
    for path in ROOT.rglob("*"):
        if not path.is_file() or should_skip(path):
            continue
        if path.suffix.lower() not in exts and path.name not in ("Makefile", "NOTICE"):
            continue
        try:
            raw = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        new = transform(raw, path)
        if new != raw:
            path.write_text(new, encoding="utf-8")
            changed += 1
    changed += auto_translate_repo()
    print(f"updated {changed} files")
    remaining = []
    for path in ROOT.rglob("*"):
        if not path.is_file() or should_skip(path):
            continue
        if path.suffix.lower() not in exts and path.name not in ("Makefile", "NOTICE"):
            continue
        try:
            t = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if CYR.search(t):
            remaining.append(path.relative_to(ROOT).as_posix())
    print(f"files with Cyrillic (excl. skip list): {len(remaining)}")
    for r in sorted(remaining)[:60]:
        print(" ", r)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
