#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate synthetic Helpdesk + Bot demo data for Gelassenheit ELT."""
from __future__ import annotations

import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import urllib.request
except ImportError:
    pass

ROOT = Path(__file__).resolve().parents[1]
OUT_SQL = ROOT / "fixtures" / "sql" / "seed_demo_data.sql"
CH_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CH_PORT = os.environ.get("CLICKHOUSE_HTTP_PORT", os.environ.get("CLICKHOUSE_PORT", "8123"))
CH_USER = os.environ.get("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "gel")

STATUSES = ["New", "In Progress", "Deferred", "Waiting Vendor", "Resolved", "Closed", "Reopened"]
GROUPS = ["L1 Support", "L2 Support", "L3 Support", "Dispatch", "Integrations", "Billing"]
THEMES = ["Access", "Billing", "ProductA Setup", "ProductB Error", "Integrations", "General"]
CHANNELS = ["Email", "Partner Portal", "Client Portal", "Phone", "Chat"]
OUTCOMES = ["resolved", "continued", "handoff", "rejected", ""]

MSK = timezone(timedelta(hours=3))


def gen(
    n_tickets: int = 500,
    days: int = 14,
    
) -> tuple[list[dict], list[dict]]:
    random.seed(42)
    now = datetime.now(MSK)
    tickets: list[dict] = []
    sessions: list[dict] = []
    for i in range(n_tickets):
        tid = uuid.uuid4()
        created = now - timedelta(days=random.randint(0, days), hours=random.randint(0, 23))
        status = random.choice(STATUSES)
        group = random.choice(GROUPS)
        theme = random.choice(THEMES)
        channel = random.choice(CHANNELS)
        tickets.append(
            {
                "id": str(tid),
                "number": f"HD-{100000 + i}",
                "created_at": created.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "updated_at": (created + timedelta(hours=random.randint(1, 48))).strftime(
                    "%Y-%m-%d %H:%M:%S.%f"
                )[:-3],
                "current_status_name": status,
                "user_group_name": group,
                "service_theme": theme,
                "new_channel": channel,
            }
        )
        if random.random() < 0.35:
            answered = 1 if random.random() < 0.7 else 0
            continued = 1 if answered and random.random() < 0.25 else 0
            sessions.append(
                {
                    "ticket_id": str(tid),
                    "started_at": (created + timedelta(minutes=random.randint(1, 120))).strftime(
                        "%Y-%m-%d %H:%M:%S.%f"
                    )[:-3],
                    "is_classified": 1 if random.random() < 0.9 else 0,
                    "is_answered": answered,
                    "user_continued": continued,
                    "rating": round(random.uniform(1, 5), 1) if random.random() < 0.4 else None,
                    "outcome": random.choice(OUTCOMES),
                }
            )
    return tickets, sessions


def to_sql(tickets: list[dict], sessions: list[dict]) -> str:
    lines = [
        "TRUNCATE TABLE IF EXISTS gel_helpdesk.demo_tickets;",
        "TRUNCATE TABLE IF EXISTS gel_bot.demo_bot_sessions;",
    ]
    for t in tickets:
        lines.append(
            "INSERT INTO gel_helpdesk.demo_tickets VALUES ("
            f"toUUID('{t['id']}'), '{t['number']}', "
            f"toDateTime64('{t['created_at']}', 3, 'Europe/Moscow'), "
            f"toDateTime64('{t['updated_at']}', 3, 'Europe/Moscow'), "
            f"'{t['current_status_name']}', '{t['user_group_name']}', "
            f"'{t['service_theme']}', '{t['new_channel']}');"
        )
    for s in sessions:
        rating = "NULL" if s["rating"] is None else str(s["rating"])
        lines.append(
            "INSERT INTO gel_bot.demo_bot_sessions VALUES ("
            f"toUUID('{s['ticket_id']}'), "
            f"toDateTime64('{s['started_at']}', 3, 'Europe/Moscow'), "
            f"{s['is_classified']}, {s['is_answered']}, {s['user_continued']}, "
            f"{rating}, '{s['outcome']}');"
        )
    return "\n".join(lines) + "\n"


def ch_exec(sql: str) -> None:
    import base64
    from urllib.error import HTTPError

    url = f"http://{CH_HOST}:{CH_PORT}/"
    req = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
    token = base64.b64encode(f"{CH_USER}:{CH_PASSWORD}".encode()).decode()
    req.add_header("Authorization", f"Basic {token}")
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            resp.read()
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"ClickHouse HTTP {exc.code}: {body}") from exc


def ch_exec_script(script: str) -> None:
    """Execute SQL script statement-by-statement (CH HTTP rejects multiquery)."""
    for stmt in script.split(";"):
        lines = [ln for ln in stmt.splitlines() if ln.strip() and not ln.strip().startswith("--")]
        if not lines:
            continue
        ch_exec("\n".join(lines) + ";")



def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Synthetic Helpdesk + Bot demo fixtures")
    ap.add_argument("--tickets", type=int, default=500)
    ap.add_argument("--days", type=int, default=14, help="lookback window in days")
    args = ap.parse_args()
    
    tickets, sessions = gen(n_tickets=args.tickets, days=args.days)
    sql = to_sql(tickets, sessions)
    OUT_SQL.parent.mkdir(parents=True, exist_ok=True)
    OUT_SQL.write_text(sql, encoding="utf-8")
    print(f"wrote {OUT_SQL} tickets={len(tickets)} sessions={len(sessions)}")

    init = (ROOT / "fixtures" / "sql" / "init_clickhouse.sql").read_text(encoding="utf-8")
    try:
        ch_exec_script(init)
        ch_exec_script(sql)
        print(f"loaded into ClickHouse @ {CH_HOST}:{CH_PORT}")
    except Exception as exc:
        print(f"ClickHouse load skipped ({exc}). Run after: docker compose up -d")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
