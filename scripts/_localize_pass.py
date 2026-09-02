#!/usr/bin/env python3
"""One-shot EN localization helpers (imported by localize_en.py)."""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CYR = re.compile(r"[\u0400-\u04FF\u0451\u0401]")
CYR_CHAR = re.compile(r"[\u0400-\u04FF\u0451\u0401]")

SOURCE_TICKET_GEO = Path(r"C:\Users\Esp\Downloads\dwh-airflow\dwh\common\ticket_geo.py")

NC_ENTITIES = """# Nextcloud WebDAV xlsx → bronze/gs → dbt gel_workforce (full snapshot).
# Schedule: per-minute sheet slots → flatten (date, employee, plan hours).
conn_id: nextcloud_calltrafic
host: cloud.calltraffic.ru
# On this host WebDAV root is the user UUID, not the login name.
# Login/password live in the Connection; UUID may be duplicated in extra.dav_user.
dav_path: "/remote.php/dav/files/0D810015-CF53-4FBB-80EB-779B9E685B80/Workforce Training/Employee timesheet.xlsx"
employees_sheet: "Employee list"

entities:
  - entity: calltrafic1_techsup_schedule
    kind: schedule
    columns: [date, employee, hours]
    # Col27 = plan; Col2 = full name; Col30 = date (same layout as GS AKC QUERY)
    plan_col_1based: 27
    name_col_1based: 2
    date_col_1based: 30
    skip_name_labels:
      - "actual hours"
      - "required hours (load plan)"

  - entity: calltrafic_employees
    kind: employees
    sheet: "Employee list"
    has_header: true
    # Sheet headers → bronze columns (anonymized xlsx)
    header_map:
      "Supervisor": supervisor
      "Status": employment_status
      "Group": group_name
      "Training start date": training_start_date
      "Training end date": training_end_date
      "Line start date": line_start_date
      "Full name": full_name
      "Email": email
      "Telegram login": telegram_login
      "Mattermost login": mm_login
      "Phone": phone
      "Birthday": birthday
      "Dismissal date": dismissal_date
      "Dismissal reason": dismissal_reason
    required_column: full_name
    columns:
      - supervisor
      - employment_status
      - group_name
      - training_start_date
      - training_end_date
      - line_start_date
      - full_name
      - email
      - telegram_login
      - mm_login
      - phone
      - birthday
      - dismissal_date
      - dismissal_reason
"""

NDA_SCRUB = """# NDA scrub checklist

This portfolio repo is scrubbed from an internal pipeline. Before publishing, re-run:

```powershell
Get-ChildItem -Recurse -File -Include *.py,*.sql,*.yml,*.yaml,*.md,*.json,*.example |
  Select-String -Pattern 'esp\\.local|ao-esp|nornir|JamesHetfield|SerjTankian|sync_fsd|metabase\\.esp|E9Df6b|\\u0415\\u0421\\u041c|\\u041f\\u041c\\u0421\\u0420|\\u0427\\u0435\\u0441\\u0442\\u043d|\\u041b\\u041a\\u041f|\\u0426\\u0420\\u041f\\u0422' |
  Select-Object -First 50
```

Expected: **no matches** outside `scripts/scrub/apply_scrub.py` (patterns list).

## English-only human text

All comments, docstrings, dbt catalog descriptions, docs, config notes, and demo fixture strings must be **English**. Re-check before publish:

```powershell
Get-ChildItem -Recurse -File -Path . |
  Where-Object { $_.FullName -notmatch '\\\.git\\' } |
  ForEach-Object {
    $rel = $_.FullName.Replace((Get-Location).Path + '\', '').Replace('\', '/')
    if ($rel -eq 'scripts/scrub/apply_scrub.py') { return }
    if (Select-String -Path $_.FullName -Pattern '[\u0400-\u04FF\u0451\u0401]' -Quiet -ErrorAction SilentlyContinue) {
      $rel
    }
  }
```

Expected: **no output** (Cyrillic is allowed only in `scripts/scrub/apply_scrub.py` NDA search regexes).

Optional automation: `python scripts/localize_en.py` (requires `pip install deep-translator`; prints remaining Cyrillic file count at the end).

## Replaced

- Hostnames / VPN names → `localhost` / `clickhouse` / `minio` / `bastion`
- Company / product brands → Gelassenheit / ProductA / ProductB / portals
- CH databases → `gel_*` split domains
- Secrets → `CHANGEME_*` or removed
- Business dictionaries → generic L1–L3 / New–Closed themes

## Intentionally omitted

- Production credentials and session cookies
- One-off prod deploy/probe scripts with live IPs
- Real customer / employee datasets
- Private Git remotes
"""


def escape_cyrillic_chars(text: str) -> str:
    out: list[str] = []
    for ch in text:
        if CYR_CHAR.match(ch):
            out.append(f"\\u{ord(ch):04x}")
        else:
            out.append(ch)
    return "".join(out)


def rebuild_ticket_geo() -> None:
    if not SOURCE_TICKET_GEO.is_file():
        return
    text = SOURCE_TICKET_GEO.read_text(encoding="utf-8")
    subs = [
        ("Metabase maps", "BI maps"),
        ("s3://dwh-lake/", "s3://gel-lake/"),
        ("Metabase reads sync_fsd.", "BI reads gel_helpdesk."),
        ("CH_DATABASE_FSD", "CH_DATABASE_HELPDESK"),
    ]
    for old, new in subs:
        text = text.replace(old, new)
    text = escape_cyrillic_chars(text)
    dst = ROOT / "airflow/dwh/common/ticket_geo.py"
    dst.write_text(text, encoding="utf-8")


def write_static_files() -> None:
    (ROOT / "airflow/dwh/config/nc_entities.yaml").write_text(NC_ENTITIES, encoding="utf-8")
    (ROOT / "docs/NDA_SCRUB.md").write_text(NDA_SCRUB, encoding="utf-8")
    for name in ("_cyr_sample.txt", "_cyr_lines.json"):
        p = ROOT / name
        if p.is_file():
            p.unlink()


def patch_gitignores() -> None:
    for rel, old, new in [
        (
            "airflow/.gitignore",
            "# \u043b\u043e\u043a\u0430\u043b\u044c\u043d\u044b\u0435 one-off deploy/probe",
            "# local one-off deploy/probe",
        ),
        (
            "dbt/.gitignore",
            "# \u043b\u043e\u043a\u0430\u043b\u044c\u043d\u044b\u0435 one-off / probe / \u043b\u043e\u0433\u0438 (\u043d\u0435 \u043a\u043e\u043c\u043c\u0438\u0442\u0438\u0442\u044c)",
            "# local one-off / probe / logs (do not commit)",
        ),
    ]:
        p = ROOT / rel
        t = p.read_text(encoding="utf-8")
        if old in t:
            p.write_text(t.replace(old, new), encoding="utf-8")


def fix_localize_en_self() -> None:
    p = ROOT / "scripts/localize_en.py"
    t = p.read_text(encoding="utf-8")
    t = t.replace(
        "legacy brand tokens (see NDA_SCRUB.md audit regex)",
        "legacy brand tokens (see NDA_SCRUB.md audit regex)",
    )
    p.write_text(t, encoding="utf-8")


def _translate_text(text: str, cache: dict[str, str]) -> str:
    import time

    from deep_translator import MyMemoryTranslator

    if text in cache:
        return cache[text]
    tr = MyMemoryTranslator(source="ru-RU", target="en-US")

    def _call(block: str) -> str:
        for attempt in range(5):
            try:
                time.sleep(0.2)
                return tr.translate(block)
            except Exception:
                time.sleep(1.5 * (attempt + 1))
        return block

    # MyMemory limit: 500 chars per request — chunk by line groups.
    if len(text) <= 480:
        out = _call(text)
        cache[text] = out
        return out
    parts: list[str] = []
    buf: list[str] = []
    size = 0
    for para in text.split("\n"):
        chunk = para + "\n"
        if size + len(chunk) > 450 and buf:
            parts.append(_call("".join(buf)))
            buf = [chunk]
            size = len(chunk)
        else:
            buf.append(chunk)
            size += len(chunk)
    if buf:
        parts.append(_call("".join(buf)))
    out = "".join(parts)
    cache[text] = out
    return out


def _yaml_description_block_indent(line: str) -> int | None:
    """Indent of a folded/literal description block content line, or None."""
    m = re.match(r"^(\s+)description:\s*(?:>|\|)\s*$", line)
    if m:
        return len(m.group(1)) + 2
    return None


def _should_auto_translate_line(line: str, in_yaml_description: bool) -> bool:
    if not CYR.search(line):
        return False
    if in_yaml_description:
        return True
    stripped = line.lstrip()
    if stripped.startswith(("#", "--", "*", "{#")):
        return True
    if "description:" in line and CYR.search(line.split("description:", 1)[-1]):
        return True
    if stripped.startswith("|") and CYR.search(line):
        return True
    return False


def _translate_comment_blocks(raw: str, cache: dict[str, str]) -> str:
    """One API call per file: batch all comment/jinja lines, preserve code lines."""
    lines = raw.splitlines(keepends=True)
    markers: list[tuple[int, str | None]] = []
    batch: list[str] = []
    in_jinja = False
    yaml_desc_indent: int | None = None
    for idx, line in enumerate(lines):
        bare = line.rstrip("\n\r")
        if "{#" in bare:
            in_jinja = True
        block_indent = _yaml_description_block_indent(bare)
        if block_indent is not None:
            yaml_desc_indent = block_indent
        elif yaml_desc_indent is not None:
            stripped = bare.lstrip()
            if stripped and not bare.startswith(" " * yaml_desc_indent):
                if re.match(r"^(\s+)(-\s|\w)", bare) and len(bare) - len(stripped) < yaml_desc_indent:
                    yaml_desc_indent = None
            elif not stripped:
                pass
            elif len(bare) - len(stripped) < yaml_desc_indent:
                yaml_desc_indent = None
        in_yaml_desc = yaml_desc_indent is not None and bool(bare.strip())
        if in_yaml_desc and block_indent is None:
            if bare.strip() and len(bare) - len(bare.lstrip()) >= yaml_desc_indent:
                in_yaml_desc = True
            else:
                in_yaml_desc = False
        translatable = bool(
            CYR.search(bare)
            and (
                in_jinja
                or _should_auto_translate_line(bare, in_yaml_desc and block_indent is None)
            )
        )
        if translatable:
            markers.append((idx, f"__L{len(batch)}__"))
            batch.append(bare)
        else:
            markers.append((idx, None))
        if "#}" in bare:
            in_jinja = False
    if not batch:
        return raw
    joined = "\n".join(batch)
    translated = _translate_text(joined, cache).split("\n")
    if len(translated) != len(batch):
        translated = [_translate_text(x, cache) for x in batch]
    out_lines = [lines[i].rstrip("\n\r") for i in range(len(lines))]
    bi = 0
    for idx, tag in markers:
        if tag is not None:
            out_lines[idx] = translated[bi]
            bi += 1
    ending = "\n" if raw.endswith("\n") else ""
    return "\n".join(out_lines) + ending


def auto_translate_repo(cache: dict[str, str] | None = None) -> int:
    """Translate comments/docs; skip apply_scrub and ticket_geo (unicode escapes)."""
    cache = cache or {}
    exts = {".py", ".sql", ".yml", ".yaml", ".md", ".json"}
    skip_parts = {".git", "target", "__pycache__", "dbt_packages"}
    changed = 0
    for path in sorted(ROOT.rglob("*")):
        if not path.is_file():
            continue
        if any(p in path.parts for p in skip_parts):
            continue
        rel = path.relative_to(ROOT).as_posix()
        if rel in ("scripts/scrub/apply_scrub.py", "scripts/_localize_pass.py"):
            continue
        if path.suffix.lower() not in exts:
            continue
        try:
            raw = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if not CYR.search(raw):
            continue
        if rel == "airflow/dwh/common/ticket_geo.py":
            continue
        if rel.endswith(".md"):
            new = _translate_text(raw, cache)
        elif rel.endswith(".json"):
            new = _translate_text(raw, cache)
        else:
            new = _translate_comment_blocks(raw, cache)
        if new != raw:
            path.write_text(new, encoding="utf-8")
            changed += 1
    return changed


def strip_geo_csvs() -> None:
    for path in ROOT.glob("airflow/dwh/data/geo/**/*.csv"):
        if not path.is_file():
            continue
        raw = path.read_text(encoding="utf-8")
        if "geonameid,name,asciiname" not in raw:
            continue
        lines: list[str] = []
        for i, line in enumerate(raw.splitlines()):
            if i == 0 or not line.strip():
                lines.append(line)
                continue
            parts = line.split(",", 3)
            if len(parts) < 4:
                lines.append(line)
                continue
            alt_clean = re.sub(r"[\u0400-\u04FF\u0451\u0401]+", " ", parts[3])
            alt_clean = re.sub(r"\s+", " ", alt_clean).strip()
            lines.append(f"{parts[0]},{parts[1]},{parts[2]},{alt_clean}")
        new = "\n".join(lines) + ("\n" if raw.endswith("\n") else "")
        if new != raw:
            path.write_text(new, encoding="utf-8")


def run_static_pass() -> None:
    write_static_files()
    rebuild_ticket_geo()
    patch_gitignores()
    fix_localize_en_self()
    strip_geo_csvs()
