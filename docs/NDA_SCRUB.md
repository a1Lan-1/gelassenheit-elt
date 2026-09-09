# NDA scrub checklist

This portfolio repo is scrubbed from an internal pipeline. Before publishing, re-run:

```powershell
Get-ChildItem -Recurse -File -Include *.py,*.sql,*.yml,*.yaml,*.md,*.json,*.example |
  Select-String -Pattern 'esp\.local|ao-esp|nornir|JamesHetfield|SerjTankian|sync_fsd|metabase\.esp|E9Df6b|\u0415\u0421\u041c|\u041f\u041c\u0421\u0420|\u0427\u0435\u0441\u0442\u043d|\u041b\u041a\u041f|\u0426\u0420\u041f\u0422' |
  Select-Object -First 50
```

Expected: **no matches** outside `scripts/scrub/apply_scrub.py` (patterns list).

## Replaced

- Hostnames / VPN names → `localhost` / `clickhouse` / `minio` / `bastion`
- Company / product brands → Gelassenheit / ProductA / ProductB / portals
- CH databases → `gel_*` split domains
- Secrets → `CHANGEME_*` or removed
- Business dictionaries → generic L1–L3 / New–Closed themes

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

## Intentionally omitted

- Production credentials and session cookies
- One-off prod deploy/probe scripts with live IPs
- Real customer / employee datasets
- Private Git remotes

_Last reviewed: 2026-09-09._
