$ErrorActionPreference = "Stop"
$Root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $Root

if (Test-Path .env) {
  Get-Content .env | ForEach-Object {
    if ($_ -match '^\s*#' -or $_ -match '^\s*$') { return }
    $parts = $_ -split '=', 2
    if ($parts.Count -eq 2) {
      [Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1].Trim(), "Process")
    }
  }
}

$chUser = if ($env:CLICKHOUSE_USER) { $env:CLICKHOUSE_USER } else { "default" }
$chPass = if ($env:CLICKHOUSE_PASSWORD) { $env:CLICKHOUSE_PASSWORD } else { "gel" }

Write-Host "==> docker compose up"
docker compose up -d clickhouse minio minio_init postgres

Write-Host "==> wait ClickHouse"
$ready = $false
$i = 0
while ((-not $ready) -and ($i -lt 60)) {
  try {
    $null = Invoke-WebRequest -Uri "http://localhost:8123/ping" -UseBasicParsing -TimeoutSec 2
    $ready = $true
  } catch {
    Start-Sleep -Seconds 1
  }
  $i++
}
if (-not $ready) {
  throw "ClickHouse did not become ready"
}

Write-Host "==> generate + load fixtures (DDL + seed via Python)"
$env:CLICKHOUSE_USER = $chUser
$env:CLICKHOUSE_PASSWORD = $chPass
python scripts\generate_fixtures.py
if ($LASTEXITCODE -ne 0) {
  throw "generate_fixtures failed"
}

Write-Host "==> smoke counts"
$pair = $chUser + ":" + $chPass
$t = & curl.exe -sS -u $pair "http://localhost:8123/" --data "SELECT count() FROM gel_helpdesk.demo_tickets"
$s = & curl.exe -sS -u $pair "http://localhost:8123/" --data "SELECT count() FROM gel_bot.demo_bot_sessions"
Write-Host ("tickets=" + $t)
Write-Host ("sessions=" + $s)
if ([int]$t -lt 1 -or [int]$s -lt 1) {
  throw "smoke counts too low"
}
Write-Host "OK - Gelassenheit ELT demo data is up"
