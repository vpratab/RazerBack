param(
    [string]$PythonExe = "C:\fx_data\pipeline-venv\Scripts\python.exe",
    [string]$StartDate = "2011-01-01",
    [string]$EndDate = "2026-01-01",
    [int]$Workers = 8
)

$repoRoot = Split-Path -Parent $PSScriptRoot
$scriptPath = Join-Path $PSScriptRoot "download_dukascopy_ticks.py"
$logDir = "C:\fx_data\logs"
$stdoutLog = Join-Path $logDir "full_tick_download.stdout.log"
$stderrLog = Join-Path $logDir "full_tick_download.stderr.log"

New-Item -ItemType Directory -Force -Path "C:\fx_data\tick", "C:\fx_data\m1", "C:\fx_data\models", $logDir | Out-Null

Start-Process `
    -FilePath $PythonExe `
    -WorkingDirectory $repoRoot `
    -ArgumentList @($scriptPath, "--start-date", $StartDate, "--end-date", $EndDate, "--workers", $Workers) `
    -RedirectStandardOutput $stdoutLog `
    -RedirectStandardError $stderrLog `
    -WindowStyle Hidden
