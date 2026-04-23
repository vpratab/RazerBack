param(
    [string]$PythonExe = "C:\fx_data\pipeline-venv\Scripts\python.exe"
)

$repoRoot = Split-Path -Parent $PSScriptRoot
$pairs = @("EURUSD","GBPUSD","USDJPY","AUDUSD","USDCAD","USDCHF","EURJPY","EURGBP","EURCHF","AUDJPY","GBPJPY")
$aggregateScript = Join-Path $PSScriptRoot "aggregate_ticks_to_m1.py"
$watchdogScript = Join-Path $PSScriptRoot "master_pipeline_watchdog.ps1"
$foreverScript = Join-Path $PSScriptRoot "run_downloader_forever.ps1"
$startupDir = Join-Path $env:APPDATA "Microsoft\Windows\Start Menu\Programs\Startup"

$aggregateCmd = "`"$PythonExe`" `"$aggregateScript`""
schtasks /Create /F /TN "RazerBack FX Aggregate TickToM1 Hourly" /SC HOURLY /MO 1 /TR $aggregateCmd | Out-Null

foreach ($pair in $pairs) {
    $downloadCmd = "powershell.exe -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$foreverScript`" -Pair $pair -PythonExe `"$PythonExe`""
    schtasks /Create /F /TN "RazerBack Download $pair Keeper" /SC MINUTE /MO 15 /TR $downloadCmd | Out-Null
}

$watchdogCmd = "powershell.exe -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$watchdogScript`""
schtasks /Create /F /TN "RazerBack Master Pipeline Watchdog Keeper" /SC MINUTE /MO 5 /TR $watchdogCmd | Out-Null

New-Item -ItemType Directory -Force -Path $startupDir | Out-Null
$startupCmd = "@echo off`r`npowershell.exe -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$watchdogScript`"`r`n"
Set-Content -Path (Join-Path $startupDir "RazerBackMasterPipeline.cmd") -Value $startupCmd -Encoding ASCII

Write-Output "Registered nonstop pipeline tasks."
