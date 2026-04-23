param(
    [string]$PythonExe = "C:\fx_data\pipeline-venv\Scripts\python.exe"
)

$repoRoot = Split-Path -Parent $PSScriptRoot
$aggregateScript = Join-Path $PSScriptRoot "aggregate_ticks_to_m1.py"
$downloadLauncher = Join-Path $PSScriptRoot "start_full_tick_download.ps1"

$aggregateCmd = "`"$PythonExe`" `"$aggregateScript`""
$downloadCmd = "powershell.exe -ExecutionPolicy Bypass -File `"$downloadLauncher`" -PythonExe `"$PythonExe`""

schtasks /Create /F /TN "RazerBack FX Aggregate TickToM1" /SC DAILY /ST 03:00 /TR $aggregateCmd | Out-Null
schtasks /Create /F /TN "RazerBack FX Resume Tick Download" /SC ONLOGON /TR $downloadCmd | Out-Null

Write-Output "Registered scheduled tasks:"
Write-Output " - RazerBack FX Aggregate TickToM1"
Write-Output " - RazerBack FX Resume Tick Download"
