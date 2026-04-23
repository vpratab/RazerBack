param(
    [string]$PythonExe = "C:\fx_data\pipeline-venv\Scripts\python.exe",
    [string]$LiveRoot = "C:\fx_data\live"
)

$repoRoot = Split-Path -Parent $PSScriptRoot
$startupDir = Join-Path $env:APPDATA "Microsoft\Windows\Start Menu\Programs\Startup"

$liveScript = Join-Path $PSScriptRoot "live_trading_engine.py"
$tearSheetScript = Join-Path $PSScriptRoot "generate_daily_tear_sheet.py"
$investorScript = Join-Path $PSScriptRoot "generate_investor_report.py"
$healthScript = Join-Path $PSScriptRoot "health_check.py"

$liveCmd = "`"$PythonExe`" `"$liveScript`" --live-root `"$LiveRoot`""
$tearCmd = "`"$PythonExe`" `"$tearSheetScript`" --live-root `"$LiveRoot`""
$investorCmd = "`"$PythonExe`" `"$investorScript`" --live-root `"$LiveRoot`""
$healthCmd = "`"$PythonExe`" `"$healthScript`" --live-root `"$LiveRoot`""

schtasks /Create /F /TN "RazerBack Live Trading Engine Keeper" /SC MINUTE /MO 5 /TR $liveCmd | Out-Null
schtasks /Create /F /TN "RazerBack Daily Tear Sheet" /SC DAILY /ST 17:00 /TR $tearCmd | Out-Null
schtasks /Create /F /TN "RazerBack Weekly Investor Report" /SC WEEKLY /D SUN /ST 18:00 /TR $investorCmd | Out-Null
schtasks /Create /F /TN "RazerBack Live Health Check" /SC MINUTE /MO 15 /TR $healthCmd | Out-Null

New-Item -ItemType Directory -Force -Path $startupDir | Out-Null
$startupCmd = "@echo off`r`n$liveCmd`r`n"
Set-Content -Path (Join-Path $startupDir "RazerBackLiveEngine.cmd") -Value $startupCmd -Encoding ASCII

Write-Output "Registered production live tasks."
