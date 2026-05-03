param(
    [Parameter(Mandatory = $true)]
    [string]$Pair,
    [string]$PythonExe = "C:\fx_data\pipeline-venv\Scripts\python.exe",
    [int]$Workers = 2,
    [string]$StartDate = "2011-01-01",
    [string]$EndDate = "2026-04-24",
    [string]$RecentStartDate = "2020-01-01"
)

$repoRoot = Split-Path -Parent $PSScriptRoot
$downloadScript = Join-Path $PSScriptRoot "download_dukascopy_ticks.py"
$statusScript = Join-Path $PSScriptRoot "pipeline_status.py"
$logDir = "C:\fx_data\logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$existingLoop = Get-CimInstance Win32_Process |
    Where-Object {
        $_.ProcessId -ne $PID `
        -and $_.Name -like "*powershell*" `
        -and $_.CommandLine -like "*run_downloader_forever.ps1*" `
        -and $_.CommandLine -like "*-Pair $Pair*"
    } |
    Select-Object -First 1

if ($existingLoop) {
    Write-Output "Downloader loop for $Pair is already running in PID $($existingLoop.ProcessId)."
    exit 0
}

while ($true) {
    $startDateValue = [datetime]::ParseExact($StartDate, "yyyy-MM-dd", $null)
    $endDateValue = [datetime]::ParseExact($EndDate, "yyyy-MM-dd", $null)
    $recentStartValue = [datetime]::ParseExact($RecentStartDate, "yyyy-MM-dd", $null)

    $phases = @()
    $recentPhaseStart = if ($recentStartValue -gt $startDateValue) { $RecentStartDate } else { $StartDate }
    if ([datetime]::ParseExact($recentPhaseStart, "yyyy-MM-dd", $null) -le $endDateValue) {
        $phases += @{
            Name = "recent"
            StartDate = $recentPhaseStart
            EndDate = $EndDate
            DateOrder = "descending"
        }
    }

    $backfillEndValue = $recentStartValue.AddDays(-1)
    if ($startDateValue -le $backfillEndValue) {
        $phases += @{
            Name = "backfill"
            StartDate = $StartDate
            EndDate = $backfillEndValue.ToString("yyyy-MM-dd")
            DateOrder = "descending"
        }
    }

    if ($phases.Count -eq 0) {
        $phases += @{
            Name = "full"
            StartDate = $StartDate
            EndDate = $EndDate
            DateOrder = "descending"
        }
    }

    $phase = $null
    foreach ($candidate in $phases) {
        $statusJson = & $PythonExe $statusScript --pairs $Pair --start-date $candidate.StartDate --end-date $candidate.EndDate --repo-root $repoRoot
        $status = $statusJson | ConvertFrom-Json
        if (-not $status.download_complete) {
            $phase = $candidate
            break
        }
    }

    if (-not $phase) {
        Write-Output "$Pair download already complete for all phases between $StartDate and $EndDate"
        break
    }

    $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $stdoutLog = Join-Path $logDir "download_${Pair}_${stamp}.log"
    $stderrLog = Join-Path $logDir "download_${Pair}.stderr.log"
    "Starting $($phase.Name) download for $Pair [$($phase.StartDate) -> $($phase.EndDate)] at $(Get-Date -Format o)" | Tee-Object -FilePath $stdoutLog -Append | Out-Null

    $arguments = @(
        $downloadScript,
        "--pairs", $Pair,
        "--start-date", $phase.StartDate,
        "--end-date", $phase.EndDate,
        "--workers", $Workers,
        "--date-order", $phase.DateOrder
    )

    $proc = Start-Process `
        -FilePath $PythonExe `
        -ArgumentList $arguments `
        -WorkingDirectory $repoRoot `
        -NoNewWindow `
        -PassThru `
        -RedirectStandardOutput $stdoutLog `
        -RedirectStandardError $stderrLog

    $proc.WaitForExit()
    $exitCode = $proc.ExitCode
    "Download for $Pair phase $($phase.Name) exited with code $exitCode at $(Get-Date -Format o)" | Tee-Object -FilePath $stdoutLog -Append | Out-Null

    if ($exitCode -eq 0) {
        $statusJson = & $PythonExe $statusScript --pairs $Pair --start-date $phase.StartDate --end-date $phase.EndDate --repo-root $repoRoot
        $status = $statusJson | ConvertFrom-Json
        if ($status.download_complete) {
            "Download for $Pair phase $($phase.Name) completed successfully." | Tee-Object -FilePath $stdoutLog -Append | Out-Null
            continue
        }
    }

    "Restarting $Pair phase $($phase.Name) in 60 seconds..." | Tee-Object -FilePath $stdoutLog -Append | Out-Null
    Start-Sleep -Seconds 60
}
