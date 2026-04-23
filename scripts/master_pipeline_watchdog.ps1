$repoRoot = Split-Path -Parent $PSScriptRoot
$pythonExe = "C:\fx_data\pipeline-venv\Scripts\python.exe"
$statusScript = Join-Path $PSScriptRoot "pipeline_status.py"
$aggregateScript = Join-Path $PSScriptRoot "aggregate_ticks_to_m1.py"
$reportScript = Join-Path $PSScriptRoot "generate_forensic_report.py"
$enrichScript = Join-Path $repoRoot "enrich_forex_research_data.py"
$runScript = Join-Path $repoRoot "run_locked_portfolio.py"
$configPath = Join-Path $repoRoot "configs\continuation_portfolio_total_v1.json"
$logDir = "C:\fx_data\logs"
$pairs = @("EURUSD","GBPUSD","USDJPY","AUDUSD","USDCAD","USDCHF","EURJPY","EURGBP","EURCHF","AUDJPY","GBPJPY")
$lowerPairs = $pairs | ForEach-Object { $_.ToLower() }
$scenarios = @("base", "conservative", "hard")

New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$watchdogLog = Join-Path $logDir "master_pipeline_watchdog.log"

$existingWatchdog = Get-CimInstance Win32_Process |
    Where-Object {
        $_.ProcessId -ne $PID `
        -and $_.Name -like "*powershell*" `
        -and $_.CommandLine -like "*master_pipeline_watchdog.ps1*"
    } |
    Select-Object -First 1

if ($existingWatchdog) {
    exit 0
}

function Write-WatchdogLog {
    param([string]$Message)
    $line = "$(Get-Date -Format o) $Message"
    $line | Tee-Object -FilePath $watchdogLog -Append | Out-Null
}

function Test-CommandLineRunning {
    param([string]$Needle)
    $matches = Get-CimInstance Win32_Process |
        Where-Object { $_.Name -like "python*" -and $_.CommandLine -like "*$Needle*" }
    return [bool]($matches | Select-Object -First 1)
}

function Invoke-LoggedPython {
    param(
        [string]$Name,
        [string[]]$Arguments
    )
    $stdoutLog = Join-Path $logDir "$Name.stdout.log"
    $stderrLog = Join-Path $logDir "$Name.stderr.log"
    Write-WatchdogLog "Starting $Name"
    Push-Location $repoRoot
    try {
        & $pythonExe @Arguments 1>> $stdoutLog 2>> $stderrLog
        $exitCode = $LASTEXITCODE
    }
    finally {
        Pop-Location
    }
    Write-WatchdogLog "$Name exited with code $exitCode"
    if ($exitCode -ne 0) {
        throw "$Name failed with exit code $exitCode"
    }
}

function Get-PipelineStatus {
    $statusJson = & $pythonExe $statusScript --pairs $pairs --repo-root $repoRoot
    return $statusJson | ConvertFrom-Json
}

while ($true) {
    $status = Get-PipelineStatus
    Write-WatchdogLog "Phase=$($status.phase) download_complete=$($status.download_complete) m1_complete=$($status.m1_complete) enrichment_complete=$($status.enrichment_complete) scenarios_complete=$($status.scenarios_complete) report_complete=$($status.report_complete)"

    try {
        if (-not (Test-CommandLineRunning "aggregate_ticks_to_m1.py")) {
            Invoke-LoggedPython -Name "aggregate_watchdog" -Arguments @($aggregateScript)
        }

        if ($status.download_complete -and $status.m1_complete -and -not $status.enrichment_complete -and -not (Test-CommandLineRunning "enrich_forex_research_data.py")) {
            Invoke-LoggedPython -Name "enrich_watchdog" -Arguments @(
                $enrichScript,
                "--data-dir", "C:\fx_data\m1",
                "--instruments"
            ) + $lowerPairs
        }

        $status = Get-PipelineStatus
        if ($status.download_complete -and $status.enrichment_complete -and -not $status.scenarios_complete -and -not (Test-CommandLineRunning "run_locked_portfolio.py")) {
            foreach ($scenario in $scenarios) {
                $scenarioOutput = Join-Path $repoRoot "output\full_15yr_$scenario"
                if (-not (Test-Path (Join-Path $scenarioOutput "summary.csv"))) {
                    Invoke-LoggedPython -Name "run_$scenario" -Arguments @(
                        $runScript,
                        "--config", $configPath,
                        "--data-dir", "C:\fx_data\m1",
                        "--output-dir", $scenarioOutput,
                        "--scenario", $scenario
                    )
                }
            }
        }

        $status = Get-PipelineStatus
        if ($status.scenarios_complete -and -not $status.report_complete -and -not (Test-CommandLineRunning "generate_forensic_report.py")) {
            foreach ($scenario in $scenarios) {
                $scenarioOutput = Join-Path $repoRoot "output\full_15yr_$scenario"
                $otherScenarioDirs = $scenarios | Where-Object { $_ -ne $scenario } | ForEach-Object { Join-Path $repoRoot "output\full_15yr_$_" }
                Invoke-LoggedPython -Name "report_$scenario" -Arguments @(
                    $reportScript,
                    "--output-dir", $scenarioOutput,
                    "--scenario-dir", $otherScenarioDirs[0],
                    "--scenario-dir", $otherScenarioDirs[1]
                )
            }
        }

        $status = Get-PipelineStatus
        if ($status.phase -eq "complete") {
            Write-WatchdogLog "Pipeline fully complete."
            break
        }
    }
    catch {
        Write-WatchdogLog "Watchdog caught error: $($_.Exception.Message)"
    }

    Start-Sleep -Seconds 300
}
