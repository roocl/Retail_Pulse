param([switch]$SkipBuild)
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/stack-support.ps1"
$output = "$stackRoot/target/stage7-lifecycle/$([Guid]::NewGuid().ToString('N'))"
New-Item -ItemType Directory -Force $output | Out-Null
& "$PSScriptRoot/stack.ps1" up -SkipBuild:$SkipBuild | Set-Content "$output/up.txt"
$before = Get-Content "$stackRoot/checkpoints/stack.json" -Raw | ConvertFrom-Json
$deadline = [DateTime]::UtcNow.AddSeconds(60)
do {
    $response = Invoke-WebRequest "http://127.0.0.1:8080/api/metrics/latest?dataset=$($before.settings.dataset)" -SkipHttpErrorCheck
    if ($response.StatusCode -eq 200) { break }
    Start-Sleep -Seconds 1
} while ([DateTime]::UtcNow -lt $deadline)
if ($response.StatusCode -ne 200) { throw 'Demo producer did not produce a queryable metric.' }
$metric = $response.Content | ConvertFrom-Json
$response.Content | Set-Content "$output/before.json"
& "$PSScriptRoot/stack.ps1" down | Set-Content "$output/down.txt"
$stopped = Get-Content "$stackRoot/checkpoints/stack.json" -Raw | ConvertFrom-Json
if (!$stopped.savepoint) { throw 'Normal shutdown did not persist a savepoint.' }
& "$PSScriptRoot/stack.ps1" up -NoJob -SkipBuild | Set-Content "$output/restart.txt"
& "$PSScriptRoot/stack.ps1" recover | Set-Content "$output/recover.txt"
$after = Get-Content "$stackRoot/checkpoints/stack.json" -Raw | ConvertFrom-Json
if ($after.jobId -eq $before.jobId) { throw 'Recovery did not submit a new job.' }
$checkpoints = Wait-StackCheckpoint $after.jobId
if ($checkpoints.counts.restored -lt 1 -or !$checkpoints.latest.restored.is_savepoint) { throw 'Job was not restored from the stop savepoint.' }
$window = ([DateTimeOffset]$metric.windowStart).ToUnixTimeMilliseconds()
$restoredMetric = Wait-StackMetric $after.settings.dataset $window $metric.gmv ([long]$metric.paidOrders)
& "$PSScriptRoot/stack.ps1" up -SkipBuild | Set-Content "$output/repeated-up.txt"
$repeated = Get-Content "$stackRoot/checkpoints/stack.json" -Raw | ConvertFrom-Json
if ($repeated.jobId -ne $after.jobId) { throw 'Repeated startup created another job.' }
$checkpoints | ConvertTo-Json -Depth 20 | Set-Content "$output/checkpoints.json"
@{beforeJob=$before.jobId;restoredJob=$after.jobId;dataset=$after.settings.dataset;savepoint=$stopped.savepoint;
    metric=$restoredMetric;repeatedUpPreservedJob=$true} | ConvertTo-Json -Depth 10 | Set-Content "$output/result.json"
Write-Output "PASS: producer -> HTTP, stop savepoint, restored job and repeated startup; dataset=$($after.settings.dataset). Evidence: $output"
