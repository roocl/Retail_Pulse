$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/stack-support.ps1"
$jobs = Read-JobApi '/jobs/overview'
if (@($jobs.jobs | Where-Object state -NotIn @('FAILED','CANCELED','FINISHED')).Count) { throw 'Clean-start test requires an idle cluster.' }
$id = [Guid]::NewGuid().ToString('N')
$output = "$stackRoot/target/stage7-clean-start/$id"
New-Item -ItemType Directory -Force $output | Out-Null
$originalCompose = $stackCompose
$cleanCompose = $stackCompose.Clone()
$cleanCompose[[Array]::IndexOf($cleanCompose, '-p') + 1] = "retailpulse-clean-$id"
$cleanStarted = $false
try {
    Invoke-Stack @('down') | Out-Null
    $stackCompose = $cleanCompose
    $cleanStarted = $true
    Invoke-Stack @('up','-d','--wait','--wait-timeout','180') | Set-Content "$output/startup.txt"
    $settings = @{'bootstrap-servers'='kafka:29092'; 'input-topic'='commerce-events'; 'dead-letter-topic'='commerce-events-dead-letter';
        'group-id'="clean-$id"; dataset="clean-$id"; 'result-version'=1; parallelism=1}
    $empty = Invoke-WebRequest "http://127.0.0.1:8080/api/metrics/latest?dataset=$($settings.dataset)" -SkipHttpErrorCheck
    if ($empty.StatusCode -ne 204) { throw 'Fresh dataset was not empty.' }
    $jobId = Submit-StackJob $settings
    Send-StackEvents $settings['input-topic'] (Get-Content "$PSScriptRoot/fixtures/stage7-startup.jsonl")
    $metric = Wait-StackMetric $settings.dataset 1768471200000L '25.00' 1
    $checkpoint = Wait-StackCheckpoint $jobId
    $checkpoint | ConvertTo-Json -Depth 20 | Set-Content "$output/checkpoints.json"
    Invoke-Stack @('exec','-T','jobmanager','flink','cancel',$jobId) | Out-Null
    $result = @{project="retailpulse-clean-$id";dataset=$settings.dataset;jobId=$jobId;initialStatus=$empty.StatusCode;
        metric=$metric;fixtureSha256=(Get-FileHash "$PSScriptRoot/fixtures/stage7-startup.jsonl").Hash;
        volumes='New isolated Compose project volumes; removed after test.'}
} finally {
    if ($cleanStarted) { Invoke-Stack @('down','--volumes') | Out-Null }
    $stackCompose = $originalCompose
    Invoke-Stack @('up','-d','--wait','--wait-timeout','180') | Out-Null
}
$result | ConvertTo-Json -Depth 12 | Set-Content "$output/result.json"
Write-Output "PASS: clean volumes -> initialized schema -> Kafka -> Flink -> HTTP GMV=25.00; original volumes restored. Evidence: $output"
