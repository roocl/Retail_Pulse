param([string]$BaseUrl = 'http://127.0.0.1:8080')
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'flink-test-support.ps1')
$projectRoot = Split-Path -Parent $PSScriptRoot
$output = "$projectRoot/query-api/target/stage6-smoke"
$compose = @('-f', "$projectRoot/infra/docker-compose.yml", '-f', "$projectRoot/infra/flink-compose.yml", '-f', "$projectRoot/infra/clickhouse-compose.yml")
$dataset = "stage6-$([Guid]::NewGuid().ToString('N'))"
New-Item -ItemType Directory -Force $output | Out-Null
& (Join-Path $PSScriptRoot 'stage4-smoke.ps1') -Dataset $dataset -WindowStart 1768471200000L -OutputDirectory "$output/pipeline"
$latest = Invoke-RestMethod "$BaseUrl/api/metrics/latest?dataset=$dataset"
if ($latest.gmv -cne '5.00' -or [DateTimeOffset]$latest.windowStart -ne [DateTimeOffset]'2026-01-15T10:01:00Z' -or $latest.paidOrders -cne '1') { throw 'Latest API differs from fixture.' }
$range = Invoke-RestMethod "$BaseUrl/api/metrics/range?dataset=$dataset&from=2026-01-15T10:00:00Z&to=2026-01-15T10:02:00Z"
if ($range.items.Count -ne 2 -or $range.items[0].gmv -cne '70.00' -or $range.items[0].refundAmount -cne '2.30') { throw 'Range API differs from fixture.' }
$top = Invoke-RestMethod "$BaseUrl/api/metrics/top-products?dataset=$dataset&windowStart=2026-01-15T10:00:00Z&resultVersion=1&limit=2"
if (!$top.ready -or ($top.products.productId -join ',') -ne 'p2,p1' -or $top.products[0].gmv -cne '30.00') { throw 'Ranking API differs from fixture.' }
$stopped = $false
try {
    Invoke-Docker -Arguments (@('compose') + $compose + @('stop', 'clickhouse')) | Out-Null
    $stopped = $true
    foreach ($path in @('/api/metrics/latest', '/actuator/health')) {
        $response = Invoke-WebRequest "$BaseUrl$path" -SkipHttpErrorCheck -TimeoutSec 20
        if ($response.StatusCode -ne 503) { throw "Expected 503 for $path during outage." }
        $response.Content | Set-Content "$output/$(if ($path -like '*health') {'health-down'} else {'api-down'}).json"
    }
    if ((Invoke-WebRequest "$BaseUrl/index.html").StatusCode -ne 200) { throw 'Dashboard unavailable during database outage.' }
} finally {
    if ($stopped) { Invoke-Docker -Arguments (@('compose') + $compose + @('start', 'clickhouse')) | Out-Null }
}
$deadline = [DateTime]::UtcNow.AddSeconds(45)
do {
    $health = Invoke-WebRequest "$BaseUrl/actuator/health" -SkipHttpErrorCheck -TimeoutSec 20
    if ($health.StatusCode -eq 200) { break }
    Start-Sleep -Seconds 1
} while ([DateTime]::UtcNow -lt $deadline)
if ($health.StatusCode -ne 200) { throw 'Database health did not recover.' }
$recovered = Invoke-RestMethod "$BaseUrl/api/metrics/latest?dataset=$dataset"
if ($recovered.gmv -cne '5.00') { throw 'Query did not recover after database restart.' }
@{dataset=$dataset; latest=$latest; range=$range; ranking=$top; healthAfterRecovery=$health.StatusCode} | ConvertTo-Json -Depth 12 | Set-Content "$output/results.json"
"PASS: Kafka -> Flink -> ClickHouse -> HTTP; exact values, database outage 503, static page available, recovery 200; dataset=$dataset" | Tee-Object -FilePath "$output/summary.txt"
