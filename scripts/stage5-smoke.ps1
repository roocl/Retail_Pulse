$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'flink-test-support.ps1')
$projectRoot = Split-Path -Parent $PSScriptRoot
$composeFiles = @('-f', "$projectRoot/infra/docker-compose.yml", '-f', "$projectRoot/infra/flink-compose.yml", '-f', "$projectRoot/infra/clickhouse-compose.yml")
$outputDirectory = "$projectRoot/analytics-job/target/stage5-smoke"
$dataset = "stage5-$([Guid]::NewGuid().ToString('N'))"
$windowStart = 1768471200000L
New-Item -ItemType Directory -Force $outputDirectory | Out-Null
foreach ($run in 1..2) {
    & (Join-Path $PSScriptRoot 'stage4-smoke.ps1') -Dataset $dataset -WindowStart $windowStart -OutputDirectory "$outputDirectory/run-$run" -InterruptStorage:($run -eq 1)
    $sql = "SELECT paid_orders, paid_users, paid_quantity, gmv, refunds, refund_amount, toUnixTimestamp64Milli(window_start) AS start FROM retailpulse.minute_metrics WHERE dataset = '$dataset' AND window_start >= fromUnixTimestamp64Milli($windowStart) ORDER BY window_start FORMAT JSONEachRow"
    $rows = $sql | & docker compose @composeFiles exec -T clickhouse sh -c 'exec clickhouse-client --user retailpulse --password "$CLICKHOUSE_PASSWORD"'
    if ($LASTEXITCODE -ne 0) { throw 'Minute query failed.' }
    $minutes = @($rows | ConvertFrom-Json)
    if ($minutes.Count -ne 2 -or $minutes[0].paid_orders -ne 4 -or $minutes[0].paid_users -ne 3 -or $minutes[0].paid_quantity -ne 5 -or
        $minutes[0].gmv -ne 70 -or $minutes[0].refunds -ne 1 -or $minutes[0].refund_amount -ne 2.30 -or $minutes[0].start -ne $windowStart -or
        $minutes[1].gmv -ne 5) { throw 'Stored minute snapshots differ from expected values.' }
    $sql = "SELECT rank, product_id, gmv FROM retailpulse.product_top_n WHERE dataset = '$dataset' AND window_start = fromUnixTimestamp64Milli($windowStart) ORDER BY rank FORMAT JSONEachRow"
    $rows = $sql | & docker compose @composeFiles exec -T clickhouse sh -c 'exec clickhouse-client --user retailpulse --password "$CLICKHOUSE_PASSWORD"'
    if ($LASTEXITCODE -ne 0) { throw 'Ranking query failed.' }
    $ranking = @($rows | ConvertFrom-Json)
    if ($ranking.Count -ne 2 -or ($ranking.product_id -join ',') -ne 'p2,p1' -or $ranking[0].gmv -ne 30 -or $ranking[1].gmv -ne 20) { throw 'Stored ranking differs from expected values.' }
    @{dataset=$dataset; replay=$run; minutes=$minutes; ranking=$ranking} | ConvertTo-Json -Depth 10 | Set-Content "$outputDirectory/stored-$run.json"
}
"PASS: Kafka -> Flink -> ClickHouse; two independent jobs replayed the same windows; exact minutes and ranking converge." | Tee-Object -FilePath "$outputDirectory/summary.txt"
