$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'flink-test-support.ps1')
$projectRoot = Split-Path -Parent $PSScriptRoot
$outputDirectory = Join-Path $projectRoot 'analytics-job/target/stage4-smoke'
New-Item -ItemType Directory -Force -Path $outputDirectory | Out-Null
$composeFiles = @('-f', (Join-Path $projectRoot 'infra/docker-compose.yml'), '-f', (Join-Path $projectRoot 'infra/flink-compose.yml'))
$runId = [Guid]::NewGuid().ToString('N')
$inputTopic = "stage4-input-$runId"
$deadTopic = "stage4-dead-$runId"
$jobId = $null
$createdTopics = @()
$windowStart = 1768471200000L + ([long]([Convert]::ToUInt32($runId.Substring(0, 8), 16) % 1000000) * 60000L)
$startedAt = [DateTime]::UtcNow.ToString('o')

function New-Event {
    param([string]$Id, [string]$Order, [string]$User, [string]$Product, [string]$Type, [decimal]$Amount, [int]$Quantity, [long]$Offset)
    [ordered]@{ schemaVersion=2; eventId="$runId-$Id"; orderId=$(if ($Order) {"$runId-$Order"} else {$null});
        userId=$User; productId=$Product; eventType=$Type; amount=$Amount; quantity=$Quantity;
        eventTime=[DateTimeOffset]::FromUnixTimeMilliseconds($windowStart + $Offset).ToString('yyyy-MM-ddTHH:mm:ss.fffZ') } | ConvertTo-Json -Compress
}

function Send-Events {
    param([string[]]$Events)
    $Events | ForEach-Object { "fixture|$_" } | & docker exec -i retailpulse-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:29092 --topic $inputTopic --property parse.key=true --property 'key.separator=|'
    if ($LASTEXITCODE -ne 0) { throw 'Fixture production failed.' }
}

try {
    $deadline = [DateTime]::UtcNow.AddSeconds(60)
    do {
        try { $overview = Read-JobApi '/overview' } catch { $overview = $null }
        if ($overview -and $overview.'slots-total' -ge 2) { break }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $deadline)
    if (!$overview -or $overview.'slots-total' -lt 2) { throw 'Flink needs two registered slots.' }
    foreach ($topic in @($inputTopic, $deadTopic)) {
        Invoke-Docker -Arguments @('exec', 'retailpulse-kafka', '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'kafka:29092',
            '--create', '--topic', $topic, '--partitions', '3', '--replication-factor', '1') | Out-Null
        $createdTopics += $topic
    }
    $submission = Invoke-Docker -Arguments (@('compose') + $composeFiles + @('exec', '-T', 'jobmanager', 'flink', 'run', '-d',
        '/opt/flink/usrlib/analytics-job.jar', '--bootstrap-servers', 'kafka:29092', '--input-topic', $inputTopic,
        '--dead-letter-topic', $deadTopic, '--group-id', "stage4-$runId", '--parallelism', '2', '--top-n', '2',
        '--out-of-order-ms', '30000', '--idle-timeout-ms', '1500', '--checkpoint-interval-ms', '1000'))
    $submission | Set-Content (Join-Path $outputDirectory 'submission.log') -Encoding UTF8
    if (($submission -join "`n") -notmatch 'JobID ([a-f0-9]{32})') { throw 'No submitted JobID.' }
    $jobId = $Matches[1]
    $a = New-Event a o1 u1 p1 PAYMENT_COMPLETED 12.30 1 0
    $fixtures = @($a, (New-Event b o2 u2 p2 PAYMENT_COMPLETED 30.00 1 59999), $a,
        (New-Event c o3 u1 p1 PAYMENT_COMPLETED 7.70 2 10000),
        (New-Event r o1 u1 p1 REFUND_COMPLETED 2.30 1 50000),
        (New-Event d o4 u3 p3 PAYMENT_COMPLETED 20.00 1 20000),
        (New-Event next o5 u1 p1 PAYMENT_COMPLETED 5.00 1 60000), 'not-json',
        (New-Event advance '' u1 p1 PRODUCT_CLICK 0 1 152000))
    $fixtures | Set-Content (Join-Path $outputDirectory 'input.jsonl') -Encoding UTF8
    Send-Events $fixtures
    $deadline = [DateTime]::UtcNow.AddSeconds(90)
    do {
        $job = Read-JobApi "/jobs/$jobId"
        if ($job.state -in @('FAILED','CANCELED','FINISHED')) { throw "Unexpected job state: $($job.state)" }
        $taskLog = Invoke-Docker -Arguments (@('compose') + $composeFiles + @('logs', '--since', $startedAt, '--no-color', 'taskmanager'))
        $minutes = @($taskLog | ForEach-Object { if ("$_" -match 'minute-metrics(:\d+)?>\s*(\{.*\})') {$Matches[2] | ConvertFrom-Json} } | Where-Object {$_.windowStart -in @($windowStart, ($windowStart+60000))})
        $rankings = @($taskLog | ForEach-Object { if ("$_" -match 'product-top-n(:\d+)?>\s*(\{.*\})') {$Matches[2] | ConvertFrom-Json} } | Where-Object {$_.windowStart -in @($windowStart, ($windowStart+60000))})
        if ($minutes.Count -ge 2 -and $rankings.Count -ge 2) { break }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $deadline)
    if ($minutes.Count -ne 2 -or $rankings.Count -ne 2) { throw 'Expected exactly two minutes and two rankings.' }
    $first = @($minutes | Where-Object windowStart -EQ $windowStart)
    $second = @($minutes | Where-Object windowStart -EQ ($windowStart+60000))
    if ($first.Count -ne 1 -or $first[0].paidOrders -ne 4 -or $first[0].paidUsers -ne 3 -or $first[0].paidQuantity -ne 5 -or
        $first[0].gmv -ne 70.00 -or $first[0].refunds -ne 1 -or $first[0].refundAmount -ne 2.30 -or $first[0].windowEnd -ne ($windowStart+60000)) { throw 'First minute differs from hand-calculated totals.' }
    if ($second.Count -ne 1 -or $second[0].paidOrders -ne 1 -or $second[0].gmv -ne 5.00 -or $second[0].refunds -ne 0) { throw 'Minute boundary is incorrect.' }
    $top = @($rankings | Where-Object windowStart -EQ $windowStart)
    if ($top.Count -ne 1 -or ($top[0].products.productId -join ',') -ne 'p2,p1' -or
        $top[0].products[0].gmv -ne 30.00 -or $top[0].products[1].gmv -ne 20.00) { throw 'TopN ordering, tie-break or product totals are incorrect.' }
    $late = New-Event late o6 u4 p4 PAYMENT_COMPLETED 999.00 1 15000
    Send-Events @($late, $a)
    $sentAfter = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    $deadline = [DateTime]::UtcNow.AddSeconds(60)
    do {
        $taskLog = Invoke-Docker -Arguments (@('compose') + $composeFiles + @('logs', '--since', $startedAt, '--no-color', 'taskmanager'))
        $lateEvents = @($taskLog | ForEach-Object { if ("$_" -match 'late-event(:\d+)?>\s*(\{.*\})') {$Matches[2] | ConvertFrom-Json} } | Where-Object {$_.eventId -in @("$runId-late", "$runId-a")})
        $checkpoints = Read-JobApi "/jobs/$jobId/checkpoints"
        if ($lateEvents.Count -ge 2 -and $checkpoints.latest.completed.trigger_timestamp -ge $sentAfter) { break }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $deadline)
    if ($lateEvents.Count -ne 2 -or @($lateEvents.eventId | Sort-Object -Unique).Count -ne 2) { throw 'Late event or post-cleanup duplicate was not routed.' }
    $resultLines = @($taskLog | Where-Object {"$_" -match '(minute-metrics|product-top-n)(:\d+)?>'})
    if ($resultLines.Count -ne 4) { throw 'Late input changed final window outputs.' }
    if ($checkpoints.latest.completed.trigger_timestamp -lt $sentAfter) { throw 'No completed checkpoint after late input.' }
    $deadLog = Invoke-Docker -Arguments @('exec', 'retailpulse-kafka', '/opt/kafka/bin/kafka-console-consumer.sh', '--bootstrap-server', 'kafka:29092',
        '--topic', $deadTopic, '--from-beginning', '--max-messages', '1', '--timeout-ms', '10000')
    $dead = @($deadLog | Where-Object {"$_" -match '^\{'} | ForEach-Object {"$_" | ConvertFrom-Json})
    if ($dead.Count -ne 1 -or $dead[0].sourceTopic -ne $inputTopic -or $dead[0].sourceOffset -ne 7 -or
        [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($dead[0].valueBase64)) -ne 'not-json') { throw 'Dead-letter metadata or payload is incorrect.' }
    $minutes | ConvertTo-Json -Depth 10 | Set-Content (Join-Path $outputDirectory 'minutes.json') -Encoding UTF8
    $rankings | ConvertTo-Json -Depth 10 | Set-Content (Join-Path $outputDirectory 'rankings.json') -Encoding UTF8
    $checkpoints | ConvertTo-Json -Depth 20 | Set-Content (Join-Path $outputDirectory 'checkpoints.json') -Encoding UTF8
    $summary = @("job=$jobId parallelism=2 source-partitions=3", 'minute-1: paid-orders=4 paid-users=3 paid-quantity=5 GMV=70.00 refunds=1 refund-amount=2.30',
        'minute-2: paid-orders=1 GMV=5.00', 'top-2: p2=30.00,p1=20.00; tie with p3 resolved by productId',
        "late=2 dead-letter=1 final-results=4 checkpoint=$($checkpoints.latest.completed.id)")
    $summary | Set-Content (Join-Path $outputDirectory 'summary.txt') -Encoding UTF8
    $summary
} finally {
    if ($jobId) {
        Invoke-Docker -Arguments (@('compose') + $composeFiles + @('logs', '--since', $startedAt, '--no-color', 'taskmanager')) | Set-Content (Join-Path $outputDirectory 'taskmanager.log') -Encoding UTF8
        Read-JobApi "/jobs/$jobId/exceptions" | ConvertTo-Json -Depth 20 | Set-Content (Join-Path $outputDirectory 'exceptions.json') -Encoding UTF8
        Invoke-Docker -Arguments (@('compose') + $composeFiles + @('exec', '-T', 'jobmanager', 'flink', 'cancel', $jobId)) | Out-Null
    }
    foreach ($topic in $createdTopics) {
        Invoke-Docker -Arguments @('exec', 'retailpulse-kafka', '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'kafka:29092', '--delete', '--topic', $topic) | Out-Null
    }
}
