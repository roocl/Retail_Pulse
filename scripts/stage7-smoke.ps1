param(
    [switch]$SkipBuild,
    [ValidateRange(1,2)][int]$Parallelism = 1,
    [ValidateRange(1000,60000)][int]$IdleTimeoutMs = 10000
)
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/stack-support.ps1"
& "$PSScriptRoot/stack.ps1" up -NoJob -SkipBuild:$SkipBuild
$health = Invoke-RestMethod 'http://127.0.0.1:8080/actuator/health'
if ($health.status -ne 'UP') { throw 'Query service is not healthy.' }
$overview = Read-JobApi '/overview'
if ($overview.'slots-total' -lt 2) { throw 'Flink workers are not registered.' }
$existing = Read-JobApi '/jobs/overview'
if (@($existing.jobs | Where-Object state -NotIn @('FAILED','CANCELED','FINISHED')).Count) { throw 'Recovery test requires an idle cluster.' }
$id = [Guid]::NewGuid().ToString('N')
$settings = @{'bootstrap-servers'='kafka:29092'; 'input-topic'="stage7-$id"; 'dead-letter-topic'="stage7-dead-$id";
    'group-id'="stage7-$id"; dataset="stage7-$id"; 'result-version'=1; parallelism=$Parallelism; 'checkpoint-interval-ms'=1000;
    'idle-timeout-ms'=$IdleTimeoutMs; 'out-of-order-ms'=30000; 'top-n'=2; 'jdbc-max-retries'=10}
$output = "$stackRoot/target/stage7-smoke/$id"
New-Item -ItemType Directory -Force $output | Out-Null
$settings | ConvertTo-Json | Set-Content "$output/settings.json"
$created = @()
$jobId = $null
$stopped = $null
$sent = 0L
$results = @()
try {
    foreach ($topic in @($settings['input-topic'], $settings['dead-letter-topic'])) {
        Invoke-Stack @('exec','-T','kafka','/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','kafka:29092',
            '--create','--topic',$topic,'--partitions','3','--replication-factor','1') | Out-Null
        $created += $topic
    }
    $jobId = Submit-StackJob $settings
    $index = 0
    foreach ($service in @('taskmanager','kafka','clickhouse','jobmanager')) {
        $window = 1768471200000L + $index * 180000L
        $events = foreach ($entry in @(@('a','p1','12.30',0),@('b','p1','7.70',10000),@('c','p2','30.00',59999))) {
            [ordered]@{schemaVersion=2;eventId="$id-$index-$($entry[0])";orderId="$id-$index-$($entry[0])";
                userId='u1';productId=$entry[1];eventType='PAYMENT_COMPLETED';amount=[decimal]$entry[2];quantity=1;
                eventTime=[DateTimeOffset]::FromUnixTimeMilliseconds($window + [long]$entry[3]).ToString('yyyy-MM-ddTHH:mm:ss.fffZ')} | ConvertTo-Json -Compress
        }
        $marker = [ordered]@{schemaVersion=2;eventId="$id-$index-marker";orderId=$null;userId='u1';productId='p1';
            eventType='PRODUCT_CLICK';amount=0;quantity=1;eventTime=[DateTimeOffset]::FromUnixTimeMilliseconds($window+92000).ToString('yyyy-MM-ddTHH:mm:ss.fffZ')} | ConvertTo-Json -Compress
        @($events[0],$events[1],$events[0],$events[0],$events[2],$marker) | Set-Content "$output/$service-input.jsonl"
        Send-StackEvents $settings['input-topic'] @($events[0],$events[1],$events[0])
        $sent += 3
        $before = Wait-StackInput $jobId $settings['group-id'] $sent
        $before | ConvertTo-Json -Depth 30 | Set-Content "$output/$service-before.json"
        $started = [DateTimeOffset]::UtcNow
        if ($service -eq 'clickhouse') {
            Invoke-Stack @('stop',$service) | Out-Null
            $stopped = $service
            Send-StackEvents $settings['input-topic'] @($events[0],$events[2],$marker)
            $down = Invoke-RestMethod 'http://127.0.0.1:8080/api/metrics/latest' -SkipHttpErrorCheck -StatusCodeVariable downStatus -TimeoutSec 15
            if ($downStatus -ne 503) { throw 'Expected query 503 during storage outage.' }
            if ((Invoke-WebRequest 'http://127.0.0.1:8080/index.html').StatusCode -ne 200) { throw 'Static dashboard unavailable.' }
            $down | ConvertTo-Json -Depth 10 | Set-Content "$output/storage-down.json"
            Invoke-Stack @('start',$service) | Out-Null
            $stopped = $null
        } else {
            Invoke-Stack @('restart',$service) | Out-Null
            Invoke-Stack @('up','-d','--wait','--wait-timeout','120') | Out-Null
            if ($service -eq 'jobmanager') {
                $restorePath = Find-StackCheckpoint $jobId
                $restorePath | Set-Content "$output/jobmanager-restore-path.txt"
                $jobId = Submit-StackJob $settings $restorePath
            } else {
                Wait-StackJob $jobId | Out-Null
            }
            Send-StackEvents $settings['input-topic'] @($events[0],$events[2],$marker)
        }
        $sent += 3
        $metric = Wait-StackMetric $settings.dataset $window '50.00' 3
        $from = [DateTimeOffset]::FromUnixTimeMilliseconds($window).ToString('yyyy-MM-ddTHH:mm:ssZ')
        $rankingDeadline = [DateTime]::UtcNow.AddSeconds(60)
        do {
            $top = Invoke-RestMethod "http://127.0.0.1:8080/api/metrics/top-products?dataset=$($settings.dataset)&windowStart=$from&resultVersion=1&limit=2"
            if ($top.ready) { break }
            Start-Sleep -Milliseconds 200
        } while ([DateTime]::UtcNow -lt $rankingDeadline)
        if (!$top.ready -or ($top.products.productId -join ',') -ne 'p2,p1' -or $top.products[0].gmv -cne '30.00' -or $top.products[1].gmv -cne '20.00') { throw "Incorrect ranking after $service restart." }
        $after = Wait-StackInput $jobId $settings['group-id'] $sent
        if ($service -in @('taskmanager','jobmanager') -and $after.counts.restored -lt 1) { throw "No state restoration recorded after $service restart." }
        $after | ConvertTo-Json -Depth 30 | Set-Content "$output/$service-after.json"
        $results += @{service=$service;jobId=$jobId;metric=$metric;ranking=$top;restored=$after.counts.restored;
            restartToVerifiedMs=([DateTimeOffset]::UtcNow-$started).TotalMilliseconds}
        $results | ConvertTo-Json -Depth 20 | Set-Content "$output/results.json"
        Write-Output "PASS: $service recovery, GMV=50.00, orders=3, TopN=p2:30.00,p1:20.00"
        $index++
    }
    $results | ConvertTo-Json -Depth 20 | Set-Content "$output/results.json"
    "PASS: startup, checkpoint state, duplicate convergence and four service restarts; dataset=$($settings.dataset)" | Set-Content "$output/summary.txt"
    Write-Output "Evidence: $output"
} finally {
    if ($stopped) { Invoke-Stack @('start',$stopped) | Out-Null }
    if ($jobId) {
        Read-JobApi "/jobs/$jobId/exceptions" | ConvertTo-Json -Depth 20 | Set-Content "$output/exceptions.json"
        Invoke-Stack @('exec','-T','jobmanager','flink','cancel',$jobId) | Out-Null
    }
    foreach ($topic in $created) {
        Invoke-Stack @('exec','-T','kafka','/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','kafka:29092','--delete','--topic',$topic) | Out-Null
    }
}
