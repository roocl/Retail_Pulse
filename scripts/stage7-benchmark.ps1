param(
    [ValidateRange(100,100000)][int]$Count = 5000,
    [ValidateRange(3,20)][int]$Samples = 3
)
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/stack-support.ps1"
$jobs = Read-JobApi '/jobs/overview'
if (@($jobs.jobs | Where-Object state -NotIn @('FAILED','CANCELED','FINISHED')).Count) { throw 'Benchmark requires an idle cluster.' }
$id = [Guid]::NewGuid().ToString('N')
$output = "$stackRoot/target/stage7-benchmark/$id"
New-Item -ItemType Directory -Force $output | Out-Null
$settings = @{'bootstrap-servers'='kafka:29092';'input-topic'="bench-$id";'dead-letter-topic'="bench-dead-$id";
    'group-id'="bench-$id";dataset="bench-$id";'result-version'=1;parallelism=1;'checkpoint-interval-ms'=1000;
    'idle-timeout-ms'=10000;'out-of-order-ms'=30000;'top-n'=10}
$environment = @{
    utc=[DateTimeOffset]::UtcNow.ToString('o');gitCommit=(& git -C $stackRoot rev-parse HEAD);
    worktree=(& git -C $stackRoot status --short);powershell=$PSVersionTable.PSVersion.ToString();
    docker=(Invoke-Docker @('info','--format','{{json .}}') | ConvertFrom-Json | Select-Object ServerVersion,NCPU,MemTotal,OperatingSystem,Architecture);
    compose=(Invoke-Docker @('compose','version','--short'));settings=$settings;
    warmupCount=500;count=$Count;samples=$Samples;sourcePartitions=3;activeFixturePartitions=1;
    measurement='Batch wall-clock: Kafka CLI start to producer exit and exact aggregate visible through HTTP; includes CLI/JVM startup, client piping, watermark, sink flush and polling. Historical eventTime is not used for latency.'
}
$environment.sourceHashes = @(Get-ChildItem "$stackRoot/scripts/stack*.ps1", "$stackRoot/scripts/stage7*.ps1", "$stackRoot/infra/*compose.yml", "$stackRoot/Dockerfile", "$stackRoot/.dockerignore" | ForEach-Object {
    @{path=[IO.Path]::GetRelativePath($stackRoot,$_.FullName);sha256=(Get-FileHash $_.FullName).Hash}
})
$environment.artifactHashes = @(Get-ChildItem "$stackRoot/analytics-job/target/analytics-job-0.1.0-SNAPSHOT.jar", "$stackRoot/query-api/target/query-api-0.1.0-SNAPSHOT.jar" | ForEach-Object {
    @{path=[IO.Path]::GetRelativePath($stackRoot,$_.FullName);sha256=(Get-FileHash $_.FullName).Hash}
})
$environment.images = @(Invoke-Stack @('images','--format','json') | ConvertFrom-Json)
$environment.resources = @(Invoke-Stack @('ps','-q') | ForEach-Object {
    Invoke-Docker @('inspect','--format','{{.Name}} {{.HostConfig.Memory}} {{.HostConfig.NanoCpus}}',"$_")
})
$environment | ConvertTo-Json -Depth 15 | Set-Content "$output/environment.json"
$created = @()
$jobId = $null
$measurements = @()
try {
    foreach ($topic in @($settings['input-topic'],$settings['dead-letter-topic'])) {
        Invoke-Stack @('exec','-T','kafka','/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','kafka:29092',
            '--create','--topic',$topic,'--partitions','3','--replication-factor','1') | Out-Null
        $created += $topic
    }
    $jobId = Submit-StackJob $settings
    foreach ($sample in 0..$Samples) {
        $size = if ($sample -eq 0) { 500 } else { $Count }
        $window = 1768471200000L + $sample * 180000L
        $events = [System.Collections.Generic.List[string]]::new()
        for ($i=0; $i -lt $size; $i++) {
            $events.Add(([ordered]@{schemaVersion=2;eventId="$id-$sample-$i";orderId="$id-$sample-$i";
                userId="u$($i%100)";productId="p$($i%10)";eventType='PAYMENT_COMPLETED';amount=1.00;quantity=1;
                eventTime=[DateTimeOffset]::FromUnixTimeMilliseconds($window + $i%60000).ToString('yyyy-MM-ddTHH:mm:ss.fffZ')} | ConvertTo-Json -Compress))
        }
        $events.Add(([ordered]@{schemaVersion=2;eventId="$id-$sample-marker";orderId=$null;userId='u0';productId='p0';
            eventType='PRODUCT_CLICK';amount=0;quantity=1;eventTime=[DateTimeOffset]::FromUnixTimeMilliseconds($window+92000).ToString('yyyy-MM-ddTHH:mm:ss.fffZ')} | ConvertTo-Json -Compress))
        $inputFile = "$output/sample-$sample-input.jsonl"
        $events | Set-Content $inputFile
        $started = [DateTimeOffset]::UtcNow
        $timer = [Diagnostics.Stopwatch]::StartNew()
        Send-StackEvents $settings['input-topic'] $events.ToArray()
        $ackMs = $timer.Elapsed.TotalMilliseconds
        $metric = Wait-StackMetric $settings.dataset $window "$size.00" $size
        $visibleMs = $timer.Elapsed.TotalMilliseconds
        $timer.Stop()
        $measurement = @{sample=$sample;warmup=($sample -eq 0);startedUtc=$started.ToString('o');payments=$size;records=($size+1);
            acknowledgedMs=$ackMs;visibleMs=$visibleMs;acknowledgedRecordsPerSecond=(($size+1)*1000/$ackMs);
            visiblePaymentsPerSecond=($size*1000/$visibleMs);metric=$metric;
            inputSha256=(Get-FileHash $inputFile).Hash;inputBytes=(Get-Item $inputFile).Length;
            resources=@(Invoke-Docker (@('stats','--no-stream','--format','{{json .}}') + @(Invoke-Stack @('ps','-q'))) | ConvertFrom-Json)}
        $measurement | ConvertTo-Json -Depth 12 | Set-Content "$output/sample-$sample.json"
        $measurements += $measurement
        Write-Output "sample=$sample warmup=$($sample -eq 0) payments=$size ackMs=$([math]::Round($ackMs)) visibleMs=$([math]::Round($visibleMs))"
    }
    $measured = @($measurements | Where-Object { !$_.warmup })
    $totalVisible = ($measured | Measure-Object visibleMs -Sum).Sum
    $summary = @{dataset=$settings.dataset;jobId=$jobId;measuredSamples=$Samples;payments=($Count*$Samples);
        aggregateVisiblePaymentsPerSecond=($Count*$Samples*1000/$totalVisible);
        meanBatchVisibleMs=($measured | Measure-Object visibleMs -Average).Average;
        minBatchVisibleMs=($measured | Measure-Object visibleMs -Minimum).Minimum;
        maxBatchVisibleMs=($measured | Measure-Object visibleMs -Maximum).Maximum;
        scope='Local single-active-partition synthetic payment batches; not sustained capacity, per-event latency or a production SLA.'}
    $summary | ConvertTo-Json -Depth 5 | Set-Content "$output/summary.json"
    Write-Output "Evidence: $output"
} finally {
    if ($jobId) {
        Read-JobApi "/jobs/$jobId/checkpoints" | ConvertTo-Json -Depth 20 | Set-Content "$output/checkpoints.json"
        Read-JobApi "/jobs/$jobId/exceptions" | ConvertTo-Json -Depth 20 | Set-Content "$output/exceptions.json"
        Invoke-Stack @('exec','-T','jobmanager','flink','cancel',$jobId) | Out-Null
    }
    foreach ($topic in $created) {
        Invoke-Stack @('exec','-T','kafka','/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','kafka:29092','--delete','--topic',$topic) | Out-Null
    }
}
