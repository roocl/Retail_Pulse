param([int]$Count = 1000)

$ErrorActionPreference = 'Stop'
if ($Count -lt 1000) { throw 'Count must be at least 1000 for the distribution checks.' }
$projectRoot = Split-Path -Parent $PSScriptRoot
$outputDirectory = Join-Path $projectRoot 'event-producer/target/stage2-smoke'
New-Item -ItemType Directory -Force -Path $outputDirectory | Out-Null
$runId = [Guid]::NewGuid().ToString('N')
$topic = "stage2-smoke-$runId"
$consumerName = "stage2-consumer-$runId"
$producerJar = Join-Path $projectRoot 'event-producer/target/event-producer-0.1.0-SNAPSHOT.jar'
$consumerJar = Join-Path $projectRoot 'event-consumer/target/event-consumer-0.1.0-SNAPSHOT.jar'
if (!(Test-Path $producerJar) -or !(Test-Path $consumerJar)) { throw 'Build the Producer and Consumer jars first.' }

. (Join-Path $PSScriptRoot 'flink-test-support.ps1')

try {
    Invoke-Docker -Arguments @('exec', 'retailpulse-kafka', '/opt/kafka/bin/kafka-topics.sh',
        '--bootstrap-server', 'kafka:29092', '--create', '--topic', $topic,
        '--partitions', '3', '--replication-factor', '1') | Out-Null
    Invoke-Docker -Arguments @('run', '-d', '--name', $consumerName, '--network', 'infra_default',
        '--mount', "type=bind,source=$consumerJar,target=/app.jar,readonly",
        '-e', 'KAFKA_BOOTSTRAP_SERVERS=kafka:29092', '-e', "RETAILPULSE_KAFKA_TOPIC=$topic",
        '-e', "RETAILPULSE_CONSUMER_GROUP=$consumerName", '--entrypoint', 'java',
        'apache/kafka:3.8.1', '-jar', '/app.jar') | Out-Null
    $producerLog = Invoke-Docker -Arguments @('run', '--rm', '--network', 'infra_default',
        '--mount', "type=bind,source=$producerJar,target=/app.jar,readonly",
        '-e', 'KAFKA_BOOTSTRAP_SERVERS=kafka:29092', '-e', "RETAILPULSE_KAFKA_TOPIC=$topic",
        '-e', "RETAILPULSE_GENERATOR_COUNT=$Count", '--entrypoint', 'java',
        'apache/kafka:3.8.1', '-jar', '/app.jar', '--retailpulse.generator.interval-ms=0',
        '--retailpulse.generator.seed=42', '--retailpulse.generator.duplicate-rate=0.2',
        '--retailpulse.generator.out-of-order-rate=0.3')
    $producerLog | Set-Content (Join-Path $outputDirectory 'producer.log') -Encoding UTF8
    $deadline = [DateTime]::UtcNow.AddSeconds(60)
    do {
        $consumerLog = Invoke-Docker -Arguments @('logs', $consumerName)
        $received = @($consumerLog | Where-Object { "$_" -match 'received key=' })
        if ($received.Count -ge $Count) { break }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $deadline)
    $consumerLog | Set-Content (Join-Path $outputDirectory 'consumer.log') -Encoding UTF8
    $sent = @($producerLog | Where-Object { "$_" -match 'sent key=' })
    if ($sent.Count -ne $Count -or $received.Count -ne $Count) {
        throw "Expected $Count records; sent=$($sent.Count), received=$($received.Count)"
    }
    $expected = @($sent | ForEach-Object {
        if ("$_" -match 'sent (key=.*?) duplicate=(true|false) outOfOrder=(true|false) (event=.*)$') {
            "$($Matches[1]) $($Matches[4])"
        } else { throw "Unrecognized producer line: $_" }
    } | Sort-Object)
    $actual = @($received | ForEach-Object { ("$_" -split 'received ', 2)[1] } | Sort-Object)
    if (Compare-Object $expected $actual) { throw 'Producer and Consumer record contents or offsets differ.' }
    $duplicates = @($sent | Where-Object { "$_" -match 'duplicate=true' }).Count
    $late = @($sent | Where-Object { "$_" -match 'outOfOrder=true' }).Count
    if ([Math]::Abs($duplicates / ($Count - 1.0) - .2) -gt .04) { throw 'Duplicate rate outside tolerance.' }
    if ([Math]::Abs($late / ($Count - $duplicates - 1.0) - .3) -gt .04) { throw 'Out-of-order rate outside tolerance.' }
    $types = @($sent | ForEach-Object {
        if ("$_" -match 'eventType=([A-Z_]+)') { $Matches[1] }
    } | Group-Object | Sort-Object Name | ForEach-Object { "$($_.Name)=$($_.Count)" })
    if ($types.Count -ne 6) { throw 'Not all six event types were observed.' }
    $summary = @("sent=$Count received=$($received.Count) duplicates=$duplicates outOfOrder=$late",
        "duplicate denominator=$($Count - 1); outOfOrder denominator=$($Count - $duplicates - 1)",
        'Producer/Consumer key, partition, offset and complete event content match.', $types)
    $summary | Set-Content (Join-Path $outputDirectory 'summary.txt') -Encoding UTF8
    $summary
} finally {
    & docker rm -f $consumerName 2>&1 | Out-Null
    & docker exec retailpulse-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --delete --topic $topic 2>&1 | Out-Null
}
