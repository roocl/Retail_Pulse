$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$outputDirectory = Join-Path $projectRoot 'analytics-job/target/stage3-smoke'
New-Item -ItemType Directory -Force -Path $outputDirectory | Out-Null
$composeFiles = @('-f', (Join-Path $projectRoot 'infra/docker-compose.yml'), '-f', (Join-Path $projectRoot 'infra/flink-compose.yml'))
$runId = [Guid]::NewGuid().ToString('N')
$inputTopic = "stage3-input-$runId"
$deadTopic = "stage3-dead-$runId"
$jobId = $null
$createdTopics = @()

. (Join-Path $PSScriptRoot 'flink-test-support.ps1')

try {
    $deadline = [DateTime]::UtcNow.AddSeconds(60)
    do {
        try { $overview = Read-JobApi '/overview' } catch { $overview = $null }
        if ($overview -and $overview.taskmanagers -ge 1) { break }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $deadline)
    if (!$overview -or $overview.taskmanagers -lt 1) { throw 'Flink TaskManager did not register.' }

    foreach ($topic in @($inputTopic, $deadTopic)) {
        Invoke-Docker -Arguments @('exec', 'retailpulse-kafka', '/opt/kafka/bin/kafka-topics.sh',
            '--bootstrap-server', 'kafka:29092', '--create', '--topic', $topic,
            '--partitions', '3', '--replication-factor', '1') | Out-Null
        $createdTopics += $topic
    }
    $submission = Invoke-Docker -Arguments (@('compose') + $composeFiles + @('exec', '-T', 'jobmanager',
        'flink', 'run', '-d', '/opt/flink/usrlib/analytics-job.jar', '--bootstrap-servers', 'kafka:29092',
        '--input-topic', $inputTopic, '--dead-letter-topic', $deadTopic, '--group-id', "stage3-$runId",
        '--parallelism', '1', '--out-of-order-ms', '1000', '--idle-timeout-ms', '1500', '--checkpoint-interval-ms', '1000'))
    $submission | Set-Content (Join-Path $outputDirectory 'submission.log') -Encoding UTF8
    if (($submission -join "`n") -notmatch 'JobID ([a-f0-9]{32})') { throw "No submitted JobID: $submission" }
    $jobId = $Matches[1]

    $fixtures = @(Get-Content (Join-Path $PSScriptRoot 'fixtures/stage3-events.jsonl') -Encoding UTF8 | ForEach-Object { $_.Replace('stage3-', "stage3-$runId-") })
    $expectedIds = @("stage3-$runId-a", "stage3-$runId-b", "stage3-$runId-c")
    $fixtures | ForEach-Object { "fixture|$_" } | & docker exec -i retailpulse-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:29092 --topic $inputTopic --property parse.key=true --property 'key.separator=|'
    if ($LASTEXITCODE -ne 0) { throw 'Fixture production failed.' }
    $sentAfter = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()

    $expectedWatermark = 1768471203999L
    $deadline = [DateTime]::UtcNow.AddSeconds(60)
    do {
        $job = Read-JobApi "/jobs/$jobId"
        if ($job.state -in @('FAILED', 'CANCELED', 'FINISHED')) { throw "Unexpected job state: $($job.state)" }
        $taskLog = Invoke-Docker -Arguments (@('compose') + $composeFiles + @('logs', '--no-color', 'taskmanager'))
        $valid = @($taskLog | ForEach-Object {
            if ("$_" -match 'valid-event(:\d+)?>\s*(\{.*\})') { $Matches[2] | ConvertFrom-Json }
        } | Where-Object { $_.eventId -like "stage3-$runId-*" })
        $watermark = [long]::MinValue
        $watermarkFound = $false
        foreach ($vertex in $job.vertices) {
            if ($vertex.name -match 'deduplicate-event-id') {
                $metrics = Read-JobApi "/jobs/$jobId/vertices/$($vertex.id)/metrics?get=0.deduplicate-event-id.currentInputWatermark"
                if ($metrics.Count -gt 0) { $watermark = [long]$metrics[0].value; $watermarkFound = $true }
            }
        }
        $checkpoints = Read-JobApi "/jobs/$jobId/checkpoints"
        $completed = $checkpoints.latest.completed
        if ($valid.Count -ge 3 -and $watermark -ge $expectedWatermark -and $completed.trigger_timestamp -ge $sentAfter) { break }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $deadline)

    $taskLog | Set-Content (Join-Path $outputDirectory 'taskmanager.log') -Encoding UTF8
    $checkpoints | ConvertTo-Json -Depth 20 | Set-Content (Join-Path $outputDirectory 'checkpoints.json') -Encoding UTF8
    if ($valid.Count -ne 3 -or @($valid.eventId | Sort-Object -Unique).Count -ne 3) { throw "Expected exactly three unique main records; got $($valid.Count)." }
    if (Compare-Object $expectedIds @($valid.eventId)) { throw 'Main output contains unexpected event IDs.' }
    if (!$watermarkFound) { throw 'Flink did not expose the expected subtask/operator watermark metric.' }
    if ($watermark -lt $expectedWatermark) { throw "Idle partitions blocked the watermark: $watermark" }
    if (!$completed -or $completed.trigger_timestamp -lt $sentAfter) { throw 'No completed checkpoint after input.' }

    $deadLog = Invoke-Docker -Arguments @('exec', 'retailpulse-kafka', '/opt/kafka/bin/kafka-console-consumer.sh',
        '--bootstrap-server', 'kafka:29092', '--topic', $deadTopic, '--from-beginning', '--max-messages', '2', '--timeout-ms', '10000')
    $deadLog | Set-Content (Join-Path $outputDirectory 'dead-letters.log') -Encoding UTF8
    $dead = @($deadLog | Where-Object { "$_" -match '^\{' } | ForEach-Object { "$_" | ConvertFrom-Json })
    if ($dead.Count -ne 2) { throw 'Expected two dead letters.' }
    foreach ($record in $dead) {
        if ($record.sourceTopic -ne $inputTopic -or $record.sourceOffset -notin @(3, 4) -or !$record.reason) {
            throw 'Dead-letter source metadata or reason is invalid.'
        }
        $raw = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($record.valueBase64))
        if ($raw -ne $fixtures[$record.sourceOffset]) { throw 'Dead-letter payload did not preserve source bytes.' }
    }
    if (@($dead.sourceOffset | Sort-Object -Unique).Count -ne 2) { throw 'Missing one invalid source record.' }
    $offsets = Invoke-Docker -Arguments @('exec', 'retailpulse-kafka', '/opt/kafka/bin/kafka-get-offsets.sh',
        '--bootstrap-server', 'kafka:29092', '--topic', $deadTopic, '--time', '-1')
    $deadCount = ($offsets | ForEach-Object { [long](($_ -split ':')[-1]) } | Measure-Object -Sum).Sum
    if ($deadCount -ne 2) { throw "Dead-letter topic contains $deadCount records instead of two." }
    $summary = @("job=$jobId state=$($job.state)", 'input=6 valid=3 duplicate-suppressed=1 dead-letters=2',
        "watermark=$watermark expected-at-least=$expectedWatermark (two idle partitions)",
        "checkpoint=$($completed.id) path=$($completed.external_path)", 'Dead-letter source coordinates and original payloads match.')
    $summary | Set-Content (Join-Path $outputDirectory 'summary.txt') -Encoding UTF8
    $summary
} finally {
    if ($jobId) {
        try { Read-JobApi "/jobs/$jobId/exceptions" | ConvertTo-Json -Depth 20 | Set-Content (Join-Path $outputDirectory 'exceptions.json') -Encoding UTF8 } catch { Write-Warning $_ }
        Invoke-Docker -Arguments (@('compose') + $composeFiles + @('exec', '-T', 'jobmanager', 'flink', 'cancel', $jobId)) | Out-Null
    }
    foreach ($topic in $createdTopics) {
        Invoke-Docker -Arguments @('exec', 'retailpulse-kafka', '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'kafka:29092', '--delete', '--topic', $topic) | Out-Null
    }
}
