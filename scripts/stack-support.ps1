$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'flink-test-support.ps1')
$stackRoot = Split-Path -Parent $PSScriptRoot
$stackCompose = @('compose', '--env-file', "$stackRoot/.env", '-p', 'infra',
    '-f', "$stackRoot/infra/docker-compose.yml", '-f', "$stackRoot/infra/flink-compose.yml",
    '-f', "$stackRoot/infra/clickhouse-compose.yml", '-f', "$stackRoot/infra/app-compose.yml")

function Invoke-Stack {
    param([string[]]$Arguments)
    Invoke-Docker -Arguments ($stackCompose + $Arguments)
}

function Wait-StackJob {
    param([string]$JobId, [int]$TimeoutSeconds = 120)
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $job = Read-JobApi "/jobs/$JobId"
        if ($job.state -eq 'RUNNING' -and @($job.vertices | Where-Object status -NE 'RUNNING').Count -eq 0) { return $job }
        if ($job.state -in @('FAILED', 'CANCELED', 'FINISHED')) { throw "Job $JobId terminated: $($job.state)" }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Job $JobId did not become RUNNING."
}

function Submit-StackJob {
    param([hashtable]$Settings, [string]$Checkpoint)
    $arguments = @('exec', '-T', 'jobmanager', 'flink', 'run', '-d')
    if ($Checkpoint) { $arguments += @('-s', $Checkpoint) }
    $arguments += '/opt/flink/usrlib/analytics-job.jar'
    foreach ($key in ($Settings.Keys | Sort-Object)) { $arguments += @("--$key", [string]$Settings[$key]) }
    $result = Invoke-Stack $arguments
    if (($result -join "`n") -notmatch 'JobID ([a-f0-9]{32})') { throw 'Submission did not return a JobID.' }
    $id = $Matches[1]
    try {
        Wait-StackJob $id | Out-Null
    } catch {
        Invoke-Stack @('exec','-T','jobmanager','flink','cancel',$id) | Out-Null
        throw
    }
    return $id
}

function Wait-StackCheckpoint {
    param([string]$JobId, [long]$After = 0)
    $deadline = [DateTime]::UtcNow.AddSeconds(120)
    do {
        $state = Read-JobApi "/jobs/$JobId/checkpoints"
        if ($state.latest.completed -and $state.latest.completed.trigger_timestamp -gt $After) { return $state }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "No completed checkpoint after $After for $JobId."
}

function Send-StackEvents {
    param([string]$Topic, [string[]]$Events)
    $Events | ForEach-Object { "fixture|$_" } | & docker @stackCompose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:29092 --topic $Topic --property parse.key=true --property 'key.separator=|' --producer-property acks=all --producer-property enable.idempotence=true
    if ($LASTEXITCODE -ne 0) { throw 'Kafka fixture production failed.' }
}

function Wait-StackInput {
    param([string]$JobId, [string]$Group, [long]$ExpectedCount)
    $after = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    $deadline = [DateTime]::UtcNow.AddSeconds(120)
    do {
        $checkpointState = Wait-StackCheckpoint $JobId $after
        $offsets = Invoke-Stack @('exec','-T','kafka','/opt/kafka/bin/kafka-consumer-groups.sh','--bootstrap-server','kafka:29092','--group',$Group,'--describe')
        $count = 0L
        $lag = 0L
        foreach ($line in $offsets) {
            if ("$line" -match '^\S+\s+\S+\s+\d+\s+(\d+)\s+(\d+)\s+(\d+)') {
                $count += [long]$Matches[1]
                $lag += [long]$Matches[3]
            }
        }
        if ($count -eq $ExpectedCount -and $lag -eq 0) { return $checkpointState }
        Start-Sleep -Seconds 1
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Checkpoint offsets did not reach $ExpectedCount."
}

function Wait-StackMetric {
    param([string]$Dataset, [long]$WindowStart, [string]$Gmv, [long]$Orders)
    $from = [DateTimeOffset]::FromUnixTimeMilliseconds($WindowStart).ToString('yyyy-MM-ddTHH:mm:ssZ')
    $to = [DateTimeOffset]::FromUnixTimeMilliseconds($WindowStart + 60000).ToString('yyyy-MM-ddTHH:mm:ssZ')
    $deadline = [DateTime]::UtcNow.AddSeconds(120)
    do {
        $response = Invoke-WebRequest "http://127.0.0.1:8080/api/metrics/range?dataset=$Dataset&from=$from&to=$to" -SkipHttpErrorCheck -TimeoutSec 10
        if ($response.StatusCode -eq 200) {
            $result = $response.Content | ConvertFrom-Json
            if ($result.items.Count -eq 1 -and $result.items[0].gmv -ceq $Gmv -and $result.items[0].paidOrders -ceq "$Orders") { return $result.items[0] }
        }
        Start-Sleep -Milliseconds 200
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "API did not return expected gmv=$Gmv orders=$Orders for $Dataset at $from."
}

function Find-StackCheckpoint {
    param([ValidatePattern('^[a-f0-9]{32}$')][string]$JobId)
    $paths = Invoke-Stack @('exec','-T','jobmanager','find',"/opt/flink/checkpoints/$JobId",'-name','_metadata','-type','f')
    $latest = @($paths | ForEach-Object {
        if ("$_" -match '/chk-(\d+)/_metadata$') { @{number=[long]$Matches[1];path="file://$_"} }
    } | Sort-Object number -Descending | Select-Object -First 1)
    if (!$latest.Count) { throw "No completed checkpoint metadata exists for $JobId." }
    return $latest[0].path
}

function Save-StackManifest {
    param([hashtable]$Manifest, [string]$Path)
    $temporary = "$Path.tmp"
    [IO.File]::WriteAllText($temporary, ($Manifest | ConvertTo-Json -Depth 10), [Text.UTF8Encoding]::new($false))
    [IO.File]::Move($temporary, $Path, $true)
}
