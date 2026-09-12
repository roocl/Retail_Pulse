param(
    [Parameter(Position=0)][ValidateSet('up','down','status','recover')][string]$Action = 'status',
    [switch]$SkipBuild,
    [switch]$NoJob,
    [string]$Checkpoint
)
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/stack-support.ps1"
$manifestPath = "$stackRoot/checkpoints/stack.json"
$manifest = if (Test-Path $manifestPath) { Get-Content $manifestPath -Raw | ConvertFrom-Json -AsHashtable } else { $null }
Invoke-Stack @('config', '--quiet') | Out-Null
if ($Action -eq 'up') {
    if (!$manifest) {
        try { $existing = Read-JobApi '/jobs/overview' } catch { $existing = $null }
        if ($existing -and @($existing.jobs | Where-Object state -NotIn @('FAILED','CANCELED','FINISHED')).Count) {
            throw 'Unmanaged active jobs exist; stop them explicitly before starting this stack.'
        }
    }
    if (!$SkipBuild -and !$manifest) {
        Push-Location $stackRoot
        try {
            & mvn -DskipTests package --no-transfer-progress
            if ($LASTEXITCODE -ne 0) { throw 'Maven packaging failed.' }
        } finally { Pop-Location }
        Invoke-Stack @('--profile','tools','build') | Out-Host
    }
    Invoke-Stack @('up','-d','--wait','--wait-timeout','180') | Out-Host
    if ($NoJob) { return }
    if ($manifest) {
        $jobs = Read-JobApi '/jobs/overview'
        if (@($jobs.jobs | Where-Object { $_.jid -eq $manifest.jobId -and $_.state -eq 'RUNNING' }).Count -ne 1) {
            throw 'Saved job is not running. Use stack.ps1 recover with its completed checkpoint; never start from offsets alone.'
        }
        Wait-StackJob $manifest.jobId | Out-Null
    } else {
        $id = [Guid]::NewGuid().ToString('N')
        $settings = @{'bootstrap-servers'='kafka:29092'; 'input-topic'="demo-$id"; 'dead-letter-topic'="demo-dead-$id";
            'group-id'="demo-$id"; dataset="demo-$id"; 'result-version'=1; parallelism=1}
        foreach ($topic in @($settings['input-topic'], $settings['dead-letter-topic'])) {
            Invoke-Stack @('exec','-T','kafka','/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','kafka:29092',
                '--create','--topic',$topic,'--partitions','3','--replication-factor','1') | Out-Null
        }
        $jobId = Submit-StackJob $settings
        $manifest = @{jobId=$jobId; settings=$settings; generatorSeed=[DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds(); demoProduced=$false; jarSha256=((Invoke-Stack @('exec','-T','jobmanager','sha256sum','/opt/flink/usrlib/analytics-job.jar')) -split ' ' )[0]}
        New-Item -ItemType Directory -Force (Split-Path $manifestPath) | Out-Null
        Save-StackManifest $manifest $manifestPath
    }
    if (!$manifest.demoProduced) {
        Invoke-Stack @('run','--rm','--no-deps','event-producer',"--retailpulse.kafka.topic=$($manifest.settings['input-topic'])",
            '--retailpulse.generator.count=1000','--retailpulse.generator.interval-ms=0',"--retailpulse.generator.seed=$($manifest.generatorSeed)") | Set-Content "$stackRoot/checkpoints/producer.log"
        $manifest.demoProduced = $true
        Save-StackManifest $manifest $manifestPath
        Write-Output "Demo producer completed; log=checkpoints/producer.log"
    }
}
if ($Action -eq 'recover') {
    if (!$manifest) { throw 'No saved job manifest.' }
    if (((Invoke-Stack @('exec','-T','jobmanager','sha256sum','/opt/flink/usrlib/analytics-job.jar')) -split ' ' )[0] -ne $manifest.jarSha256) { throw 'JAR differs from the saved job; review state compatibility before restoring.' }
    $jobs = Read-JobApi '/jobs/overview'
    if (@($jobs.jobs | Where-Object state -NotIn @('FAILED','CANCELED','FINISHED')).Count -gt 0) { throw 'Active jobs exist; recovery requires an idle session cluster.' }
    if (!$Checkpoint) {
        $Checkpoint = if ($manifest.savepoint) { $manifest.savepoint } else { Find-StackCheckpoint $manifest.jobId }
    }
    $manifest.jobId = Submit-StackJob $manifest.settings $Checkpoint
    $manifest.Remove('savepoint')
    Save-StackManifest $manifest $manifestPath
}
if ($manifest -and $Action -eq 'down') {
    $stopped = Invoke-Stack @('exec','-T','jobmanager','flink','stop','--savepointPath','file:///opt/flink/checkpoints', $manifest.jobId)
    if (($stopped -join "`n") -notmatch '(file:/\S*savepoint-\S+)') { throw 'No durable stop savepoint returned.' }
    $manifest.savepoint = $Matches[1]
    Save-StackManifest $manifest $manifestPath
}
if ($manifest -and $Action -ne 'down') {
    $checkpointState = Wait-StackCheckpoint $manifest.jobId
    $manifest.checkpoint = $checkpointState.latest.completed.external_path
    Save-StackManifest $manifest $manifestPath
    Write-Output "dataset=$($manifest.settings.dataset) job=$($manifest.jobId) checkpoint=$($manifest.checkpoint)"
}
if ($Action -eq 'down') {
    Invoke-Stack @('down') | Out-Host
} else {
    Invoke-Stack @('ps') | Out-Host
}
