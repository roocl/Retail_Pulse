param(
    [Parameter(Position=0)][ValidateSet('up','down','status','run','test','serve')][string]$Action = 'status',
    [switch]$SkipBuild,
    [Parameter(ValueFromRemainingArguments=$true)][string[]]$JobArguments
)
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/flink-test-support.ps1"
$offlineRoot = Split-Path -Parent $PSScriptRoot
$offlineCompose = @('compose','--env-file',"$offlineRoot/.env",'-f',"$offlineRoot/infra/offline-compose.yml")
Invoke-Docker ($offlineCompose + @('config','--quiet')) | Out-Null
if ($Action -eq 'up') {
    if (!$SkipBuild) {
        & mvn -f "$offlineRoot/pom.xml" -pl customer-analytics -am -DskipTests package --no-transfer-progress
        if ($LASTEXITCODE -ne 0) { throw 'Offline Maven packaging failed.' }
        Invoke-Docker ($offlineCompose + @('--profile','jobs','build')) | Write-Output
    }
    Invoke-Docker ($offlineCompose + @('up','-d','--wait','--wait-timeout','150')) | Write-Output
} elseif ($Action -eq 'serve') {
    if (!$SkipBuild) {
        & mvn -f "$offlineRoot/pom.xml" -pl query-api -am '-DskipTests' package --no-transfer-progress
        if ($LASTEXITCODE -ne 0) { throw 'Query API packaging failed.' }
        Invoke-Docker ($offlineCompose + @('--profile','serving','build','query-api')) | Write-Output
    }
    Invoke-Docker ($offlineCompose + @('--profile','serving','up','-d','--wait','query-api')) | Write-Output
} elseif ($Action -eq 'down') {
    Invoke-Docker ($offlineCompose + @('down')) | Write-Output
} elseif ($Action -eq 'run') {
    if (!$JobArguments) { throw 'Specify download, ingest, build, report, plan, profile, train, samples, evaluate, score or version arguments.' }
    Invoke-Docker ($offlineCompose + @('run','--rm','-T','--no-deps','spark') + $JobArguments) | Write-Output
} elseif ($Action -eq 'test') {
    Invoke-Docker ($offlineCompose + @('run','--rm','-T','--no-deps','spark','test') + $JobArguments) | Write-Output
} else {
    Invoke-Docker ($offlineCompose + @('ps')) | Write-Output
}
