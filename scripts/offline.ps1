param(
    [Parameter(Position=0)][ValidateSet('up','down','status','run','test')][string]$Action = 'status',
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
} elseif ($Action -eq 'down') {
    Invoke-Docker ($offlineCompose + @('down')) | Write-Output
} elseif ($Action -eq 'run') {
    if (!$JobArguments) { throw 'Specify download, ingest, build, report or plan arguments.' }
    Invoke-Docker ($offlineCompose + @('run','--rm','-T','--no-deps','spark') + $JobArguments) | Write-Output
} elseif ($Action -eq 'test') {
    Invoke-Docker ($offlineCompose + @('run','--rm','-T','--no-deps','spark','test')) | Write-Output
} else {
    Invoke-Docker ($offlineCompose + @('ps')) | Write-Output
}
