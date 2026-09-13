param([Parameter(Mandatory=$true)][ValidatePattern('^[a-f0-9]{64}$')][string]$SourceHash)
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/flink-test-support.ps1"
$offlineRoot = Split-Path -Parent $PSScriptRoot
$compose = @('compose','--env-file',"$offlineRoot/.env",'-f',"$offlineRoot/infra/offline-compose.yml")
$output = "$offlineRoot/target/stage8-smoke/$([Guid]::NewGuid().ToString('N'))"
New-Item -ItemType Directory -Force $output | Out-Null
Invoke-Docker ($compose + @('up','-d','--wait','--wait-timeout','150')) | Out-Null
$run = $compose + @('run','--rm','-T','--no-deps','spark')
$imported = Invoke-Docker ($run + @('ingest',"/data/retail/raw/$SourceHash/source.xlsx"))
if (@($imported | Where-Object { "$_".Trim() -eq "/data/retail/raw/$SourceHash" }).Count -ne 1) { throw 'Reimport did not preserve the source batch.' }
$built = Invoke-Docker ($run + @('build',"/data/retail/raw/$SourceHash"))
$built | Set-Content "$output/build.log"
$release = @($built | ForEach-Object { "$_".Trim() } | Where-Object { $_ -match '^[a-f0-9]{64}$' })
if ($release.Count -ne 1) { throw 'Build did not return one release.' }
$release = $release[0]
$repeated = Invoke-Docker ($run + @('build',"/data/retail/raw/$SourceHash"))
$repeated | Set-Content "$output/repeated.log"
if (@($repeated | Where-Object { "$_".Trim() -eq $release }).Count -ne 1) { throw 'Repeated build changed the release.' }
Invoke-Docker ($run + @('report',$release)) | Set-Content "$output/before-restart.log"
Invoke-Docker ($compose + @('restart','mysql','metastore')) | Out-Null
Invoke-Docker ($compose + @('up','-d','--wait','--wait-timeout','150')) | Out-Null
Invoke-Docker ($run + @('report',$release)) | Set-Content "$output/after-restart.log"
$plan = Invoke-Docker ($run + @('plan',$release,'2011-01'))
$plan | Set-Content "$output/partition-plan.txt"
if (($plan -join "`n") -notmatch 'PartitionFilters:.*2011-01') { throw 'Partition pruning not present in physical plan.' }
Copy-Item -LiteralPath "$offlineRoot/customer-analytics/data/retail/releases/$release/manifest.json" -Destination "$output/manifest.json"
Copy-Item -LiteralPath "$offlineRoot/customer-analytics/data/retail/releases/$release/quality.json" -Destination "$output/quality.json"
$images = Invoke-Docker @('image','inspect','retailpulse/offline:local','retailpulse/metastore:local','mysql:8.4.6','--format','{{.RepoTags}} {{.Id}}')
@{sourceHash=$SourceHash;release=$release;reimport=$true;repeatedBuild=$true;metadataRestart=$true;partitionPruning=$true;
    images=$images;docker=(Invoke-Docker @('info','--format','{{.ServerVersion}} CPUs={{.NCPU}} Memory={{.MemTotal}}'));
    command="./scripts/stage8-smoke.ps1 -SourceHash $SourceHash"} | ConvertTo-Json -Depth 6 | Set-Content "$output/result.json"
Write-Output "PASS: source reimport, repeated build, metadata restart and partition pruning; evidence=$output"
