param(
    [Parameter(Mandatory=$true)][ValidatePattern('^[a-f0-9]{64}$')][string]$Model,
    [Parameter(Mandatory=$true)][ValidatePattern('^[a-f0-9]{64}$')][string]$Batch,
    [string]$Dataset='uci-online-retail'
)
$ErrorActionPreference='Stop'
. "$PSScriptRoot/flink-test-support.ps1"
$stageRoot=Split-Path -Parent $PSScriptRoot
$compose=@('compose','--env-file',"$stageRoot/.env",'-f',"$stageRoot/infra/offline-compose.yml")
$output="$stageRoot/target/stage10-smoke/$([Guid]::NewGuid().ToString('N'))"
New-Item -ItemType Directory -Force $output | Out-Null
$ids=@()
foreach($attempt in 1..2){
    $lines=Invoke-Docker ($compose+@('run','--rm','-T','--no-deps','spark','--dataset',$Dataset,'score',$Model,$Batch))
    $lines | Set-Content "$output/score-$attempt.log"
    $found=@($lines | Where-Object {"$_" -match '^[a-f0-9]{64}$'})
    if($found.Count -ne 1){throw 'Expected one scoring batch identity'}
    $ids+="$($found[0])"
}
if($ids[0] -ne $ids[1]){throw 'Repeated scoring changed batch identity'}
$parameters="dataset=$([Uri]::EscapeDataString($Dataset))&batch=$Batch"
$page=Invoke-RestMethod "http://127.0.0.1:8080/api/customers?$parameters&limit=1"
if(!$page.items.Count){throw 'Missing customer cohort'}
$customer=$page.items[0].customerId
$prediction=Invoke-RestMethod "http://127.0.0.1:8080/api/customers/$customer/prediction?$parameters"
if($prediction.batch.id -ne $ids[0] -or $prediction.batch.modelId -ne $Model -or $prediction.batch.profileBatchId -ne $Batch){throw 'Prediction identity mismatch'}
if($prediction.score.value -lt 0 -or $prediction.score.value -gt 1){throw 'Invalid score'}
$missing=Invoke-WebRequest "http://127.0.0.1:8080/api/customers/$customer/prediction?dataset=missing&batch=$Batch" -SkipHttpErrorCheck
if([int]$missing.StatusCode -ne 204){throw 'Prediction leaked across dataset'}
$prediction | ConvertTo-Json -Depth 8 | Set-Content "$output/prediction.json"
foreach($file in 'manifest.json','selection.json','evaluation.json'){
    Copy-Item "$stageRoot/customer-analytics/data/retail/models/$Model/$file" "$output/$file"
}
Copy-Item "$stageRoot/customer-analytics/data/retail/scores/$($ids[0])/batch.json" "$output/score-batch.json"
@{model=$Model;profileBatch=$Batch;scoreBatch=$ids[0];repeatedScoring=$true;httpPrediction=$true;datasetIsolation=$true;
    command="./scripts/stage10-smoke.ps1 -Model $Model -Batch $Batch -Dataset $Dataset"} | ConvertTo-Json | Set-Content "$output/result.json"
Write-Output "PASS: model scoring and HTTP; evidence=$output"
