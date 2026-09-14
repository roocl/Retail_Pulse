param([Parameter(Mandatory=$true)][ValidatePattern('^[a-f0-9]{64}$')][string]$Batch)
$ErrorActionPreference='Stop'
. "$PSScriptRoot/flink-test-support.ps1"
$stageRoot=Split-Path -Parent $PSScriptRoot
$compose=@('compose','--env-file',"$stageRoot/.env",'-f',"$stageRoot/infra/offline-compose.yml")
$artifact="$stageRoot/customer-analytics/data/retail/profiles/$Batch"
$manifest=Get-Content -Raw "$artifact/batch.json" | ConvertFrom-Json
$output="$stageRoot/target/stage9-smoke/$([Guid]::NewGuid().ToString('N'))"
New-Item -ItemType Directory -Force $output | Out-Null
$repeated=Invoke-Docker ($compose+@('run','--rm','-T','--no-deps','spark','--dataset',$manifest.dataset,'--window-days',"$($manifest.window_days)",'profile',$manifest.source_release,$manifest.observation))
$repeated | Set-Content "$output/repeated.log"
if(@($repeated | Where-Object {"$_".Trim() -eq $Batch}).Count -ne 1){throw 'Repeated publication changed batch identity'}
& "$PSScriptRoot/offline.ps1" serve -SkipBuild | Out-Null
$dataset=[Uri]::EscapeDataString($manifest.dataset)
$base='http://127.0.0.1:8080'
$seen=[Collections.Generic.HashSet[string]]::new()
$after=''
$pages=0
$first=$null
while($true){
    $page=Invoke-RestMethod "$base/api/customers?dataset=$dataset&batch=$Batch&limit=100&after=$after"
    if($page.batch.id -ne $Batch){throw 'Page changed batch'}
    foreach($profile in $page.items){if(!$seen.Add($profile.customerId)){throw 'Pagination repeated a customer'};if(!$first){$first=$profile}}
    $pages++
    if(!$page.hasMore){break}
    if(!$page.nextAfter){throw 'Missing next cursor'}
    $after=[Uri]::EscapeDataString($page.nextAfter)
}
if($seen.Count -ne $manifest.customers){throw 'Pagination lost customers'}
if(!$first){throw 'Expected real customer cohort'}
$detail=Invoke-RestMethod "$base/api/customers/$($first.customerId)?dataset=$dataset&batch=$Batch"
if($detail.customerId -ne $first.customerId -or $detail.purchaseAmount -ne $first.purchaseAmount){throw 'Detail differs from list'}
$summary=Invoke-RestMethod "$base/api/customers/summary?dataset=$dataset&batch=$Batch"
if(($summary | Measure-Object -Property customers -Sum).Sum -ne $seen.Count){throw 'Summary count mismatch'}
$filtered=Invoke-RestMethod "$base/api/customers?dataset=$dataset&batch=$Batch&segment=$($first.segment)&limit=100"
if(@($filtered.items | Where-Object segment -NE $first.segment).Count){throw 'Segment filter mismatch'}
$invalid=Invoke-WebRequest "$base/api/customers?limit=101" -SkipHttpErrorCheck
if([int]$invalid.StatusCode -ne 400){throw 'Invalid limit accepted'}
$plan=Invoke-Docker ($compose+@('exec','-T','customer-mysql','sh','-c','MYSQL_PWD="$MYSQL_PASSWORD" mysql -ucustomer customers -e "$1"','sh',"EXPLAIN SELECT * FROM customer_profiles WHERE batch_id='$Batch' AND segment='SINGLE_PURCHASE' AND customer_id>'10000' ORDER BY customer_id LIMIT 26"))
$plan | Set-Content "$output/query-plan.txt"
if(($plan -join "`n") -notmatch 'segment_page|PRIMARY'){throw 'Expected indexed page lookup'}
Invoke-Docker ($compose+@('restart','customer-mysql')) | Out-Null
Invoke-Docker ($compose+@('up','-d','--wait','customer-mysql')) | Out-Null
$restored=Invoke-RestMethod "$base/api/customers/$($first.customerId)?dataset=$dataset&batch=$Batch"
if($restored.purchaseAmount -ne $first.purchaseAmount){throw 'Restart changed profile'}
Copy-Item "$artifact/batch.json" "$output/batch.json"
$summary | ConvertTo-Json -Depth 8 | Set-Content "$output/summary.json"
$detail | ConvertTo-Json -Depth 8 | Set-Content "$output/sample.json"
@{batch=$Batch;customers=$seen.Count;pages=$pages;repeatedPublish=$true;stablePagination=$true;detail=$true;segmentFilter=$true;invalidInput=$true;databaseRestart=$true;indexedQuery=$true;command="./scripts/stage9-smoke.ps1 -Batch $Batch"} | ConvertTo-Json | Set-Content "$output/result.json"
Write-Output "PASS: customer publication and HTTP checks; evidence=$output"
