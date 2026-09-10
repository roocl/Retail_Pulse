$ErrorActionPreference = 'Stop'

function Invoke-Docker {
    param([string[]]$Arguments)
    $lines = & docker @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) { throw "docker failed: $($lines -join [Environment]::NewLine)" }
    return $lines
}

function Read-JobApi {
    param([string]$Path)
    Invoke-RestMethod -Uri "http://localhost:8081$Path" -TimeoutSec 5
}
