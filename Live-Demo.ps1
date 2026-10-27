param([switch]$NoPause, [switch]$PredictionDemo)
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot
function Docker-Step {
    param([string[]]$DockerArgs)
    & docker @DockerArgs
    if ($LASTEXITCODE -ne 0) { throw "Docker step failed: docker $($DockerArgs -join ' ')" }
}
function Pause-Demo([string]$Message) {
    Write-Host "`n$Message" -ForegroundColor Cyan
    if (-not $NoPause) { Read-Host 'Press Enter to continue' | Out-Null }
}
Write-Host 'BDA live demonstration: HDFS + Spark on YARN' -ForegroundColor Green
Docker-Step -DockerArgs @('info','--format','Docker engine: {{.ServerVersion}}')
Docker-Step -DockerArgs @('compose','up','-d')
$ready = $false
for ($attempt=0; $attempt -lt 30; $attempt++) {
    & docker compose exec -T master hdfs dfs -test -e /datasets/food/products_500mb_plus.tsv 2>$null
    if ($LASTEXITCODE -eq 0) { $ready=$true; break }
    Start-Sleep -Seconds 2
}
if (-not $ready) { throw 'HDFS dataset is unavailable. Follow handoff/RECREATE.txt; this script does not format or replace data.' }
Docker-Step -DockerArgs @('compose','exec','-T','master','hdfs','dfsadmin','-safemode','wait')
Pause-Demo '1. Show HDFS at http://localhost:9870. Explain one NameNode, two DataNodes and replication factor 2.'
Docker-Step -DockerArgs @('compose','exec','-T','master','hdfs','dfsadmin','-report')
Docker-Step -DockerArgs @('compose','exec','-T','master','hdfs','dfs','-ls','-h','/datasets/food')
Pause-Demo '2. Show YARN at http://localhost:8088. Both NodeManagers should be RUNNING.'
Docker-Step -DockerArgs @('compose','exec','-T','master','yarn','node','-list')
Pause-Demo '3. Run a fresh verification job. Watch YARN RUNNING status and http://localhost:4040 while it executes. This counts the real HDFS dataset and runs a small synthetic MLlib check; it does not retrain the project models.'
Docker-Step -DockerArgs @('compose','exec','-T','master','bash','/cluster/submit-verification.sh')
Pause-Demo '4. Show completed jobs at http://localhost:18080. Open the new verification job, then the saved BDA-Food-Nutrition-Full-Pipeline job. Show its Executors tab and tasks on worker1 and worker2.'
$summary = Get-Content results/summary.json -Raw | ConvertFrom-Json
Write-Host "Recorded full-pipeline application: $($summary.application_id)"
Write-Host "Cleaned records: $($summary.clean_rows)"
Write-Host ('Held-out accuracy: {0:P2}; weighted F1: {1:N4}' -f $summary.test_metrics.accuracy,$summary.test_metrics.f1)
Write-Host "Test products: $($summary.model_rows.test); training products: $($summary.model_rows.train)"
Pause-Demo '5. Open results/charts: confusion.png, importance.png and clusters.png. Explain that these are saved full-pipeline results, not results of the short verification job.'
if ($PredictionDemo) {
    Pause-Demo 'Optional: saved-model reload demo. This path has not yet been runtime-tested. It writes 100 demonstration predictions and requires previously saved HDFS models.'
    Docker-Step -DockerArgs @('compose','exec','-T','master','bash','/cluster/predict.sh')
}
Write-Host 'Demo finished. The cluster remains running. Use docker compose stop after the presentation to release resources.' -ForegroundColor Green
