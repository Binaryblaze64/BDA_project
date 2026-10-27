$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot
Write-Host 'Food Atlas: http://localhost:8765' -ForegroundColor Green
Write-Host 'Keep this window open. Press Ctrl+C to stop the dashboard and its inference process.'
Write-Host 'Saved charts work without Docker. Live predictions require the running BDA cluster.'
python dashboard/server.py
if ($LASTEXITCODE -ne 0) { throw 'Dashboard stopped with an error. Check whether port 8765 is already in use.' }
