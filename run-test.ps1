# Start Kafka, send test messages, and show consumer output.
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

Write-Host "Starting Kafka..." -ForegroundColor Cyan
docker compose up -d
if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to start Kafka. Start Docker Desktop first, then run this script again." -ForegroundColor Red
    exit 1
}

Write-Host "Waiting 30s for Kafka to be ready..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

Write-Host "`nSending test messages..." -ForegroundColor Cyan
python producer.py

Write-Host "`nReading messages (5 second window)..." -ForegroundColor Cyan
$job = Start-Job { Set-Location $using:PSScriptRoot; python consumer.py }
Start-Sleep -Seconds 8
Stop-Job $job -ErrorAction SilentlyContinue
Receive-Job $job
Remove-Job $job -Force -ErrorAction SilentlyContinue

Write-Host "`nDone. Take a screenshot of the output above for your deliverable." -ForegroundColor Green
