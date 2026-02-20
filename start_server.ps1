# ============================================================
# QuoteSnap Backend - PowerShell Start Script
# Alternative to start_server.bat for PowerShell users.
# ============================================================

Set-Location $PSScriptRoot

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host " QuoteSnap Backend - Starting Server" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# Check virtual environment
if (-not (Test-Path "venv\Scripts\Activate.ps1")) {
    Write-Host "[ERROR] Virtual environment not found. Run setup.bat first!" -ForegroundColor Red
    exit 1
}

# Activate virtual environment
& .\venv\Scripts\Activate.ps1

Write-Host "Starting server... (Press Ctrl+C to stop)" -ForegroundColor Green
Write-Host ""

python run.py

Write-Host ""
Write-Host "Server stopped." -ForegroundColor Yellow
