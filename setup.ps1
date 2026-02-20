# ============================================================
# QuoteSnap Backend - PowerShell First-Time Setup Script
# Run this ONCE when deploying to a new Windows server.
# ============================================================

Set-Location $PSScriptRoot

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host " QuoteSnap Backend - Windows Setup" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# Check Python
try {
    $pythonVersion = python --version 2>&1
    Write-Host "[OK] Found: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "[ERROR] Python not found. Install from https://www.python.org/downloads/" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "[1/5] Creating Python virtual environment..."
python -m venv venv

Write-Host "[2/5] Activating virtual environment..."
& .\venv\Scripts\Activate.ps1

Write-Host "[3/5] Upgrading pip..."
python -m pip install --upgrade pip

Write-Host "[4/5] Installing Python dependencies..."
pip install -r requirements.txt
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Failed to install requirements." -ForegroundColor Red
    exit 1
}

Write-Host "[5/5] Creating required directories..."
@("database", "uploads", "generated", "logs", "tokens") | ForEach-Object {
    if (-not (Test-Path $_)) {
        New-Item -ItemType Directory -Path $_ | Out-Null
        Write-Host "  Created: $_"
    } else {
        Write-Host "  Exists:  $_"
    }
}

Write-Host ""
Write-Host "============================================" -ForegroundColor Green
Write-Host " Setup complete!" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green
Write-Host ""
Write-Host "NEXT STEPS:"
Write-Host "  1. Edit .env with your settings"
Write-Host "  2. Copy credentials.json for Gmail OAuth"
Write-Host "  3. Run: .\start_server.ps1"
Write-Host ""
