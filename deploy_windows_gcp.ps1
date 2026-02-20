# =======================================================
# QuoteSnap GCP Windows Server Deployment Script
# =======================================================

Write-Host "=== Starting QuoteSnap Deployment on Windows Server ===" -ForegroundColor Cyan

# 1. Verify Python Installation
if (!(Get-Command "python" -ErrorAction SilentlyContinue)) {
    Write-Host "ERROR: Python is not installed or not in PATH." -ForegroundColor Red
    Write-Host "Please download Python 3.10+ and install it, checking 'Add to PATH'."
    exit 1
}

# 2. Verify Microsoft Excel Installation
$excelInstalled = $false
try {
    $testExcel = New-Object -ComObject Excel.Application
    $testExcel.Quit()
    $excelInstalled = $true
} catch {
    Write-Host "WARNING: Microsoft Excel COM Object not found!" -ForegroundColor Yellow
    Write-Host "win32com requires a licensed Microsoft Excel installation to generate quotations."
    Write-Host "Please install Microsoft Office before running the application in production."
}

if ($excelInstalled) {
    Write-Host "Success: Microsoft Excel is installed." -ForegroundColor Green
}

# 3. Create and Activate Virtual Environment
Write-Host "Creating Virtual Environment..." -ForegroundColor Cyan
python -m venv venv

# 4. Install Dependencies
Write-Host "Installing pip requirements..." -ForegroundColor Cyan
.\venv\Scripts\python.exe -m pip install --upgrade pip
.\venv\Scripts\python.exe -m pip install -r requirements.txt

# 5. MotherDuck and Environment Verification
if (!(Test-Path ".env")) {
    Write-Host "No .env file found. Creating from .env.example..." -ForegroundColor Yellow
    Copy-Item ".env.example" ".env"
    Write-Host "Please edit the .env file and add your MOTHERDUCK_TOKEN and OPENAI_API_KEY!" -ForegroundColor Red
}

$envContent = Get-Content .env
if ($envContent -match "MOTHERDUCK_TOKEN=.*[a-zA-Z0-9]") {
    Write-Host "MotherDuck Token verified in .env." -ForegroundColor Green
} else {
    Write-Host "WARNING: MOTHERDUCK_TOKEN is missing or empty. DuckDB will fall back to local_dev.duckdb." -ForegroundColor Yellow
}

# 6. Ensure Required Directories Exist
Write-Host "Ensuring output directories exist..." -ForegroundColor Cyan
$MissingDirs = @("generated", "uploads", "logs", "tokens")
foreach ($dir in $MissingDirs) {
    if (!(Test-Path $dir)) {
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
    }
}

Write-Host "=== Deployment Configuration Complete ===" -ForegroundColor Green
Write-Host "To start the application manually for testing, run:"
Write-Host ".\venv\Scripts\python.exe run.py"
Write-Host ""
Write-Host "For production, please configure NSSM (Non-Sucking Service Manager) to run 'run.py' as a Windows Background Service."
