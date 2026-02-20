@echo off
:: ============================================================
:: QuoteSnap Backend - First-Time Windows Setup Script
:: Run this ONCE when deploying to a new Windows server.
:: ============================================================

cd /d "%~dp0"

echo.
echo ============================================
echo  QuoteSnap Backend - Windows Setup
echo ============================================
echo.

:: Check Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python is not found in PATH.
    echo Please install Python 3.11+ from https://www.python.org/downloads/
    echo Make sure to check "Add Python to PATH" during installation.
    pause
    exit /b 1
)

echo [1/5] Creating Python virtual environment...
python -m venv venv
if errorlevel 1 (
    echo [ERROR] Failed to create virtual environment.
    pause
    exit /b 1
)

echo [2/5] Activating virtual environment...
call venv\Scripts\activate.bat

echo [3/5] Upgrading pip...
python -m pip install --upgrade pip

echo [4/5] Installing Python dependencies...
pip install -r requirements.txt
if errorlevel 1 (
    echo [ERROR] Failed to install requirements.
    pause
    exit /b 1
)

echo [5/5] Creating required directories...
if not exist "database"  mkdir database
if not exist "uploads"   mkdir uploads
if not exist "generated" mkdir generated
if not exist "logs"      mkdir logs
if not exist "tokens"    mkdir tokens

echo.
echo ============================================
echo  Setup complete!
echo ============================================
echo.
echo NEXT STEPS:
echo   1. Copy your .env file (or edit the existing one)
echo   2. Copy credentials.json for Gmail OAuth
echo   3. Run: start_server.bat
echo.
pause
