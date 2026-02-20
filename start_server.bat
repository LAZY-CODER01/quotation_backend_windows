@echo off
:: ============================================================
:: QuoteSnap Backend - Windows Start Script
:: Run this to start the backend server.
:: ============================================================

cd /d "%~dp0"

echo.
echo ============================================
echo  QuoteSnap Backend - Starting Server
echo ============================================
echo.

:: Activate virtual environment
if not exist "venv\Scripts\activate.bat" (
    echo [ERROR] Virtual environment not found.
    echo Please run setup.bat first!
    pause
    exit /b 1
)

call venv\Scripts\activate.bat

:: Load .env is handled by python-dotenv inside the app.
:: Start the server using the Windows-compatible run.py entry point.
echo Starting server... (Press Ctrl+C to stop)
echo.
python run.py

echo.
echo Server stopped.
pause
