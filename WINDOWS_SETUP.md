# QuoteSnap Backend — Windows Server Setup Guide

> **No Docker required.** Run the Flask backend directly on Windows using Python + Waitress.

---

## Prerequisites

Install the following tools **before** running setup:

### 1. Python 3.11+
Download from [python.org/downloads](https://www.python.org/downloads/)

> ⚠️ During installation, check **"Add Python to PATH"**

Verify:
```
python --version
```

---

### 2. Tesseract OCR (required for PDF/image parsing)

1. Download the installer from:  
   👉 [github.com/UB-Mannheim/tesseract/wiki](https://github.com/UB-Mannheim/tesseract/wiki)
2. Run the installer, note the install path (e.g. `C:\Program Files\Tesseract-OCR`)
3. Add that path to your Windows **System PATH** environment variable

Verify:
```
tesseract --version
```

---

### 3. Poppler (required for `pdf2image`)

1. Download the latest Windows binary from:  
   👉 [github.com/oschwartz10612/poppler-windows/releases](https://github.com/oschwartz10612/poppler-windows/releases)
2. Extract to a folder, e.g. `C:\poppler`
3. Add `C:\poppler\Library\bin` to your Windows **System PATH**

Verify:
```
pdftoppm -v
```

---

## First-Time Setup

1. Copy the project folder to your server (e.g. `C:\QuoteSnap\`)
2. Open **Command Prompt** (or PowerShell) inside that folder
3. Run the setup script:

**CMD / Command Prompt:**
```bat
setup.bat
```

**PowerShell:**
```powershell
.\setup.ps1
```

This will:
- Create a Python virtual environment (`venv/`)
- Install all Python dependencies (including Waitress)
- Create required directories: `database/`, `uploads/`, `generated/`, `logs/`, `tokens/`

---

## Configure Environment Variables

Edit the `.env` file in the project root. Key settings:

```env
# Server (optional — defaults to 0.0.0.0:8000 with 4 threads)
SERVER_HOST=0.0.0.0
SERVER_PORT=8000
SERVER_THREADS=4

# Security
JWT_SECRET=your-strong-secret-here
SECRET_KEY=your-flask-secret-key

# Database
DUCKDB_PATH=database/snapquote.duckdb

# OpenAI
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-4o-mini

# Gmail OAuth
GMAIL_CREDENTIALS_FILE=credentials.json
GMAIL_TOKEN_DIRECTORY=tokens
OAUTH_REDIRECT_URI=http://YOUR_SERVER_IP:8000/api/admin/gmail/callback

# Frontend CORS
FRONTEND_URL=http://YOUR_FRONTEND_URL
CORS_ORIGINS=http://YOUR_FRONTEND_URL,http://localhost:5173

# File paths
UPLOAD_FOLDER=uploads
EXCEL_TEMPLATE_PATH=sample/QuotationFormat.xlsx
LOG_FILE=logs/app.log
```

---

## Place Gmail Credentials

Copy your `credentials.json` (Google OAuth client secret) to the project root:
```
C:\QuoteSnap\credentials.json
```

---

## Start the Server

**CMD / Command Prompt:**
```bat
start_server.bat
```

**PowerShell:**
```powershell
.\start_server.ps1
```

**Manually:**
```
venv\Scripts\activate
python run.py
```

The server will start on `http://0.0.0.0:8000` by default.

---

## Run as a Windows Service (Optional)

To run the backend automatically at boot and restart on crashes, use **NSSM** (Non-Sucking Service Manager):

1. Download NSSM from [nssm.cc/download](https://nssm.cc/download)
2. Extract `nssm.exe` to `C:\QuoteSnap\`
3. Open an **Administrator** Command Prompt and run:

```bat
nssm install QuoteSnapBackend
```

4. In the NSSM dialog, configure:
   - **Path**: `C:\QuoteSnap\venv\Scripts\python.exe`
   - **Startup directory**: `C:\QuoteSnap\`
   - **Arguments**: `run.py`

5. Click "Install service", then start it:

```bat
nssm start QuoteSnapBackend
```

To view logs or manage the service:
```bat
nssm edit QuoteSnapBackend     # Edit config
nssm restart QuoteSnapBackend  # Restart
nssm stop QuoteSnapBackend     # Stop
```

---

## Project Structure

```
quotationv3/
├── run.py                  ← Windows entry point (Waitress WSGI)
├── backend_app.py          ← Flask application
├── requirements.txt        ← Python dependencies
├── .env                    ← Environment variables
├── credentials.json        ← Gmail OAuth credentials
├── setup.bat               ← First-time setup (CMD)
├── setup.ps1               ← First-time setup (PowerShell)
├── start_server.bat        ← Start server (CMD)
├── start_server.ps1        ← Start server (PowerShell)
├── database/               ← DuckDB database files
├── uploads/                ← Uploaded PDF/email attachments
├── generated/              ← Generated Excel quotations
├── logs/                   ← Application logs
├── tokens/                 ← Gmail OAuth tokens
└── sample/                 ← Excel template files
```

---

## Verify it Works

After starting, open your browser or use curl:

```
http://localhost:8000/api/health
```

Expected response:
```json
{"status": "ok"}
```

Then navigate to your frontend — login should work normally.

---

## Troubleshooting

| Problem | Solution |
|---|---|
| `pytesseract` errors | Make sure Tesseract is installed and added to PATH |
| `pdf2image` errors | Make sure Poppler `bin` folder is added to PATH |
| Port already in use | Change `SERVER_PORT` in `.env` |
| `venv not found` | Run `setup.bat` first |
| PowerShell ExecutionPolicy error | Run: `Set-ExecutionPolicy RemoteSigned -Scope CurrentUser` |
