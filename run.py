"""
run.py — Windows-compatible entry point for QuoteSnap backend.

Replaces gunicorn (Linux-only) with Waitress, a pure-Python WSGI server
that runs natively on Windows.

Usage:
    python run.py
"""
import sys
import io

# Force stdout and stderr to UTF-8 to prevent UnicodeEncodeError on Windows
try:
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    elif hasattr(sys.stdout, 'buffer'):
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8')
    elif hasattr(sys.stderr, 'buffer'):
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
except Exception:
    pass

import threading
import os
from dotenv import load_dotenv

# Load environment variables before creating the app
load_dotenv()

from backend_app import create_flask_app, start_company_gmail_monitoring

# Create the Flask app
app = create_flask_app()

if __name__ == '__main__':
    # Start Gmail monitoring in a background daemon thread
    monitor_thread = threading.Thread(
        target=start_company_gmail_monitoring,
        daemon=True,
        name="gmail-monitor"
    )
    monitor_thread.start()

    # Read config from env (with sensible defaults)
    host = os.getenv("SERVER_HOST", "0.0.0.0")
    port = int(os.getenv("SERVER_PORT", "8000"))
    threads = int(os.getenv("SERVER_THREADS", "4"))

    print(f"[START] Starting QuoteSnap backend on http://{host}:{port}")
    print(f"   Threads: {threads}")
    print(f"   Press Ctrl+C to stop.\n")

    from waitress import serve
    serve(app, host=host, port=port, threads=threads)
