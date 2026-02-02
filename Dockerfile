FROM python:3.11

# Set working directory
WORKDIR /app

# Install system dependencies (Tesseract, Poppler, etc.)
RUN apt-get update && apt-get install -y \
    tesseract-ocr \
    poppler-utils \
    libgl1 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# 1. FIX: Upgrade pip and increase timeout to prevent ReadTimeoutError
RUN pip install --upgrade pip && \
    pip install --default-timeout=1000 --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p database tokens generated uploads logs

# Set environment variables
ENV FLASK_DEBUG=false
ENV PYTHONUNBUFFERED=1

# Expose Flask port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD python -c "import requests; requests.get('http://localhost:8000/api/health')" || exit 1

# 2. FIX: Use 'gevent' worker class for Flask-SocketIO support
# Standard sync workers (default) will block WebSockets.
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--worker-class", "gevent", "--workers", "1", "--timeout", "120", "--access-logfile", "-", "--error-logfile", "-", "backend_app:app"]