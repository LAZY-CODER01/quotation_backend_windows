FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for OCR and PDF processing
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr \
    poppler-utils \
    libgl1 \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# --- PRE-LOAD MODEL STEP ---
# This ensures the model is baked into the image, not downloaded at runtime
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('BAAI/bge-base-en-v1.5')"

COPY . .

ENV PYTHONUNBUFFERED=1

# GUNICORN TUNING:
# For Eventlet/SocketIO, workers should usually be 1 unless using a load balancer.
# We increase the timeout to 300 to account for heavy PDF parsing + Embedding.
CMD ["gunicorn", "backend_app:app", \
     "--worker-class", "eventlet", \
     "--workers", "1", \
     "--bind", "0.0.0.0:8000", \
     "--timeout", "1000", \
     "--keep-alive", "5"]