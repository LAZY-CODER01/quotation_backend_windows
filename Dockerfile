FROM python:3.11

WORKDIR /app

RUN apt-get update && apt-get install -y \
    tesseract-ocr \
    poppler-utils \
    libgl1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1

# IMPORTANT: Azure provides PORT
# CMD ["sh", "-c", "gunicorn backend_app:app \
#  --worker-class eventlet \
#  --workers 1 \
#  --bind 0.0.0.0:${PORT} \
#  --timeout 120"]

# IMPORTANT: Azure provides PORT
CMD ["sh", "-c", "gunicorn backend_app:app \
 --worker-class eventlet \
 --workers 5 \
 --worker-connections 1000 \
 --bind 0.0.0.0:8000 \
 --keep-alive 5 \
 --timeout 60"]
