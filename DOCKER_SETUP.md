# Docker Setup Guide for QuoteSnap Backend

## Prerequisites

1. Docker and Docker Compose installed
2. `credentials.json` file (Gmail OAuth credentials) in the project root
3. Environment variables configured (see below)

## Quick Start

### 1. Create `.env` file (optional, or use environment variables)

```env
SECRET_KEY=your-secret-key-here
OPENAI_API_KEY=your-openai-api-key
GMAIL_CREDENTIALS_FILE=credentials.json
OAUTH_REDIRECT_URI=http://localhost:8000/api/auth/callback
FRONTEND_URL=http://localhost:5173
CORS_ORIGINS=http://localhost:5173,http://localhost:3000
EMAIL_CHECK_INTERVAL=300
```

### 2. Build and run with Docker Compose

```bash
# Build and start the container
docker-compose up -d

# View logs
docker-compose logs -f

# Stop the container
docker-compose down
```

### 3. Or build and run with Docker directly

```bash
# Build the image
docker build -t quotesnap-backend .

# Run the container
docker run -d \
  --name quotesnap-backend \
  -p 8000:8000 \
  -e OPENAI_API_KEY=your-api-key \
  -e SECRET_KEY=your-secret-key \
  -v $(pwd)/database:/app/database \
  -v $(pwd)/tokens:/app/tokens \
  -v $(pwd)/generated:/app/generated \
  -v $(pwd)/credentials.json:/app/credentials.json:ro \
  quotesnap-backend
```

## Important Notes

### Volume Mounts

The following directories are mounted as volumes to persist data:

- `./database` - DuckDB database files
- `./tokens` - Gmail OAuth tokens
- `./generated` - Generated Excel quotation files
- `./uploads` - Uploaded files
- `./logs` - Application logs

### Environment Variables

Required environment variables:

- `OPENAI_API_KEY` - Your OpenAI API key (required)
- `SECRET_KEY` - Flask secret key for sessions

Optional environment variables (with defaults):

- `EMAIL_CHECK_INTERVAL` - Email check interval in seconds (default: 300)
- `OAUTH_REDIRECT_URI` - OAuth callback URL
- `FRONTEND_URL` - Frontend URL for redirects
- `CORS_ORIGINS` - Allowed CORS origins

### Email Monitoring

Email monitoring starts automatically when:

1. A user authenticates via `/api/auth/login`
2. Existing tokens are found in the `tokens/` directory on startup

The monitoring runs in background threads and will:

- Check for new emails every `EMAIL_CHECK_INTERVAL` seconds
- Process emails and extract quotation data
- Save to DuckDB database
- Generate Excel files when requested

### Troubleshooting

#### Email monitoring not working

1. **Check if tokens exist:**
   ```bash
   docker exec quotesnap-backend ls -la /app/tokens/
   ```

2. **Check logs:**
   ```bash
   docker-compose logs -f | grep -i "monitoring\|email\|gmail"
   ```

3. **Verify Gmail credentials:**
   ```bash
   docker exec quotesnap-backend ls -la /app/credentials.json
   ```

#### Database issues

1. **Check database directory:**
   ```bash
   docker exec quotesnap-backend ls -la /app/database/
   ```

2. **Verify database file permissions:**
   ```bash
   docker exec quotesnap-backend chmod 666 /app/database/*.duckdb
   ```

#### Excel generation not working

1. **Check template file exists:**
   ```bash
   docker exec quotesnap-backend ls -la /app/sample/QuotationFormat.xlsx
   ```

2. **Check generated directory:**
   ```bash
   docker exec quotesnap-backend ls -la /app/generated/
   ```

#### Container won't start

1. **Check logs:**
   ```bash
   docker-compose logs
   ```

2. **Verify environment variables:**
   ```bash
   docker exec quotesnap-backend env | grep -E "OPENAI|SECRET|GMAIL"
   ```

3. **Check health endpoint:**
   ```bash
   curl http://localhost:8000/api/health
   ```

### Development vs Production

**Development:**
- Use `docker-compose up` (runs in foreground, shows logs)
- Set `FLASK_DEBUG=true` in environment

**Production:**
- Use `docker-compose up -d` (runs in background)
- Set `FLASK_DEBUG=false`
- Use proper `SECRET_KEY`
- Configure proper `CORS_ORIGINS` and `FRONTEND_URL`
- Use HTTPS and set `SESSION_COOKIE_SECURE=true`

### Updating the Application

```bash
# Rebuild the image
docker-compose build

# Restart with new image
docker-compose up -d

# Or force recreate
docker-compose up -d --force-recreate
```

### Accessing the Container

```bash
# Open a shell in the container
docker exec -it quotesnap-backend /bin/bash

# View Python processes
docker exec quotesnap-backend ps aux

# Check Python packages
docker exec quotesnap-backend pip list
```

