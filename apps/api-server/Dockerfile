FROM python:3.11-slim

WORKDIR /app

# Install system dependencies required by asyncpg
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies first for better layer caching
COPY apps/api-server/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared packages
COPY packages/shared/ /app/packages/shared/

# Copy api-server application code
COPY apps/api-server/ /app/apps/api-server/

# Set PYTHONPATH so imports resolve correctly
ENV PYTHONPATH="/app/apps/api-server:/app/packages"

CMD ["sh", "-c", "uvicorn api_server.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
