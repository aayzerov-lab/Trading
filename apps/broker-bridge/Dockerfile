FROM python:3.11-slim

WORKDIR /app

# Install system dependencies required by asyncpg
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies first for better layer caching
COPY apps/broker-bridge/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared packages
COPY packages/shared/ /app/packages/shared/

# Copy broker-bridge application code
COPY apps/broker-bridge/ /app/apps/broker-bridge/

# Set PYTHONPATH so imports resolve correctly
ENV PYTHONPATH="/app/apps/broker-bridge:/app/packages"

CMD ["python", "-m", "broker_bridge.main"]
