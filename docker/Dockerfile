# Dockerfile
FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    MPLCONFIGDIR=/tmp/matplotlib

# Install system dependencies for building Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Install pinned Python dependencies before copying source so rebuilds can reuse this layer
COPY requirements.txt .
RUN pip install --no-cache-dir --requirement requirements.txt

# Copy application and benchmark configuration
COPY src/app.py .
COPY src/XYZProcessor.py .
COPY src/benchmark_runner.py .
COPY src/db_backends ./db_backends
COPY src/workloads ./workloads
COPY benchmark_config.yaml .

# Create necessary directories
RUN mkdir -p /data/input /data/output /input /results /tmp/matplotlib

# Set permissions
RUN chmod +x /app/app.py

# Run the application
CMD ["python3", "/app/app.py"]
