# Use Python 3.12 slim image
FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy project files
COPY . .

# Install the package and its dependencies
RUN pip install --no-cache-dir . prometheus_client

# Environment variables
ENV PYTHONUNBUFFERED=1
ENV PSPF_DATA_DIR=/data

# Create data directory
RUN mkdir -p /data

# Expose default port
EXPOSE 8000

# Default command (expected to be overridden by docker-compose or run arguments)
CMD ["uvicorn", "examples.inventory_app.api:app", "--host", "0.0.0.0", "--port", "8000"]
