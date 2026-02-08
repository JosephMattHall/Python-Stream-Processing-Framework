FROM python:3.12-slim

WORKDIR /app

# Install dependencies including build tools if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY . .

# Install the package in editable mode or just install requirements
RUN pip install --no-cache-dir .
# Ensure we have runtime dependencies for the demo if not in package
RUN pip install valkey uvicorn fastapi

CMD ["python", "examples/valkey_demo.py"]
