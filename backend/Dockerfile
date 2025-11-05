FROM python:3.11-slim

WORKDIR /app

# System deps for confluent-kafka
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
 && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/

# Ensure package path
ENV PYTHONPATH=/app/src

# Create data and logs directories
RUN mkdir -p /data /app/logs

# Expose port
EXPOSE 8000

# Default run command (overridden by docker-compose services)
CMD ["uvicorn", "charcentric.main:app", "--host", "0.0.0.0", "--port", "8000"]