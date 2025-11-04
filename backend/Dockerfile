FROM python:3.11-slim

WORKDIR /app

# Install Poetry
RUN pip install poetry

# Copy poetry files
COPY pyproject.toml poetry.lock* ./

# Install dependencies
RUN poetry config virtualenvs.create false && \
    poetry install --no-dev --no-interaction --no-ansi

# Copy source code
COPY src/ ./src/

# Ensure package path
ENV PYTHONPATH=/app/src

# Create data directory
RUN mkdir -p /data

# Expose port
EXPOSE 8000

# Default run command (overridden by docker-compose services)
CMD ["uvicorn", "charcentric.main:app", "--host", "0.0.0.0", "--port", "8000"]