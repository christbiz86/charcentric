# Charcentric Assessment Test

A containerized system for building and executing data pipelines with real-time log streaming and parallel DAG execution.

## Architecture

The system consists of:
- **API Backend**: FastAPI service for pipeline management
- **Orchestrator**: DAG scheduler and execution coordinator
- **Workers**: Two worker instances for parallel step execution
- **Kafka**: Streaming backend for real-time logs
- **SQLite**: Database for pipeline metadata
- **React UI**: Minimal interface for live log viewing

## Quick Start

1. **Set up environment variables:**
   ```bash
   cp .env.example .env

2. **Start the system:**
   ```bash
   docker-compose up

3. **Access the services:**
    API: http://localhost:8000
    Frontend: http://localhost:3000
    API Docs: http://localhost:8000/docs

4. **Run tests with:**
   ```bash
   cd backend
   poetry run pytest