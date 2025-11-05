# Charcentric Pipeline System

## Architecture

The system consists of:
- **API Backend**: FastAPI service for pipeline management
- **Orchestrator**: DAG scheduler and execution coordinator
- **Workers**: Two worker instances for parallel step execution
- **Kafka**: Streaming backend for real-time logs
- **SQLite**: Database for pipeline metadata
- **React UI**: Minimal interface for live log viewing

## Setup & Run

### Prerequisites
- Docker and Docker Compose installed
- (Optional) Gemini API key for LLM features (system works with mock LLM if not provided)

### Quick Start

1. **Create environment file:**
   ```bash
   # Copy the example file and add your API key
   cp .env.example .env
   # Edit .env and add your Gemini API key (optional)
   # If you don't have a Gemini API key, leave it empty - the system will use a mock LLM provider
   ```
   
   The `.env` file is gitignored for security. You can start the system without an API key - it will use a mock LLM provider.

2. **Start all services:**
   ```bash
   docker-compose up
   ```
   This will start all services in the background. Use `docker-compose up -d` for detached mode.

3. **Access the services:**
   - **API**: http://localhost:8000
   - **API Docs (Swagger)**: http://localhost:8000/docs
   - **Frontend UI**: http://localhost:3000
   - **Kafka**: localhost:9092
   - **Zookeeper**: localhost:2181

4. **Stop all services:**
   ```bash
   docker-compose down
   ```

5. **View logs:**
   ```bash
   docker-compose logs -f [service-name]
   # Examples:
   docker-compose logs -f api
   docker-compose logs -f orchestrator
   docker-compose logs -f worker-1
   ```

## Service Overview

### API Backend (`api`)
- **Service**: FastAPI REST API
- **Port**: 8000
- **Purpose**: Pipeline CRUD operations, pipeline execution, and SSE log streaming
- **Endpoints**:
  - `POST /pipelines` - Create a new pipeline
  - `GET /pipelines` - List all pipelines
  - `POST /pipelines/{pipeline_id}/runs` - Execute a pipeline
  - `GET /pipelines/{pipeline_id}/runs` - List runs for a pipeline
  - `GET /logs/{pipeline_run_id}` - Stream logs via SSE
- **Dependencies**: Kafka, SQLite database

### Orchestrator (`orchestrator`)
- **Service**: DAG execution coordinator
- **Purpose**: 
  - Resolves DAG dependencies
  - Enqueues runnable blocks to Kafka
  - Monitors block completion/failure
  - Updates pipeline run status
  - Prevents dependents from running when dependencies fail
- **Dependencies**: Kafka, SQLite database

### Workers (`worker-1`, `worker-2`)
- **Service**: Block execution workers
- **Purpose**:
  - Consume block tasks from Kafka
  - Execute steps (CSV reader, LLM sentiment, LLM toxicity, file writer)
  - Retry failed blocks with exponential backoff (3 attempts)
  - Emit execution logs and completion/failure events
- **Features**:
  - Retry logic: 3 attempts with exponential backoff (1s, 2s, 4s delays)
  - Parallel execution across 2 worker instances
- **Dependencies**: Kafka, SQLite database, LLM API (optional)

### Frontend (`frontend`)
- **Service**: React frontend
- **Port**: 3000
- **Purpose**: Live log stream viewer for all pipeline runs
- **Features**:
  - Lists all pipelines and their runs
  - Real-time log streaming via Server-Sent Events (SSE)
  - Displays logs from all pipeline runs
- **Dependencies**: API backend

### Kafka (`kafka`)
- **Service**: Apache Kafka message broker
- **Port**: 9092
- **Purpose**: 
  - Streams log entries to UI
  - Distributes block tasks to workers
  - Routes orchestration events
- **Topics**:
  - `pipeline-logs`: Log entries from all pipeline runs
  - `block-tasks`: Block execution tasks for workers
  - `orchestrator-events`: Block completion/failure events

### Zookeeper (`zookeeper`)
- **Service**: Apache Zookeeper
- **Port**: 2181
- **Purpose**: Coordination service for Kafka

### Database (SQLite)
- **Service**: SQLite database
- **Storage**: Persistent volume `db_data`
- **Purpose**: Stores pipeline definitions, runs, block runs, and artifacts
- **Location**: `/app/data/charcentric.db` (inside containers)

## Environment Variables

### Required for Full Functionality

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `GEMINI_API_KEY` | Google Gemini API key for LLM features | None | No (uses mock if not set) |

### Service-Specific Variables

#### API Service
- `DATABASE_URL`: SQLite database path (default: `sqlite:///./charcentric.db`)
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker address (default: `kafka:9092`)
- `GEMINI_API_KEY`: Gemini API key for LLM operations

#### Orchestrator Service
- `DATABASE_URL`: SQLite database path (default: `sqlite:///./charcentric.db`)
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker address (default: `kafka:9092`)

#### Worker Services
- `DATABASE_URL`: SQLite database path (default: `sqlite:///./charcentric.db`)
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker address (default: `kafka:9092`)
- `GEMINI_API_KEY`: Gemini API key for LLM operations
- `WORKER_ID`: Unique worker identifier (default: `worker`)
- `LOG_FILE_PATH`: File path to write structured JSON logs (e.g., `/app/logs/worker.log`)

#### UI Service
- `VITE_API_URL`: API backend URL (default: `http://localhost:8000`)

### Volumes

The system uses the following volumes:

1. **`./data:/data`** (bind mount)
   - Purpose: Shared data directory for CSV files and outputs
   - Location: Project root `data/` directory
   - Used by: API, Orchestrator, Workers

2. **`db_data`** (named volume)
   - Purpose: Persistent SQLite database storage
   - Location: Docker-managed volume
   - Used by: API, Orchestrator, Workers

### Example `.env` File

A `.env.example` file is provided in the project root. Copy it to create your `.env`:

```bash
cp .env.example .env
```

Then edit `.env` and add your values:

```bash
# LLM API Configuration (optional - system uses mock if not provided)
GEMINI_API_KEY=your_gemini_api_key_here

# Optional: Override defaults
# DATABASE_URL=sqlite:///./charcentric.db
# KAFKA_BOOTSTRAP_SERVERS=kafka:9092
# VITE_API_URL=http://localhost:8000
```

**Note**: The `.env` file is gitignored for security. Never commit your actual API keys.

## Responsible AI

This system includes LLM-powered features for sentiment analysis and toxicity detection. We are committed to responsible AI practices:

### Bias & Fairness
- **Acknowledgment**: LLM-based classification may reflect biases present in training data. Sentiment and toxicity labels should be reviewed with human oversight.
- **Mitigation**: The system logs all LLM responses and classification results for audit purposes. Review outputs regularly for potential bias.

### Transparency
- **Logging**: All LLM API calls, inputs, and outputs are logged to the streaming backend and stored in the database.
- **Traceability**: Each pipeline run, block execution, and artifact is traceable through the database with timestamps and error messages.

### User Control
- **Configuration**: Users control which pipelines execute and what data is processed.
- **Failure Handling**: Failed blocks do not proceed to dependent blocks, preventing cascading failures.
- **Manual Review**: All classification outputs should be reviewed before use in production decisions.

### Privacy & Data Protection
- **Data Storage**: Pipeline inputs and outputs are stored in the local SQLite database and file system.
- **No External Sharing**: By default, data remains within the local infrastructure unless explicitly configured otherwise.
- **API Keys**: Store API keys securely in environment variables, never in code.

### Recommendations
1. **Review Classifications**: Manually review a sample of LLM classifications to ensure accuracy.
2. **Monitor Logs**: Regularly review logs for unexpected patterns or biases.
3. **Test with Diverse Data**: Test pipelines with diverse, representative data sets.
4. **Set Appropriate Thresholds**: Use classification scores as signals, not absolute decisions.
5. **Document Use Cases**: Document intended use cases and limitations of the system.

## Testing Instructions

### Manual Testing

1. **Start the system:**
   ```bash
   docker-compose up -d
   ```

2. **Wait for services to be ready:**
   ```bash
   # Check service health
   docker-compose ps
   # Check logs
   docker-compose logs api
   ```

3. **Create a test pipeline:**
   ```bash
   curl -X POST http://localhost:8000/pipelines \
     -H "Content-Type: application/json" \
     -d '{
       "name": "Test Pipeline",
       "description": "Test sentiment analysis",
       "blocks": [
         {
           "pipeline_id": "00000000-0000-0000-0000-000000000000",
           "name": "CSV Reader",
           "type": "csv_reader",
           "config": {"file_path": "/data/sample.csv"}
         },
         {
           "pipeline_id": "00000000-0000-0000-0000-000000000000",
           "name": "Sentiment Analysis",
           "type": "llm_sentiment",
           "config": {}
         }
       ],
       "edges": [
         {
           "pipeline_id": "00000000-0000-0000-0000-000000000000",
           "source_block_id": "<block-1-id>",
           "target_block_id": "<block-2-id>"
         }
       ]
     }'
   ```

4. **Execute the pipeline:**
   ```bash
   curl -X POST http://localhost:8000/pipelines/{pipeline_id}/runs
   ```

5. **View logs:**
   - Open http://localhost:3000 in browser
   - Or stream logs via SSE:
     ```bash
     curl http://localhost:8000/logs/{pipeline_run_id}
     ```

6. **Check pipeline status:**
   ```bash
   curl http://localhost:8000/pipelines/{pipeline_id}/runs
   ```

### Automated Testing

1. **Unit Tests**:
   ```bash
   cd backend
   # Install test dependencies
   pip install -r requirements-test.txt
   # Or if using poetry:
   poetry install
   
   # Run all tests
   pytest tests/
   
   # Run specific test file
   pytest tests/test_dag.py
   pytest tests/test_steps.py
   pytest tests/test_repository.py
   pytest tests/test_llm.py
   
   # Run end-to-end test
   pytest tests/test_e2e.py -v
   
   # Run with coverage
   pip install pytest-cov
   pytest tests/ --cov=charcentric --cov-report=html
   ```

2. **Integration Tests**:
   - Start services: `docker-compose up -d`
   - Run API tests against http://localhost:8000
   - Test DAG execution with various pipeline configurations
   - Verify failure handling by injecting errors

### Testing Failure Scenarios

1. **Block Failure:**
   - Create a pipeline with an invalid block configuration
   - Execute and verify:
     - Block is marked as FAILED
     - Dependent blocks are not executed
     - Pipeline is marked as FAILED when no progress possible

2. **Retry Logic:**
   - Monitor logs for retry attempts
   - Verify exponential backoff (1s, 2s, 4s delays)
   - Verify block fails after 3 attempts

3. **DAG Validation:**
   - Attempt to create a pipeline with cycles
   - Verify API returns 400 error

### Testing UI

1. **Access UI**: http://localhost:3000
2. **Verify**:
   - Pipelines are listed
   - Runs are displayed
   - Logs stream in real-time
   - Multiple pipeline runs can be viewed simultaneously

## Additional Resources

- **API Documentation**: http://localhost:8000/docs (Interactive Swagger UI)
- **Kafka Topics**: Use Kafka tools to inspect topics:
  ```bash
  docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
  ```
- **Database Access**: SQLite database is at `/app/data/charcentric.db` in containers

## Troubleshooting

### Services won't start
- Check Docker and Docker Compose versions
- Ensure ports 8000, 3000, 9092, 2181 are not in use
- Review logs: `docker-compose logs`

### Kafka connection errors
- Wait for Zookeeper to fully start before Kafka
- Check Kafka logs: `docker-compose logs kafka`

### Database errors
- Ensure volume permissions are correct
- Check database file exists: `docker-compose exec api ls -la /app/data/`

### LLM API errors
- Verify API key is set correctly
- Check worker logs for API errors
- System falls back to mock LLM if API key is missing
