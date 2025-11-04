from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import os
from typing import List
from uuid import UUID

from .models import Pipeline, PipelineRun
from .streaming import KafkaStreamingBackend
from .repository import SQLiteRepository
from .orchestrator import Orchestrator


app = FastAPI(title="Charcentric Pipeline API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

db_path = os.getenv('DATABASE_URL', '').replace('sqlite:///', '') or "/app/data/charcentric.db"
kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

repo = SQLiteRepository(db_path)
streaming_backend = KafkaStreamingBackend(kafka_bootstrap)
orchestrator = Orchestrator(repo, kafka_bootstrap)


@app.get("/")
async def root():
    return {"message": "Charcentric Pipeline API"}


@app.post("/pipelines", response_model=Pipeline)
async def create_pipeline(pipeline: Pipeline):
    try:
        orchestrator.save_pipeline(pipeline)
        return pipeline
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/pipelines", response_model=List[Pipeline])
async def get_pipelines():
    return orchestrator.list_pipelines()


@app.post("/pipelines/{pipeline_id}/runs", response_model=PipelineRun)
async def execute_pipeline(pipeline_id: UUID, background_tasks: BackgroundTasks):
    pipeline = orchestrator.get_pipeline(pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    run = orchestrator.create_pipeline_run(pipeline_id)
    background_tasks.add_task(orchestrator.execute_pipeline, run.id, pipeline)
    return run


@app.get("/pipelines/{pipeline_id}/runs", response_model=List[PipelineRun])
async def get_pipeline_runs(pipeline_id: UUID):
    return repo.list_runs_for_pipeline(pipeline_id)


@app.get("/logs/{pipeline_run_id}")
async def stream_logs(pipeline_run_id: UUID):
    def generate():
        for log_entry in streaming_backend.consume_logs(str(pipeline_run_id)):
            yield f"data: {log_entry.json()}\n\n"
    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)