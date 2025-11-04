import json
import os
from typing import Dict, List
from uuid import UUID

try:
    from confluent_kafka import Producer
except Exception:
    Producer = None

from .dag import DAGEngine
from .models import Pipeline, PipelineRun, BlockRunStatus, PipelineStatus
from .repository import SQLiteRepository


class Orchestrator:
    def __init__(self, repository: SQLiteRepository, kafka_bootstrap: str):
        self.dag_engine = DAGEngine()
        self.repo = repository
        if Producer is None:
            raise RuntimeError("confluent-kafka not installed")
        self.producer = Producer({'bootstrap.servers': kafka_bootstrap, 'client.id': 'charcentric-orchestrator'})

    # Pipeline CRUD
    def save_pipeline(self, pipeline: Pipeline) -> UUID:
        if not self.dag_engine.validate_dag(pipeline.blocks, pipeline.edges):
            raise ValueError("Invalid DAG: contains cycles")
        return self.repo.save_pipeline(pipeline)

    def list_pipelines(self) -> List[Pipeline]:
        return self.repo.list_pipelines()

    def get_pipeline(self, pipeline_id: UUID) -> Pipeline | None:
        return self.repo.get_pipeline(pipeline_id)

    # Execution
    def create_pipeline_run(self, pipeline_id: UUID) -> PipelineRun:
        return self.repo.create_pipeline_run(pipeline_id)

    def execute_pipeline(self, run_id: UUID, pipeline: Pipeline):
        self.repo.update_pipeline_run_status(run_id, PipelineStatus.RUNNING)
        # materialize block runs
        self.repo.create_block_runs(self.repo.list_runs_for_pipeline(pipeline.id)[0], pipeline.blocks)  # ensure rows
        # initial runnable blocks: no incoming edges
        in_degrees: Dict[UUID, int] = {b.id: 0 for b in pipeline.blocks}
        for e in pipeline.edges:
            in_degrees[e.target_block_id] += 1
        for block in pipeline.blocks:
            if in_degrees[block.id] == 0:
                self._enqueue_block(run_id, pipeline, block.id)

    def on_block_completed(self, run_id: UUID, pipeline: Pipeline, completed_block_id: UUID):
        # find blocks whose dependencies are all completed
        # Build dependency map
        deps: Dict[UUID, set[UUID]] = {b.id: set() for b in pipeline.blocks}
        for e in pipeline.edges:
            deps[e.target_block_id].add(e.source_block_id)
        # query block run statuses
        # naive approach: for all blocks, check if deps completed via repo.get_block_run_by_block
        for block in pipeline.blocks:
            if block.id == completed_block_id:
                continue
            if all(self.repo.get_block_run_by_block(run_id, dep_id).status == BlockRunStatus.COMPLETED for dep_id in deps.get(block.id, set())):
                br = self.repo.get_block_run_by_block(run_id, block.id)
                if br and br.status == BlockRunStatus.PENDING:
                    self._enqueue_block(run_id, pipeline, block.id)

    def _enqueue_block(self, run_id: UUID, pipeline: Pipeline, block_id: UUID):
        payload = {
            "pipeline_run_id": str(run_id),
            "pipeline": pipeline.model_dump(),
            "block_id": str(block_id),
        }
        self.producer.produce('block-tasks', key=str(run_id), value=json.dumps(payload))
        self.producer.poll(0)