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
        all_runs = self.repo.list_runs_for_pipeline(pipeline.id)
        run = next((r for r in all_runs if r.id == run_id), None)
        if not run:
            return
        self.repo.create_block_runs(run, pipeline.blocks)
        in_degrees: Dict[UUID, int] = {b.id: 0 for b in pipeline.blocks}
        for e in pipeline.edges:
            in_degrees[e.target_block_id] += 1
        for block in pipeline.blocks:
            if in_degrees[block.id] == 0:
                self._enqueue_block(run_id, pipeline, block.id)

    def on_block_completed(self, run_id: UUID, pipeline: Pipeline, completed_block_id: UUID):
        deps: Dict[UUID, set[UUID]] = {b.id: set() for b in pipeline.blocks}
        for e in pipeline.edges:
            deps[e.target_block_id].add(e.source_block_id)
        for block in pipeline.blocks:
            if block.id == completed_block_id:
                continue
            dep_statuses = [self.repo.get_block_run_by_block(run_id, dep_id) for dep_id in deps.get(block.id, set())]
            if dep_statuses and all(br and br.status == BlockRunStatus.COMPLETED for br in dep_statuses):
                br = self.repo.get_block_run_by_block(run_id, block.id)
                if br and br.status == BlockRunStatus.PENDING:
                    self._enqueue_block(run_id, pipeline, block.id)
        
        self._check_pipeline_status(run_id, pipeline)

    def on_block_failed(self, run_id: UUID, pipeline: Pipeline, failed_block_id: UUID):
        """Handle block failure: prevent dependents from running, check if pipeline should fail"""
        dependents: List[UUID] = []
        for e in pipeline.edges:
            if e.source_block_id == failed_block_id:
                dependents.append(e.target_block_id)
        
        self._check_pipeline_status(run_id, pipeline)

    def _check_pipeline_status(self, run_id: UUID, pipeline: Pipeline):
        """Check if pipeline should be marked as completed or failed"""
        block_runs = self.repo.get_all_block_runs_for_run(run_id)
        block_statuses = {br.block_id: br.status for br in block_runs}
        
        deps: Dict[UUID, set[UUID]] = {b.id: set() for b in pipeline.blocks}
        for e in pipeline.edges:
            deps[e.target_block_id].add(e.source_block_id)
        
        all_completed = all(status == BlockRunStatus.COMPLETED for status in block_statuses.values())
        any_failed = any(status == BlockRunStatus.FAILED for status in block_statuses.values())
        
        if all_completed:
            self.repo.update_pipeline_run_status(run_id, PipelineStatus.COMPLETED)
            return
        
        can_progress = False
        for block in pipeline.blocks:
            status = block_statuses.get(block.id)
            if status in (BlockRunStatus.COMPLETED, BlockRunStatus.FAILED, BlockRunStatus.RUNNING):
                continue
            
            block_deps = deps.get(block.id, set())
            if not block_deps:
                can_progress = True
                break
            if all(block_statuses.get(dep_id) == BlockRunStatus.COMPLETED for dep_id in block_deps):
                can_progress = True
                break
        
        if not can_progress and any_failed:
            self.repo.update_pipeline_run_status(run_id, PipelineStatus.FAILED)

    def _enqueue_block(self, run_id: UUID, pipeline: Pipeline, block_id: UUID):
        payload = {
            "pipeline_run_id": str(run_id),
            "pipeline": pipeline.model_dump(),
            "block_id": str(block_id),
        }
        self.producer.produce('block-tasks', key=str(run_id), value=json.dumps(payload))
        self.producer.poll(0)