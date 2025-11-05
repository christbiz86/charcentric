import pytest
import os
import tempfile
from uuid import uuid4
from charcentric.repository import SQLiteRepository
from charcentric.models import (
    Pipeline, Block, Edge, PipelineRun, BlockRun, Artifact,
    PipelineStatus, BlockRunStatus, BlockType
)


class TestSQLiteRepository:
    def setup_method(self):
        """Set up test fixtures"""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.repo = SQLiteRepository(self.temp_db.name)
    
    def teardown_method(self):
        """Clean up test fixtures"""
        if os.path.exists(self.temp_db.name):
            os.unlink(self.temp_db.name)
    
    def test_save_and_get_pipeline(self):
        """Test saving and retrieving a pipeline"""
        pipeline_id = uuid4()
        block = Block(pipeline_id=pipeline_id, name="Test Block", type=BlockType.CSV_READER)
        edge = Edge(pipeline_id=pipeline_id, source_block_id=block.id, target_block_id=uuid4())
        
        pipeline = Pipeline(
            id=pipeline_id,
            name="Test Pipeline",
            description="Test",
            blocks=[block],
            edges=[edge]
        )
        
        saved_id = self.repo.save_pipeline(pipeline)
        assert saved_id == pipeline_id
        
        retrieved = self.repo.get_pipeline(pipeline_id)
        assert retrieved is not None
        assert retrieved.name == "Test Pipeline"
        assert len(retrieved.blocks) == 1
        assert len(retrieved.edges) == 1
    
    def test_create_pipeline_run(self):
        """Test creating a pipeline run"""
        pipeline_id = uuid4()
        run = self.repo.create_pipeline_run(pipeline_id)
        
        assert run.pipeline_id == pipeline_id
        assert run.status == PipelineStatus.QUEUED
        
        runs = self.repo.list_runs_for_pipeline(pipeline_id)
        assert len(runs) == 1
        assert runs[0].id == run.id
    
    def test_update_pipeline_run_status(self):
        """Test updating pipeline run status"""
        pipeline_id = uuid4()
        run = self.repo.create_pipeline_run(pipeline_id)
        
        self.repo.update_pipeline_run_status(run.id, PipelineStatus.RUNNING)
        runs = self.repo.list_runs_for_pipeline(pipeline_id)
        assert runs[0].status == PipelineStatus.RUNNING
        assert runs[0].started_at is not None
        
        self.repo.update_pipeline_run_status(run.id, PipelineStatus.COMPLETED)
        runs = self.repo.list_runs_for_pipeline(pipeline_id)
        assert runs[0].status == PipelineStatus.COMPLETED
        assert runs[0].completed_at is not None
    
    def test_create_block_runs(self):
        """Test creating block runs for a pipeline run"""
        pipeline_id = uuid4()
        run = self.repo.create_pipeline_run(pipeline_id)
        
        blocks = [
            Block(pipeline_id=pipeline_id, name="Block 1", type=BlockType.CSV_READER),
            Block(pipeline_id=pipeline_id, name="Block 2", type=BlockType.LLM_SENTIMENT)
        ]
        
        block_runs = self.repo.create_block_runs(run, blocks)
        
        assert len(block_runs) == 2
        assert all(br.status == BlockRunStatus.PENDING for br in block_runs)
        assert all(br.pipeline_run_id == run.id for br in block_runs)
    
    def test_update_block_run_status(self):
        """Test updating block run status"""
        pipeline_id = uuid4()
        run = self.repo.create_pipeline_run(pipeline_id)
        block = Block(pipeline_id=pipeline_id, name="Test Block", type=BlockType.CSV_READER)
        block_runs = self.repo.create_block_runs(run, [block])
        block_run = block_runs[0]
        
        self.repo.update_block_run_status(block_run.id, BlockRunStatus.RUNNING)
        retrieved = self.repo.get_block_run_by_block(run.id, block.id)
        assert retrieved.status == BlockRunStatus.RUNNING
        assert retrieved.started_at is not None
        
        self.repo.update_block_run_status(block_run.id, BlockRunStatus.COMPLETED)
        retrieved = self.repo.get_block_run_by_block(run.id, block.id)
        assert retrieved.status == BlockRunStatus.COMPLETED
        assert retrieved.completed_at is not None
        
        self.repo.update_block_run_status(block_run.id, BlockRunStatus.FAILED, error_message="Test error")
        retrieved = self.repo.get_block_run_by_block(run.id, block.id)
        assert retrieved.status == BlockRunStatus.FAILED
        assert retrieved.error_message == "Test error"
    
    def test_save_artifacts(self):
        """Test saving artifacts"""
        pipeline_id = uuid4()
        run = self.repo.create_pipeline_run(pipeline_id)
        block = Block(pipeline_id=pipeline_id, name="Test Block", type=BlockType.CSV_READER)
        block_runs = self.repo.create_block_runs(run, [block])
        block_run = block_runs[0]
        
        artifacts = [
            Artifact(
                block_run_id=block_run.id,
                name="test_artifact",
                type="test_type",
                data={"key": "value"}
            )
        ]
        
        self.repo.save_artifacts(artifacts)