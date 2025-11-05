import pytest
import os
import tempfile
import csv
import json
from uuid import uuid4
from charcentric.models import (
    Pipeline, Block, Edge, PipelineRun, BlockRunStatus, PipelineStatus,
    BlockType
)
from charcentric.repository import SQLiteRepository
from charcentric.orchestrator import Orchestrator
from charcentric.steps import StepExecutor
from charcentric.llm import MockLLMProvider
from charcentric.streaming import MockStreamingBackend


class TestE2EPipeline:
    """End-to-end test simulating a complete pipeline execution"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.repo = SQLiteRepository(self.temp_db.name)
        
        self.temp_dir = tempfile.mkdtemp()
        csv_file = os.path.join(self.temp_dir, "sample.csv")
        
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow(['id', 'text'])
            writer.writerow(['1', 'I love this product!'])
            writer.writerow(['2', 'This is terrible'])
            writer.writerow(['3', 'Neutral comment'])
        
        self.llm_provider = MockLLMProvider()
        self.executor = StepExecutor(self.llm_provider, data_dir=self.temp_dir)
        
        self.pipeline_id = uuid4()
        self.pipeline = self._create_sample_pipeline()
        
        self.repo.save_pipeline(self.pipeline)
    
    def teardown_method(self):
        """Clean up test fixtures"""
        if os.path.exists(self.temp_db.name):
            os.unlink(self.temp_db.name)
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def _create_sample_pipeline(self):
        """Create a sample pipeline: CSV Reader -> Sentiment Analysis -> File Writer"""
        blocks = [
            Block(
                pipeline_id=self.pipeline_id,
                name="CSV Reader",
                type=BlockType.CSV_READER,
                config={"file_path": os.path.join(self.temp_dir, "sample.csv"), "delimiter": ";"}
            ),
            Block(
                pipeline_id=self.pipeline_id,
                name="Sentiment Analysis",
                type=BlockType.LLM_SENTIMENT,
                config={}
            ),
            Block(
                pipeline_id=self.pipeline_id,
                name="File Writer",
                type=BlockType.FILE_WRITER,
                config={"output_type": "sentiment"}
            )
        ]
        
        edges = [
            Edge(
                pipeline_id=self.pipeline_id,
                source_block_id=blocks[0].id,
                target_block_id=blocks[1].id
            ),
            Edge(
                pipeline_id=self.pipeline_id,
                source_block_id=blocks[1].id,
                target_block_id=blocks[2].id
            )
        ]
        
        return Pipeline(
            id=self.pipeline_id,
            name="E2E Test Pipeline",
            description="End-to-end test pipeline",
            blocks=blocks,
            edges=edges
        )
    
    def test_e2e_pipeline_execution(self):
        """Test complete pipeline execution from start to finish"""
        run = self.repo.create_pipeline_run(self.pipeline_id)
        assert run.status == PipelineStatus.QUEUED
        
        block_runs = self.repo.create_block_runs(run, self.pipeline.blocks)
        assert len(block_runs) == 3
        assert all(br.status == BlockRunStatus.PENDING for br in block_runs)
        
        csv_block = self.pipeline.blocks[0]
        sentiment_block = self.pipeline.blocks[1]
        writer_block = self.pipeline.blocks[2]
        
        csv_block_run = next(br for br in block_runs if br.block_id == csv_block.id)
        sentiment_block_run = next(br for br in block_runs if br.block_id == sentiment_block.id)
        writer_block_run = next(br for br in block_runs if br.block_id == writer_block.id)
        
        self.repo.update_block_run_status(csv_block_run.id, BlockRunStatus.RUNNING)
        csv_artifacts = self.executor.execute_csv_reader(csv_block_run, csv_block.config)
        self.repo.save_artifacts(csv_artifacts)
        self.repo.update_block_run_status(csv_block_run.id, BlockRunStatus.COMPLETED)
        
        csv_block_run = self.repo.get_block_run_by_block(run.id, csv_block.id)
        assert csv_block_run.status == BlockRunStatus.COMPLETED
        assert len(csv_artifacts) == 1
        assert csv_artifacts[0].data["row_count"] == 3
        
        self.repo.update_block_run_status(sentiment_block_run.id, BlockRunStatus.RUNNING)
        sentiment_artifacts = self.executor.execute_llm_sentiment(sentiment_block_run, csv_artifacts)
        self.repo.save_artifacts(sentiment_artifacts)
        self.repo.update_block_run_status(sentiment_block_run.id, BlockRunStatus.COMPLETED)
        
        sentiment_block_run = self.repo.get_block_run_by_block(run.id, sentiment_block.id)
        assert sentiment_block_run.status == BlockRunStatus.COMPLETED
        assert len(sentiment_artifacts) == 1
        assert sentiment_artifacts[0].data["total_processed"] == 3
        
        self.repo.update_block_run_status(writer_block_run.id, BlockRunStatus.RUNNING)
        writer_artifacts = self.executor.execute_file_writer(writer_block_run, sentiment_artifacts, writer_block.config)
        self.repo.save_artifacts(writer_artifacts)
        self.repo.update_block_run_status(writer_block_run.id, BlockRunStatus.COMPLETED)
        
        writer_block_run = self.repo.get_block_run_by_block(run.id, writer_block.id)
        assert writer_block_run.status == BlockRunStatus.COMPLETED
        assert len(writer_artifacts) == 1
        
        output_file = writer_artifacts[0].data["file_path"]
        assert os.path.exists(output_file)
        assert output_file.endswith("sentiment_results.csv")
        
        import csv
        with open(output_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 3
            assert all("text" in row for row in rows)
            assert all("sentiment" in row for row in rows)
            assert all("score" in row for row in rows)
        
        self.repo.update_pipeline_run_status(run.id, PipelineStatus.COMPLETED)
        
        final_run = self.repo.list_runs_for_pipeline(self.pipeline_id)[0]
        assert final_run.status == PipelineStatus.COMPLETED
        
        all_block_runs = self.repo.get_all_block_runs_for_run(run.id)
        assert all(br.status == BlockRunStatus.COMPLETED for br in all_block_runs)