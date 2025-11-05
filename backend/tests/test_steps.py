import pytest
import os
import tempfile
import csv
from uuid import uuid4
from charcentric.steps import StepExecutor
from charcentric.models import BlockRun, Artifact, BlockType, BlockRunStatus
from charcentric.llm import MockLLMProvider


class TestStepExecutor:
    def setup_method(self):
        """Set up test fixtures"""
        self.llm_provider = MockLLMProvider()
        self.temp_dir = tempfile.mkdtemp()
        self.executor = StepExecutor(self.llm_provider, data_dir=self.temp_dir)
        self.block_run = BlockRun(
            pipeline_run_id=uuid4(),
            block_id=uuid4(),
            status=BlockRunStatus.PENDING
        )
    
    def test_execute_csv_reader(self):
        """Test CSV reader step"""
        # Create test CSV file
        csv_file = os.path.join(self.temp_dir, "test.csv")
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow(['id', 'text'])
            writer.writerow(['1', 'Positive text'])
            writer.writerow(['2', 'Negative text'])
        
        config = {"file_path": csv_file, "delimiter": ";"}
        artifacts = self.executor.execute_csv_reader(self.block_run, config)
        
        assert len(artifacts) == 1
        assert artifacts[0].type == "csv_rows"
        assert artifacts[0].data["row_count"] == 2
        assert "id" in artifacts[0].data["columns"]
        assert "text" in artifacts[0].data["columns"]
        assert len(artifacts[0].data["preview"]) == 2
    
    def test_execute_llm_sentiment(self):
        """Test LLM sentiment analysis step"""
        input_artifacts = [
            Artifact(
                block_run_id=uuid4(),
                name="csv_data",
                type="csv_rows",
                data={
                    "preview": [
                        {"text": "I love this"},
                        {"text": "I hate this"}
                    ]
                }
            )
        ]
        
        artifacts = self.executor.execute_llm_sentiment(self.block_run, input_artifacts)
        
        assert len(artifacts) == 1
        assert artifacts[0].type == "sentiment_analysis"
        assert artifacts[0].data["total_processed"] == 2
        assert len(artifacts[0].data["results"]) == 2
    
    def test_execute_llm_toxicity(self):
        """Test LLM toxicity detection step"""
        input_artifacts = [
            Artifact(
                block_run_id=uuid4(),
                name="csv_data",
                type="csv_rows",
                data={
                    "preview": [
                        {"text": "Friendly message"},
                        {"text": "Toxic message"}
                    ]
                }
            )
        ]
        
        artifacts = self.executor.execute_llm_toxicity(self.block_run, input_artifacts)
        
        assert len(artifacts) == 1
        assert artifacts[0].type == "toxicity_detection"
        assert artifacts[0].data["total_processed"] == 2
    
    def test_execute_file_writer_sentiment(self):
        """Test file writer for sentiment results"""
        input_artifacts = [
            Artifact(
                block_run_id=uuid4(),
                name="sentiment_results",
                type="sentiment_analysis",
                data={
                    "results": [
                        {"text": "Test", "sentiment": "POSITIVE", "score": 5}
                    ]
                }
            )
        ]
        
        config = {"output_type": "sentiment"}
        artifacts = self.executor.execute_file_writer(self.block_run, input_artifacts, config)
        
        assert len(artifacts) == 1
        assert artifacts[0].type == "file_output"
        assert os.path.exists(artifacts[0].data["file_path"])
        assert artifacts[0].data["file_path"].endswith("sentiment_results.csv")
        
        with open(artifacts[0].data["file_path"], 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 1
            assert rows[0]["sentiment"] == "POSITIVE"
    
    def test_execute_file_writer_toxicity(self):
        """Test file writer for toxicity results"""
        input_artifacts = [
            Artifact(
                block_run_id=uuid4(),
                name="toxicity_results",
                type="toxicity_detection",
                data={
                    "results": [
                        {"text": "Test", "toxicity": "NON_TOXIC", "score": 0}
                    ]
                }
            )
        ]
        
        config = {"output_type": "toxicity"}
        artifacts = self.executor.execute_file_writer(self.block_run, input_artifacts, config)
        
        assert len(artifacts) == 1
        assert artifacts[0].data["file_path"].endswith("toxicity_results.csv")
        
        with open(artifacts[0].data["file_path"], 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 1
            assert rows[0]["toxicity"] == "NON_TOXIC"