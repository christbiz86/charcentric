import json
import os
import time
from typing import List
from uuid import UUID

try:
    from confluent_kafka import Consumer, Producer
except Exception:
    Consumer = None
    Producer = None

from .models import BlockType, BlockRunStatus, LogEntry, Artifact
from .repository import SQLiteRepository
from .steps import StepExecutor
from .llm import MockLLMProvider, GeminiLLMProvider


class Worker:
    def __init__(self, repository: SQLiteRepository, kafka_bootstrap: str, worker_id: str):
        if Consumer is None or Producer is None:
            raise RuntimeError("confluent-kafka not installed")
        self.repo = repository
        self.worker_id = worker_id
        self.consumer = Consumer({
            'bootstrap.servers': kafka_bootstrap,
            'group.id': f'charcentric-workers',
            'auto.offset.reset': 'earliest'
        })
        self.producer = Producer({'bootstrap.servers': kafka_bootstrap, 'client.id': worker_id})
        api_key = os.getenv('GEMINI_API_KEY')
        llm = GeminiLLMProvider(api_key) if api_key else MockLLMProvider()
        self.executor = StepExecutor(llm_provider=llm)

    def run(self):
        self.consumer.subscribe(['block-tasks'])
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                task = json.loads(msg.value())
                run_id = UUID(task['pipeline_run_id'])
                block_id = UUID(task['block_id'])
                pipeline = task['pipeline']
                self._process_block(run_id, block_id, pipeline)
        finally:
            self.consumer.close()

    def _process_block(self, run_id: UUID, block_id: UUID, pipeline: dict):
        # get block
        blocks = {UUID(b['id']): b for b in pipeline['blocks']}
        edges = pipeline['edges']
        block = blocks[block_id]
        block_run = self.repo.get_block_run_by_block(run_id, block_id)
        if not block_run:
            return
        
        max_retries = 3
        retry_delay = 1.0
        last_error = None
        
        for attempt in range(max_retries):
            if attempt > 0:
                self._emit_log(run_id, block_run.id, f"Retrying block {block['name']} (attempt {attempt + 1}/{max_retries})", level="WARN")
                time.sleep(retry_delay * (2 ** (attempt - 1)))  # exponential backoff
            
            self.repo.update_block_run_status(block_run.id, BlockRunStatus.RUNNING)
            if attempt == 0:
                self._emit_log(run_id, block_run.id, f"Starting block {block['name']} ({block['type']})", level="INFO")
            
            preds = [UUID(e['source_block_id']) for e in edges if UUID(e['target_block_id']) == block_id]
            input_artifacts: List[Artifact] = []
            for pred_id in preds:
                pred_run = self.repo.get_block_run_by_block(run_id, pred_id)

            try:
                btype = block['type']
                artifacts: List[Artifact] = []
                if btype == BlockType.CSV_READER.value:
                    artifacts = self.executor.execute_csv_reader(block_run, block.get('config', {}))
                elif btype == BlockType.LLM_SENTIMENT.value:
                    artifacts = self.executor.execute_llm_sentiment(block_run, input_artifacts)
                elif btype == BlockType.LLM_TOXICITY.value:
                    artifacts = self.executor.execute_llm_toxicity(block_run, input_artifacts)
                elif btype == BlockType.FILE_WRITER.value:
                    artifacts = self.executor.execute_file_writer(block_run, input_artifacts, block.get('config', {}))
                else:
                    raise ValueError(f"Unknown block type {btype}")
                
                self.repo.save_artifacts(artifacts)
                self.repo.update_block_run_status(block_run.id, BlockRunStatus.COMPLETED)
                self._emit_log(run_id, block_run.id, f"Completed block {block['name']}", level="INFO")
                event = {"pipeline_run_id": str(run_id), "block_id": str(block_id), "event": "block_completed", "pipeline": pipeline}
                self.producer.produce('orchestrator-events', key=str(run_id), value=json.dumps(event))
                self.producer.poll(0)
                return 
                
            except Exception as e:
                last_error = e
                self._emit_log(run_id, block_run.id, f"Block attempt {attempt + 1} failed: {e}", level="ERROR")
                if attempt < max_retries - 1:
                    continue 
        
        self.repo.update_block_run_status(block_run.id, BlockRunStatus.FAILED, error_message=str(last_error))
        self._emit_log(run_id, block_run.id, f"Block {block['name']} failed after {max_retries} attempts: {last_error}", level="ERROR")
        event = {"pipeline_run_id": str(run_id), "block_id": str(block_id), "event": "block_failed", "pipeline": pipeline, "error": str(last_error)}
        self.producer.produce('orchestrator-events', key=str(run_id), value=json.dumps(event))
        self.producer.poll(0)

    def _emit_log(self, run_id: UUID, block_run_id: UUID, message: str, level: str = "INFO"):
        log = LogEntry(pipeline_run_id=run_id, block_run_id=block_run_id, level=level, message=message)
        self.producer.produce('pipeline-logs', key=str(run_id), value=log.json())
        self.producer.poll(0)


def main():
    repo = SQLiteRepository(os.getenv('DATABASE_URL', '').replace('sqlite:///', '') or "/app/data/charcentric.db")
    kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    worker_id = os.getenv('WORKER_ID', 'worker')
    worker = Worker(repo, kafka_bootstrap, worker_id)
    worker.run()


if __name__ == "__main__":
    main()


