import os
import json
try:
    from confluent_kafka import Consumer
except Exception:
    Consumer = None

from .repository import SQLiteRepository
from .orchestrator import Orchestrator


def main():
    if Consumer is None:
        raise RuntimeError("confluent-kafka not installed")
    repo = SQLiteRepository(os.getenv('DATABASE_URL', '').replace('sqlite:///', '') or "/app/data/charcentric.db")
    kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    orchestrator = Orchestrator(repo, kafka_bootstrap)
    consumer = Consumer({'bootstrap.servers': kafka_bootstrap, 'group.id': 'charcentric-orchestrator', 'auto.offset.reset': 'earliest'})
    consumer.subscribe(['orchestrator-events'])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            event = json.loads(msg.value())
            if event.get('event') == 'block_completed':
                run_id = UUID(event['pipeline_run_id'])
                block_id = UUID(event['block_id'])
                from .models import Pipeline
                pipeline = Pipeline(**event['pipeline'])
                orchestrator.on_block_completed(run_id, pipeline, block_id)
            elif event.get('event') == 'block_failed':
                run_id = UUID(event['pipeline_run_id'])
                block_id = UUID(event['block_id'])
                from .models import Pipeline
                pipeline = Pipeline(**event['pipeline'])
                orchestrator.on_block_failed(run_id, pipeline, block_id)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()