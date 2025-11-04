from abc import ABC, abstractmethod
from typing import Any
import json

try:
    from confluent_kafka import Producer, Consumer
except Exception:  # optional install
    Producer = None
    Consumer = None

from .models import LogEntry


class StreamingBackend(ABC):
    @abstractmethod
    def publish_log(self, log_entry: LogEntry):
        pass

    @abstractmethod
    def consume_logs(self, pipeline_run_id: str):
        pass


class KafkaStreamingBackend(StreamingBackend):
    def __init__(self, bootstrap_servers: str):
        if Producer is None or Consumer is None:
            raise RuntimeError("confluent-kafka not installed")
        self.bootstrap_servers = bootstrap_servers
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'charcentric-pipeline'
        })

    def publish_log(self, log_entry: LogEntry):
        try:
            self.producer.produce(
                topic='pipeline-logs',
                key=str(log_entry.pipeline_run_id),
                value=log_entry.json()
            )
            self.producer.poll(0)
        except Exception as e:  # noqa: BLE001
            print(f"Failed to publish log: {e}")

    def consume_logs(self, pipeline_run_id: str):
        consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': 'charcentric-ui',
            'auto.offset.reset': 'earliest'
        })
        consumer.subscribe(['pipeline-logs'])
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                log_entry = LogEntry.parse_raw(msg.value())
                if str(log_entry.pipeline_run_id) == pipeline_run_id:
                    yield log_entry
        finally:
            consumer.close()


class MockStreamingBackend(StreamingBackend):
    def __init__(self):
        self.logs = []

    def publish_log(self, log_entry: LogEntry):
        self.logs.append(log_entry)

    def consume_logs(self, pipeline_run_id: str):
        for log in self.logs:
            if str(log.pipeline_run_id) == pipeline_run_id:
                yield log