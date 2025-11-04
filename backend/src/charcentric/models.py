from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4
from datetime import datetime
from pydantic import BaseModel, Field


class PipelineStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class BlockType(str, Enum):
    CSV_READER = "csv_reader"
    LLM_SENTIMENT = "llm_sentiment"
    LLM_TOXICITY = "llm_toxicity"
    FILE_WRITER = "file_writer"


class BlockRunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class Pipeline(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    name: str
    description: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    blocks: List['Block'] = Field(default_factory=list)
    edges: List['Edge'] = Field(default_factory=list)


class Block(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    pipeline_id: UUID
    name: str
    type: BlockType
    config: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class Edge(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    pipeline_id: UUID
    source_block_id: UUID
    target_block_id: UUID


class PipelineRun(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    pipeline_id: UUID
    status: PipelineStatus = PipelineStatus.QUEUED
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class BlockRun(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    pipeline_run_id: UUID
    block_id: UUID
    status: BlockRunStatus = BlockRunStatus.PENDING
    input_artifacts: List[UUID] = Field(default_factory=list)
    output_artifacts: List[UUID] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None


class Artifact(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    block_run_id: UUID
    name: str
    type: str
    data: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class LogEntry(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    pipeline_run_id: UUID
    block_run_id: Optional[UUID] = None
    level: str
    message: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = Field(default_factory=dict)