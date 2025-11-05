import os
import sqlite3
import json
from contextlib import contextmanager
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from .models import (
    Pipeline, Block, Edge, PipelineRun, BlockRun, Artifact,
    PipelineStatus, BlockRunStatus
)


def _utcnow_iso() -> str:
    return datetime.utcnow().isoformat()


class SQLiteRepository:
    def __init__(self, db_path: str = "/app/data/charcentric.db"):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db_path = db_path
        self._init_schema()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_schema(self):
        with self._conn() as conn:
            c = conn.cursor()
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS pipelines (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    description TEXT,
                    created_at TEXT,
                    blocks_json TEXT,
                    edges_json TEXT
                )
                """
            )
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    id TEXT PRIMARY KEY,
                    pipeline_id TEXT,
                    status TEXT,
                    created_at TEXT,
                    started_at TEXT,
                    completed_at TEXT
                )
                """
            )
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS block_runs (
                    id TEXT PRIMARY KEY,
                    pipeline_run_id TEXT,
                    block_id TEXT,
                    status TEXT,
                    created_at TEXT,
                    started_at TEXT,
                    completed_at TEXT,
                    error_message TEXT,
                    input_artifacts_json TEXT,
                    output_artifacts_json TEXT
                )
                """
            )
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS artifacts (
                    id TEXT PRIMARY KEY,
                    block_run_id TEXT,
                    name TEXT,
                    type TEXT,
                    data_json TEXT,
                    created_at TEXT
                )
                """
            )

    # Pipelines
    def save_pipeline(self, pipeline: Pipeline) -> UUID:
        with self._conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO pipelines (id, name, description, created_at, blocks_json, edges_json) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    str(pipeline.id), pipeline.name, pipeline.description or "",
                    pipeline.created_at.isoformat(),
                    json.dumps([b.model_dump() for b in pipeline.blocks]),
                    json.dumps([e.model_dump() for e in pipeline.edges]),
                ),
            )
        return pipeline.id

    def get_pipeline(self, pipeline_id: UUID) -> Optional[Pipeline]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT id, name, description, created_at, blocks_json, edges_json FROM pipelines WHERE id = ?",
                (str(pipeline_id),),
            ).fetchone()
        if not row:
            return None
        blocks = [Block(**b) for b in json.loads(row[4])]
        edges = [Edge(**e) for e in json.loads(row[5])]
        return Pipeline(id=UUID(row[0]), name=row[1], description=row[2] or None, created_at=datetime.fromisoformat(row[3]), blocks=blocks, edges=edges)

    def list_pipelines(self) -> List[Pipeline]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT id, name, description, created_at, blocks_json, edges_json FROM pipelines ORDER BY created_at DESC"
            ).fetchall()
        pipelines: List[Pipeline] = []
        for row in rows:
            blocks = [Block(**b) for b in json.loads(row[4])]
            edges = [Edge(**e) for e in json.loads(row[5])]
            pipelines.append(Pipeline(id=UUID(row[0]), name=row[1], description=row[2] or None, created_at=datetime.fromisoformat(row[3]), blocks=blocks, edges=edges))
        return pipelines

    # Runs
    def create_pipeline_run(self, pipeline_id: UUID) -> PipelineRun:
        run = PipelineRun(pipeline_id=pipeline_id, status=PipelineStatus.QUEUED)
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO pipeline_runs (id, pipeline_id, status, created_at, started_at, completed_at) VALUES (?, ?, ?, ?, ?, ?)",
                (str(run.id), str(run.pipeline_id), run.status.value, run.created_at.isoformat(), None, None),
            )
        return run

    def update_pipeline_run_status(self, run_id: UUID, status: PipelineStatus):
        timestamps = {"started_at": None, "completed_at": None}
        if status == PipelineStatus.RUNNING:
            timestamps["started_at"] = _utcnow_iso()
        if status in (PipelineStatus.COMPLETED, PipelineStatus.FAILED):
            timestamps["completed_at"] = _utcnow_iso()
        with self._conn() as conn:
            conn.execute(
                "UPDATE pipeline_runs SET status = ?, started_at = COALESCE(started_at, ?), completed_at = COALESCE(completed_at, ?) WHERE id = ?",
                (status.value, timestamps["started_at"], timestamps["completed_at"], str(run_id)),
            )

    def list_runs_for_pipeline(self, pipeline_id: UUID) -> List[PipelineRun]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT id, pipeline_id, status, created_at, started_at, completed_at FROM pipeline_runs WHERE pipeline_id = ? ORDER BY created_at DESC",
                (str(pipeline_id),),
            ).fetchall()
        runs: List[PipelineRun] = []
        for row in rows:
            runs.append(PipelineRun(id=UUID(row[0]), pipeline_id=UUID(row[1]), status=PipelineStatus(row[2]), created_at=datetime.fromisoformat(row[3]), started_at=datetime.fromisoformat(row[4]) if row[4] else None, completed_at=datetime.fromisoformat(row[5]) if row[5] else None))
        return runs

    # Block runs
    def create_block_runs(self, run: PipelineRun, blocks: List[Block]) -> List[BlockRun]:
        block_runs: List[BlockRun] = []
        with self._conn() as conn:
            for block in blocks:
                br = BlockRun(pipeline_run_id=run.id, block_id=block.id, status=BlockRunStatus.PENDING)
                block_runs.append(br)
                conn.execute(
                    "INSERT INTO block_runs (id, pipeline_run_id, block_id, status, created_at, started_at, completed_at, error_message, input_artifacts_json, output_artifacts_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (str(br.id), str(br.pipeline_run_id), str(br.block_id), br.status.value, br.created_at.isoformat(), None, None, None, json.dumps([]), json.dumps([])),
                )
        return block_runs

    def update_block_run_status(self, block_run_id: UUID, status: BlockRunStatus, error_message: Optional[str] = None):
        timestamps = {"started_at": None, "completed_at": None}
        if status == BlockRunStatus.RUNNING:
            timestamps["started_at"] = _utcnow_iso()
        if status in (BlockRunStatus.COMPLETED, BlockRunStatus.FAILED):
            timestamps["completed_at"] = _utcnow_iso()
        with self._conn() as conn:
            conn.execute(
                "UPDATE block_runs SET status = ?, started_at = COALESCE(started_at, ?), completed_at = COALESCE(completed_at, ?), error_message = ? WHERE id = ?",
                (status.value, timestamps["started_at"], timestamps["completed_at"], error_message, str(block_run_id)),
            )

    def get_block_run_by_block(self, run_id: UUID, block_id: UUID) -> Optional[BlockRun]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT id, pipeline_run_id, block_id, status, created_at, started_at, completed_at, error_message, input_artifacts_json, output_artifacts_json FROM block_runs WHERE pipeline_run_id = ? AND block_id = ?",
                (str(run_id), str(block_id)),
            ).fetchone()
        if not row:
            return None
        return BlockRun(
            id=UUID(row[0]), pipeline_run_id=UUID(row[1]), block_id=UUID(row[2]), status=BlockRunStatus(row[3]),
            created_at=datetime.fromisoformat(row[4]), started_at=datetime.fromisoformat(row[5]) if row[5] else None,
            completed_at=datetime.fromisoformat(row[6]) if row[6] else None, error_message=row[7] or None,
            input_artifacts=[UUID(a) for a in json.loads(row[8])], output_artifacts=[UUID(a) for a in json.loads(row[9])]
        )

    def get_all_block_runs_for_run(self, run_id: UUID) -> List[BlockRun]:
        """Get all block runs for a pipeline run"""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT id, pipeline_run_id, block_id, status, created_at, started_at, completed_at, error_message, input_artifacts_json, output_artifacts_json FROM block_runs WHERE pipeline_run_id = ?",
                (str(run_id),),
            ).fetchall()
        block_runs: List[BlockRun] = []
        for row in rows:
            block_runs.append(BlockRun(
                id=UUID(row[0]), pipeline_run_id=UUID(row[1]), block_id=UUID(row[2]), status=BlockRunStatus(row[3]),
                created_at=datetime.fromisoformat(row[4]), started_at=datetime.fromisoformat(row[5]) if row[5] else None,
                completed_at=datetime.fromisoformat(row[6]) if row[6] else None, error_message=row[7] or None,
                input_artifacts=[UUID(a) for a in json.loads(row[8])], output_artifacts=[UUID(a) for a in json.loads(row[9])]
            ))
        return block_runs

    # Artifacts
    def save_artifacts(self, artifacts: List[Artifact]):
        with self._conn() as conn:
            for art in artifacts:
                conn.execute(
                    "INSERT OR REPLACE INTO artifacts (id, block_run_id, name, type, data_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (str(art.id), str(art.block_run_id), art.name, art.type, json.dumps(art.data), art.created_at.isoformat()),
                )