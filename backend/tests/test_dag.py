import pytest
from uuid import uuid4
from charcentric.dag import DAGEngine
from charcentric.models import Block, Edge, BlockType, BlockRunStatus


class TestDAGEngine:
    def test_validate_acyclic_dag(self):
        """Test that acyclic DAGs are validated correctly"""
        engine = DAGEngine()
        
        block1 = Block(pipeline_id=uuid4(), name="Block 1", type=BlockType.CSV_READER)
        block2 = Block(pipeline_id=uuid4(), name="Block 2", type=BlockType.LLM_SENTIMENT)
        
        edges = [
            Edge(pipeline_id=block1.pipeline_id, source_block_id=block1.id, target_block_id=block2.id)
        ]
        
        assert engine.validate_dag([block1, block2], edges) is True
    
    def test_validate_cyclic_dag(self):
        """Test that cyclic DAGs are rejected"""
        engine = DAGEngine()
        
        block1 = Block(pipeline_id=uuid4(), name="Block 1", type=BlockType.CSV_READER)
        block2 = Block(pipeline_id=uuid4(), name="Block 2", type=BlockType.LLM_SENTIMENT)
        
        edges = [
            Edge(pipeline_id=block1.pipeline_id, source_block_id=block1.id, target_block_id=block2.id),
            Edge(pipeline_id=block1.pipeline_id, source_block_id=block2.id, target_block_id=block1.id)
        ]
        
        assert engine.validate_dag([block1, block2], edges) is False
    
    def test_get_execution_order(self):
        """Test topological sort returns correct execution levels"""
        engine = DAGEngine()
        
        block1 = Block(pipeline_id=uuid4(), name="Block 1", type=BlockType.CSV_READER)
        block2 = Block(pipeline_id=uuid4(), name="Block 2", type=BlockType.LLM_SENTIMENT)
        block3 = Block(pipeline_id=uuid4(), name="Block 3", type=BlockType.FILE_WRITER)
        
        edges = [
            Edge(pipeline_id=block1.pipeline_id, source_block_id=block1.id, target_block_id=block2.id),
            Edge(pipeline_id=block1.pipeline_id, source_block_id=block2.id, target_block_id=block3.id)
        ]
        
        levels = engine.get_execution_order([block1, block2, block3], edges)
        
        assert len(levels) == 3
        assert block1.id in levels[0]
        assert block2.id in levels[1]
        assert block3.id in levels[2]
    
    def test_get_runnable_blocks(self):
        """Test getting runnable blocks based on dependencies"""
        engine = DAGEngine()
        
        block1 = Block(pipeline_id=uuid4(), name="Block 1", type=BlockType.CSV_READER)
        block2 = Block(pipeline_id=uuid4(), name="Block 2", type=BlockType.LLM_SENTIMENT)
        
        edges = [
            Edge(pipeline_id=block1.pipeline_id, source_block_id=block1.id, target_block_id=block2.id)
        ]
        
        block_runs = {
            block1.id: BlockRunStatus.COMPLETED,
            block2.id: BlockRunStatus.PENDING
        }
        
        runnable = engine.get_runnable_blocks(block_runs, edges)
        assert block2.id in runnable
        
        block_runs[block1.id] = BlockRunStatus.FAILED
        runnable = engine.get_runnable_blocks(block_runs, edges)
        assert block2.id not in runnable