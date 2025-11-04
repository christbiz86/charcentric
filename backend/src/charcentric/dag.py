from typing import Dict, List
from collections import deque, defaultdict
from uuid import UUID
import logging

from .models import Block, Edge, BlockRunStatus


class DAGEngine:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def validate_dag(self, blocks: List[Block], edges: List[Edge]) -> bool:
        graph = self._build_adjacency_list(blocks, edges)
        return self._is_acyclic(graph)

    def _build_adjacency_list(self, blocks: List[Block], edges: List[Edge]) -> Dict[UUID, List[UUID]]:
        graph = {block.id: [] for block in blocks}
        for edge in edges:
            graph[edge.source_block_id].append(edge.target_block_id)
        return graph

    def _is_acyclic(self, graph: Dict[UUID, List[UUID]]) -> bool:
        visited = set()
        recursion_stack = set()

        def dfs(node):
            if node in recursion_stack:
                return False
            if node in visited:
                return True
            visited.add(node)
            recursion_stack.add(node)
            for neighbor in graph.get(node, []):
                if not dfs(neighbor):
                    return False
            recursion_stack.remove(node)
            return True

        for node in graph:
            if node not in visited:
                if not dfs(node):
                    return False
        return True

    def get_execution_order(self, blocks: List[Block], edges: List[Edge]) -> List[List[UUID]]:
        graph = self._build_adjacency_list(blocks, edges)
        in_degree = {block.id: 0 for block in blocks}
        for edges_list in graph.values():
            for target in edges_list:
                in_degree[target] += 1
        queue = deque([node for node in in_degree if in_degree[node] == 0])
        levels = []
        while queue:
            level_size = len(queue)
            current_level = []
            for _ in range(level_size):
                node = queue.popleft()
                current_level.append(node)
                for neighbor in graph.get(node, []):
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)
            levels.append(current_level)
        return levels

    def get_runnable_blocks(
        self,
        block_runs: Dict[UUID, BlockRunStatus],
        edges: List[Edge]
    ) -> List[UUID]:
        dependencies = defaultdict(set)
        for edge in edges:
            dependencies[edge.target_block_id].add(edge.source_block_id)
        runnable_blocks = []
        for block_id in block_runs:
            if block_runs[block_id] in [BlockRunStatus.RUNNING, BlockRunStatus.COMPLETED]:
                continue
            block_deps = dependencies.get(block_id, set())
            all_deps_completed = all(
                block_runs.get(dep_id) == BlockRunStatus.COMPLETED
                for dep_id in block_deps
            )
            if all_deps_completed:
                runnable_blocks.append(block_id)
        return runnable_blocks