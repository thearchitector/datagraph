"""
Flow module for the DataGraph framework.
"""

from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import networkx as nx
from pydantic import BaseModel, Field

from .exceptions import CyclicFlowError, DuplicateIOError, UnresolvedFlowError
from .task import Task
from .topology import Topology

if TYPE_CHECKING:  # pragma: no cover
    from redis.asyncio.client import Pipeline


class FlowExecutionPlan(BaseModel):
    uuid: UUID = Field(default_factory=uuid4)
    partitions: list[set[Task]]
    current_partition: int = 0

    async def partition_complete(self, pipeline: "Pipeline") -> bool:
        # TODO: use uuid to resolve IO all tasks in current partition
        # check that every task's `flow:{flow_uuid}:tasks:{name}:done` key
        # is set
        raise NotImplementedError()

    def proceed(self) -> set["Task"]:
        self.current_partition += 1
        return self.partitions[self.current_partition]


class Flow:
    def __init__(self, tasks: list[Task]) -> None:
        self.tasks = tasks
        self._topology: Topology | None = None
        self._execution_plan: FlowExecutionPlan | None = None

    @classmethod
    def from_tasks(cls, *tasks: Task) -> "Flow":
        return cls(tasks=list(tasks))

    def resolve(self) -> "Flow":
        all_inputs = set()
        all_outputs = set()
        output_producers = {}

        # Validate that no output is produced by multiple tasks
        for task in self.tasks:
            for inp in task.inputs:
                all_inputs.add((task.name, inp))

            for out in task.outputs:
                # Check if this output is already produced by another task
                if out in output_producers:
                    raise DuplicateIOError()

                # Record which task produces this output
                output_producers[out] = task.name
                all_outputs.add((task.name, out))

        # Create a directed graph to represent the flow
        digraph = nx.DiGraph()

        # Add all tasks as nodes
        for task in self.tasks:
            digraph.add_node(task)

        # Connect tasks based on matching input/output names
        for src_task in self.tasks:
            for src_out in src_task.outputs:
                for dst_task in self.tasks:
                    for dst_in in dst_task.inputs:
                        if src_out == dst_in:
                            digraph.add_edge(src_task, dst_task, io_name=src_out)

        try:
            order = list(nx.topological_sort(digraph))
        except nx.NetworkXUnfeasible as e:
            # Find cycles and convert Task objects to their names for the error message
            cycles = nx.simple_cycles(digraph)

            # Sort cycles by length for better error reporting
            sorted_cycles = sorted(
                (tuple(task.name for task in cycle) for cycle in cycles), key=len
            )

            raise CyclicFlowError(sorted_cycles) from e

        self._topology = Topology(digraph=digraph, order=order)
        self._execution_plan = self._create_execution_plan()

        return self

    def _create_execution_plan(self) -> FlowExecutionPlan:
        # TODO: be smarter with partitioning? how to treat tasks in the same
        # topological level when one+ of them waits
        partitions: list[set[Task]] = []
        current_partition = set()

        for task in self._topology.order:
            if task.wait and current_partition:
                partitions.append(current_partition)
                current_partition = {task}
            else:
                current_partition.add(task)

        if current_partition:
            partitions.append(current_partition)

        return FlowExecutionPlan(partitions=partitions)

    @property
    def resolved(self) -> bool:
        return self._execution_plan is not None

    @property
    def topology(self) -> Topology:
        if not self.resolved:
            raise UnresolvedFlowError()

        return self._topology

    @property
    def execution_plan(self) -> FlowExecutionPlan:
        if not self.resolved:
            raise UnresolvedFlowError()

        return self._execution_plan
