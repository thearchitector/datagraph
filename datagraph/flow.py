"""
Flow module for the DataGraph framework.
"""

import networkx as nx

from .exceptions import CyclicFlowError, DuplicateIOError, UnresolvedFlowError
from .flow_execution_plan import FlowExecutionPlan
from .processor import Processor
from .topology import Topology


class Flow:
    def __init__(self, processors: list[Processor]) -> None:
        self.processors = processors
        self._topology: Topology | None = None
        self._execution_plan: FlowExecutionPlan | None = None

    @classmethod
    def from_processors(cls, *processors: Processor) -> "Flow":
        return cls(processors=list(processors))

    def resolve(self) -> "Flow":
        all_inputs = set()
        all_outputs = set()

        # validate that no output is produced by multiple processors
        for processor in self.processors:
            all_inputs.update(processor.inputs)

            for out in processor.outputs:
                if out in all_outputs:
                    raise DuplicateIOError()

                all_outputs.add(out)

        # create a directed graph to represent the flow
        digraph = nx.DiGraph()

        # add all processors as nodes
        for processor in self.processors:
            digraph.add_node(processor)

        # connect processors based on matching input/output names
        for src_processor in self.processors:
            for src_out in src_processor.outputs:
                for dst_processor in self.processors:
                    for dst_in in dst_processor.inputs:
                        if src_out == dst_in:
                            digraph.add_edge(
                                src_processor, dst_processor, io_name=src_out
                            )

        try:
            order = list(nx.topological_sort(digraph))
        except nx.NetworkXUnfeasible as e:
            # Find cycles and convert Processor objects to their names for the error message
            cycles = nx.simple_cycles(digraph)

            # Sort cycles by length for better error reporting
            sorted_cycles = sorted(
                (tuple(processor.name for processor in cycle) for cycle in cycles),
                key=len,
            )

            raise CyclicFlowError(sorted_cycles) from e

        self._topology = Topology(
            digraph=digraph, order=order, floating_inputs=all_inputs - all_outputs
        )
        self._execution_plan = self._create_execution_plan()

        return self

    def _create_execution_plan(self) -> FlowExecutionPlan:
        # TODO: be smarter with partitioning? how to treat processors in the same
        # topological level when one+ of them waits
        partitions: list[set[Processor]] = []
        current_partition = set()

        for processor in self._topology.order:
            if processor.wait and current_partition:
                partitions.append(current_partition)
                current_partition = {processor}
            else:
                current_partition.add(processor)

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
