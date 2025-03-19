from typing import TYPE_CHECKING

from networkx import generate_network_text

if TYPE_CHECKING:  # pragma: no cover
    from networkx import DiGraph

    from .processor import Processor


class Topology:
    def __init__(
        self, *, digraph: "DiGraph", order: list["Processor"], floating_inputs: set[str]
    ) -> None:
        self.digraph = digraph
        self.order = order
        self.floating_inputs = floating_inputs

    def __str__(self) -> str:
        return "\n".join(generate_network_text(self.digraph, vertical_chains=True))
