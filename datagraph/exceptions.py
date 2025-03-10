from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from typing import Any, Literal
    from uuid import UUID


class DatagraphError(Exception):
    def __init__(self, *args: "Any", **kwargs: "Any") -> None:
        super().__init__(*args, **kwargs)


class UnresolvedFlowError(DatagraphError):
    def __init__(self) -> None:
        super().__init__("Flows must be resolved before they can be used.")


class FlowResolutionError(DatagraphError):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class DuplicateIOError(FlowResolutionError):
    def __init__(self) -> None:
        super().__init__("IO cannot be yielded by multiple tasks in a single Flow.")


class CyclicFlowError(FlowResolutionError):
    def __init__(self, cycles: list[tuple[str]]) -> None:
        super().__init__(
            "Flows cannot contain IO dependency cycles. Offending cycles:\n"
            f" {'\n  '.join(' -> '.join(cycle) for cycle in cycles)}"
        )


class FlowExecutionAdvancementTimeout(DatagraphError):
    def __init__(self, flow_execution_uuid: "UUID") -> None:
        super().__init__(
            f"Failed to advance execution for flow '{flow_execution_uuid}'."
        )


class TamperedDataError(DatagraphError):
    def __init__(self) -> None:
        super().__init__("Deserialization failed due to signature mismatch.")


class IOError(DatagraphError):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class UnserializableValueError(IOError):
    def __init__(self, value: "Any") -> None:
        super().__init__(f"{value} is not a serializable IO value.")


class ReadOnlyIOError(IOError):
    def __init__(self, input_name: str) -> None:
        super().__init__(f"IO '{input_name}' is read-only.")


class MismatchedIOError(IOError):
    def __init__(
        self, op: 'Literal["read", "write"]', current_name: str, target_name: str
    ) -> None:
        super().__init__(
            f"Attempted to {op} value intended for IO '{target_name}' on"
            f" IO '{current_name}'."
        )
