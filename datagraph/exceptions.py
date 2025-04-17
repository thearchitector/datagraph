from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from typing import Any, Literal
    from uuid import UUID


class DatagraphError(Exception):
    def __init__(self, *args: "Any", **kwargs: "Any") -> None:
        super().__init__(*args, **kwargs)


##
## FLOW RESOLUTION
##


class UnresolvedFlowError(DatagraphError):
    def __init__(self) -> None:
        super().__init__("Flows must be resolved before they can be used.")


class FlowResolutionError(DatagraphError):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class DuplicateIOError(FlowResolutionError):
    def __init__(self) -> None:
        super().__init__(
            "IO cannot be yielded by multiple processors in a single Flow."
        )


class CyclicFlowError(FlowResolutionError):
    def __init__(self, cycles: list[tuple[str]]) -> None:
        cycle_str = "\n  ".join(" -> ".join(cycle) for cycle in cycles)
        super().__init__(
            "Flows cannot contain IO dependency cycles. Offending cycles:\n"
            f" {cycle_str}"
        )


##
## FLOW EXECUTION
##


class FlowExecutionError(DatagraphError):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class FloatingIOError(FlowExecutionError):
    def __init__(self, missing_inputs: set[str]) -> None:
        super().__init__(
            f"Flow requires supplied values for inputs: '{missing_inputs}'."
        )


class FlowExecutionAdvancementTimeout(FlowExecutionError):
    def __init__(self, flow_execution_uuid: "UUID") -> None:
        super().__init__(
            f"Failed to advance execution for Flow '{flow_execution_uuid}'."
        )


class IOStreamTimeout(FlowExecutionError):
    def __init__(self, io_name: str, timeout: int) -> None:
        super().__init__(
            f"Failed to read a new value from IO '{io_name}' before max timeout"
            f" reached ({timeout}s)."
        )


class UnregisteredProcessorError(FlowExecutionError):
    def __init__(self, processor_name: str) -> None:
        super().__init__(
            f"Processor '{processor_name}' is not defined in the current runtime."
            f" Register it to a function with `@{processor_name}`."
        )


##
## IO
##


class IOError(DatagraphError):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class UnserializableValueError(IOError):
    def __init__(self, value: "Any") -> None:
        super().__init__(f"{value} is not a serializable IO value.")


class TamperedDataError(IOError):
    def __init__(self) -> None:
        super().__init__("Deserialization failed due to signature mismatch.")


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
