from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from .io import IO
from .task import Task


class FlowExecutionPlan(BaseModel):
    uuid: UUID = Field(default_factory=uuid4)
    partitions: list[set[Task]]
    current_partition: int = -1

    async def partition_complete(self) -> bool:
        if self.current_partition < 0:
            return True

        for task in self.partitions[self.current_partition]:
            for output in task.outputs:
                if not await IO(output, self, True).is_complete():
                    return False

        return True

    def proceed(self) -> set["Task"]:
        self.current_partition += 1
        return self.partitions[self.current_partition]
