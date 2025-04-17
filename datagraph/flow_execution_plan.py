from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from .io import IO
from .processor import Processor


class FlowExecutionPlan(BaseModel):
    uuid: UUID = Field(default_factory=uuid4)
    partitions: list[set[Processor]]
    current_partition: int = -1

    async def partition_complete(self) -> bool:
        if self.current_partition < 0:
            return True

        for processor in self.partitions[self.current_partition]:
            for output in processor.outputs:
                if not await IO(output, self, True).is_complete():
                    return False

        return True

    def proceed(self) -> set["Processor"]:
        self.current_partition += 1
        return self.partitions[self.current_partition]
