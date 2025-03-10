# Flow.from_tasks accepts tasks
# Flow.resolve creates a Topology, which is backed by a networkx digraph
# resolution fails if there is a cycle in the graph

# Flow.resolve creates an ExecutionPlan (frozen, hashable) consisting of partitions
# and current step. the task's barriers, if defined, can be used to divide tasks into
# partitions based on the `'on' inputs.

# Executor is stateless and infra agnostic, `.execute` returns IO outputs from last task
# in flow takes first partition of execution plan and runs `execute_partition`

# LocalExecutor amounts only to concurrently running partition in a task group

# CeleryExecutor, default impl of celery, amounts to `apply_async` partition
# that have been decorated @app.task (@app.task, @foo, async def)

# Task decorator wraps function in TaskRunner

# when TaskRunner is invoked, uses dependency injection to resolve IOs. also support
# general dependency injection for non-io local dependencies (alla FastAPI Depends).
# optional cachable by flow uuid

# when a task completes, read the flow's execution plan and checks if all output for
# tasks in the current partition are complete. if complete, executor increments and
# runs next partition
