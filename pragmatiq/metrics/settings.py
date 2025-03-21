from typing import TYPE_CHECKING

from prometheus_client import Counter, Gauge, Summary, start_http_server

if TYPE_CHECKING:
    from pragmatiq.broker.redis_broker import RedisBroker
    from pragmatiq.core.queue import Queue

# Metrics
CPU_BOUND_TASKS_TOTAL: Counter = Counter(
    name="cpu_bound_tasks_total",
    documentation="Total CPU-bound tasks executed",
)
CPU_BOUND_TASK_DURATION: Summary = Summary(
    name="cpu_bound_task_duration",
    documentation="Duration of CPU-bound tasks",
)

TASK_TIMEOUTS_TOTAL: Counter = Counter(
    name="task_timeouts_total",
    documentation="Total number of tasks that timed out",
)
TASK_CANCELLATIONS_TOTAL: Counter = Counter(
    name="task_cancellations_total",
    documentation="Total number of tasks that were cancelled",
)

TASK_DURATION: Summary = Summary(
    name="task_duration",
    documentation="Duration of all tasks (in seconds)",
)

TASKS_ENQUEUED: Counter = Counter(
    name="tasks_enqueued_total",
    documentation="Total tasks enqueued",
)

TASKS_DEQUEUED: Counter = Counter(
    name="tasks_dequeued_total",
    documentation="Total tasks dequeued",
)

TASK_STATE_PENDING: Gauge = Gauge(
    name="task_state_pending",
    documentation="Number of tasks in pending state",
)

TASK_STATE_RUNNING: Counter = Counter(
    name="task_state_running_total",
    documentation="Number of tasks in running state",
)

TASK_STATE_COMPLETED: Counter = Counter(
    name="task_state_completed_total",
    documentation="Number of tasks in completed state",
)

TASK_STATE_FAILED: Counter = Counter(
    name="task_state_failed_total",
    documentation="Number of tasks in failed state",
)

TASK_QUEUE_LENGTH: Summary = Summary(
    name="task_queue_length",
    documentation="Current length of task queue",
)


def start_prometheus(port: int = 9090) -> None:
    start_http_server(port=port, addr="localhost")


async def update_task_queue_length(task_queue: "Queue") -> None:
    if task_queue:
        total_length = 0
        broker: "RedisBroker" = task_queue.broker
        async with broker.redis.pipeline() as pipe:
            for priority in range(10):
                await pipe.llen(f"{broker.queue_name}:{priority}")  # type: ignore[awaitable]
            lengths = await pipe.execute()
        total_length = sum(lengths)
        TASK_QUEUE_LENGTH.observe(amount=total_length)
