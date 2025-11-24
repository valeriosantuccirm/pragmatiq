import asyncio
import logging
from typing import Callable, Dict

from pragmatiq.broker.redis_broker import RedisBroker
from pragmatiq.core.task import Task
from pragmatiq.metrics.settings import (
    TASK_STATE_PENDING,
    TASKS_DEQUEUED,
    TASKS_ENQUEUED,
)
from pragmatiq.types import R


class Queue:
    def __init__(
        self,
        broker: RedisBroker,
    ) -> None:
        """Initialize a priority-based task queue using Redis.

        Sets up a queue with Redis as the backend for storing and managing tasks, supporting
        priority levels from 0 (highest) to 9 (lowest).

        Args:
            redis_host: Hostname or IP address of the Redis server.
            redis_port: Port number of the Redis server.
            queue_name: Unique name for the queue, used as a prefix for Redis keys.
        """
        self.logger: logging.Logger = logging.getLogger(name=__name__)
        self.running: bool = True
        self.broker: RedisBroker = broker

    async def enqueue(
        self,
        task: Task,
    ) -> str:
        """Enqueue a task into the Redis queue based on its priority.

        Adds the task to the appropriate priority list in Redis and updates Prometheus metrics
        for enqueued and pending tasks.

        Args:
            task: The Task instance to enqueue.
        """

        TASKS_ENQUEUED.inc()
        TASK_STATE_PENDING.inc()
        await self.broker.rpush(task=task)
        self.logger.info(f"Task '{task.func.__name__}' enqueued")
        return task.task_id

    async def dequeue(
        self,
        func_mapping: Dict[str, Callable[..., R]],
    ) -> Task | None:
        """Dequeue a task from Redis lists based on priority.

        Checks Redis lists in order of priority (0 to 9) and returns the first available task,
        updating the dequeued task metric. Returns None if no tasks are found.

        Args:
            func_mapping: Dictionary mapping function names to their callable implementations,
                used for deserializing tasks.

        Returns:
            Task | None: The dequeued Task instance, or None if the queue is empty.
        """
        for priority in range(10):  # Check priorities from 0 (highest) to 9 (lowest)
            task_data: bytes | None = await self.broker.lpop(priority=priority)
            if task_data:
                TASKS_DEQUEUED.inc()
                self.logger.info(
                    "Task denqueued",
                    task_data,
                )
                return Task.deserialize(
                    broker=self.broker,
                    data=task_data,
                    func_mapping=func_mapping,
                )
        return None

    async def run_tasks(
        self,
        loop: asyncio.AbstractEventLoop,
        func_mapping: Dict[str, Callable[..., R]],
    ) -> None:
        """Run tasks from the Redis queue indefinitely until stopped.

        Continuously dequeues and executes tasks, handling failures by logging errors and
        enqueuing failed tasks to a dead-letter queue (DLQ). Pauses briefly if the queue is empty
        to avoid busy-waiting.

        Args:
            loop: The asyncio event loop to use for task execution.
            pool: The process pool for executing CPU-bound tasks.
            func_mapping: Dictionary mapping function names to their callable implementations,
                used for deserializing tasks.

        Raises:
            Exception: Propagates any exception from task execution after logging and DLQ handling.
        """
        while self.running:
            task: Task | None = await self.dequeue(func_mapping=func_mapping)
            if task:
                try:
                    await task.run(
                        loop=loop,
                        broker=self.broker,
                    )
                except Exception as e:
                    self.logger.error("Error running task", e)
                    await self.broker.rpush(
                        task=task,
                        queue_key=self.broker.dlq_name,
                    )
            else:
                await asyncio.sleep(delay=0.1)  # Avoid busy-waiting if queue is empty
