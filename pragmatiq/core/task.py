import asyncio
import time
import traceback
from typing import TYPE_CHECKING, Any, Callable, Dict, Tuple
from uuid import uuid4

import msgpack
import ray
from loguru import logger
from opentracing import global_tracer
from ray._private.worker import RemoteFunctionNoArgs

from pragmatiq.enums import TaskState
from pragmatiq.metrics.settings import (
    CPU_BOUND_TASK_DURATION,
    CPU_BOUND_TASKS_TOTAL,
    TASK_DURATION,
    TASK_STATE_COMPLETED,
    TASK_STATE_FAILED,
    TASK_STATE_RUNNING,
    TASK_TIMEOUTS_TOTAL,
)
from pragmatiq.types import F, R
from pragmatiq.utils.serializers import deserialize_args, serialize_args

if TYPE_CHECKING:
    from pragmatiq.broker.redis_broker import RedisBroker


class Task:
    def __init__(
        self,
        broker: "RedisBroker",
        func: Callable[..., R],
        *args: Any,
        priority: int = 1,
        timeout: int = 30,
        cpu_bound: bool = False,
        **kwargs: Any,
    ) -> None:
        """Initialize a Task instance with a function and its execution parameters.

        Args:
            func: The callable to execute when the task runs.
            *args: Variable arguments to pass to the function.
            priority: Priority level of the task (0-9, 0 is highest), defaults to 1.
            timeout: Maximum execution time in seconds, defaults to 30.
            cpu_bound: Whether the task is CPU-bound (True) or I/O-bound (False), defaults to False.
        """
        self.func: Callable[..., Any] = func
        self.args: Tuple[Any, ...] = args
        self.priority: int = priority
        self.timeout: int = timeout
        self.cpu_bound: bool = cpu_bound
        self.state = TaskState.PENDING
        self._start_time: float | None = None
        self.task_id = str(uuid4())
        self.broker: "RedisBroker" = broker
        self.kwargs: Dict[str, Any] = kwargs

    async def store_result(
        self,
        result: Any,
        error: Exception | None,
        state: TaskState,
    ) -> None:
        """Store result through backend"""
        await self.broker.store_result(
            task_id=self.task_id,
            result=result,
            error=error,
            state=state,
        )

    async def run(
        self,
        loop: asyncio.AbstractEventLoop,
        broker: "RedisBroker",
    ) -> Any:
        """Execute the task and manage its lifecycle with tracing and metrics.

        Runs the task's function, updates its state, records metrics, and handles failures by
        optionally enqueuing to a dead-letter queue (DLQ). Uses Jaeger for tracing and Prometheus
        for metrics.

        Args:
            loop: The asyncio event loop to use for execution.
            pool: The process pool for CPU-bound tasks.
            dlq_redis: Optional Redis client for DLQ enqueuing.
            dlq_key: Optional key for the DLQ in Redis.

        Returns:
            Any: The result of the task execution, or None if it fails.

        Raises:
            Exception: Propagates any exception from task execution, after logging and handling failure.
        """
        logger.debug("Task: running task...")
        with global_tracer().start_active_span(
            operation_name=f"execute_task.{self.func.__name__}",
            tags={"cpu_bound": self.cpu_bound},
        ) as scope:
            try:
                self.state = TaskState.RUNNING
                TASK_STATE_RUNNING.inc()
                self._start_time = time.time()

                logger.debug(f"Task: start execution of task: {self.func.__name__}")
                result: Any = await asyncio.wait_for(
                    fut=self._execute(loop=loop),
                    timeout=self.timeout,
                )

                duration: float = time.time() - self._start_time
                if self.cpu_bound:
                    CPU_BOUND_TASKS_TOTAL.inc()
                    CPU_BOUND_TASK_DURATION.observe(amount=duration)
                else:
                    TASK_DURATION.observe(amount=duration)

                self.state = TaskState.COMPLETED
                logger.debug(
                    f"Task: task completed; storing result of: {self.func.__name__}"
                )
                await self.store_result(
                    result=result,
                    error=None,
                    state=self.state,
                )
                TASK_STATE_COMPLETED.inc()
                scope.span.set_tag(key="state", value=self.state.value)
                return result

            except Exception as e:
                self.state = TaskState.FAILED
                scope.span.set_tag(key="error", value=True)
                scope.span.log_kv(
                    key_values={
                        "event": "error",
                        "error.object": e,
                        "error.stack": traceback.format_exc(),
                    }
                )
                logger.debug(f"Task: task '{self.func.__name__}' failed")
                self._handle_failure(
                    error=e,
                    broker=broker,
                )
                scope.span.set_tag(key="state", value=self.state.value)
                await self.store_result(
                    result=None,
                    error=e,
                    state=self.state,
                )
                return None

    def _handle_failure(
        self,
        error: Exception,
        broker: "RedisBroker",
    ) -> None:
        """Handle task failure by updating state and optionally enqueuing to a DLQ.

        Updates the task state to FAILED, increments failure metrics, and enqueues the task
        to a dead-letter queue if Redis and a key are provided.

        Args:
            error: The exception that caused the failure.
            dlq_redis: Optional Redis client for DLQ enqueuing.
            dlq_key: Optional key for the DLQ in Redis.
        """
        self.state = TaskState.FAILED
        TASK_STATE_FAILED.inc()
        if isinstance(error, asyncio.TimeoutError):
            TASK_TIMEOUTS_TOTAL.inc()
        logger.debug(f"Task: moving task '{self.func.__name__}' to DLQ")
        asyncio.create_task(
            coro=broker.rpush(
                task=self,
                queue_key=broker.dlq_name,
            )
        )

    async def _execute(
        self,
        loop: asyncio.AbstractEventLoop,
    ) -> Any:
        """Execute the task's function; must be implemented by subclasses.

        Abstract method to be overridden by subclasses (CPUTask, IOTask) to define how the
        task's function is executed.

        Args:
            loop: The asyncio event loop to use for execution.
            pool: The process pool for CPU-bound tasks.

        Returns:
            Any: The result of the task execution.

        Raises:
            NotImplementedError: If not overridden by a subclass.
        """
        raise NotImplementedError()

    @classmethod
    def deserialize(
        cls,
        data: bytes,
        func_mapping: Dict[str, F],
        broker: "RedisBroker",
    ) -> "Task":
        """Deserialize a task from bytes using a function mapping.

        Reconstructs a Task instance (either CPUTask or IOTask) from serialized data, using
        the provided function mapping to recover callable objects.

        Args:
            data: The serialized task data in bytes (msgpack format).
            func_mapping: Dictionary mapping function names to their callable implementations.

        Returns:
            Task: An instance of CPUTask or IOTask based on the serialized data.
        """
        logger.debug("Task: deserializing task...")
        unpacked: Dict[str, Any] = msgpack.unpackb(data)
        func: F = func_mapping[unpacked["func_name"]]
        args: Tuple[Any, ...] = deserialize_args(
            args=unpacked["args"],
            func_mapping=func_mapping,
        )
        logger.debug(f"Task: deserialized: {unpacked}")
        if unpacked["cpu_bound"]:
            return CPUTask(
                broker,
                func,
                *args,
                priority=unpacked["priority"],
                timeout=unpacked["timeout"],
            )
        else:
            return IOTask(
                broker,
                func,
                *args,
                priority=unpacked["priority"],
                timeout=unpacked["timeout"],
            )

    def serialize(self) -> bytes | None:
        """Serialize the task into bytes using msgpack.

        Converts the task's attributes into a serializable format for storage or transmission.

        Returns:
            bytes | None: The serialized task data in bytes, or None if serialization fails.
        """
        logger.debug(f"Task: serializing task: {self.func.__name__}")
        return msgpack.packb(
            {
                "func_name": self.func.__name__,
                "func_module": self.func.__module__,
                "args": serialize_args(self.args),
                "cpu_bound": self.cpu_bound,
                "priority": self.priority,
                "timeout": self.timeout,
            }
        )


class CPUTask(Task):
    def __init__(
        self,
        broker: "RedisBroker",
        func: Callable[..., R],
        *args: Any,
        priority: int = 1,
        timeout: int = 30,
        **kwargs: Any,
    ) -> None:
        """Initialize a CPU-bound task.

        Args:
            func: The synchronous callable to execute in a process pool.
            *args: Variable arguments to pass to the function.
            priority: Priority level of the task (0-9, 0 is highest), defaults to 1.
            timeout: Maximum execution time in seconds, defaults to 30.
        """
        super().__init__(
            broker,
            func,
            *args,
            priority=priority,
            timeout=timeout,
            cpu_bound=True,
            **kwargs,
        )
        logger.debug(f"Task: CPU task to Ray remote: {self.func.__name__}")
        if not kwargs:
            self.remote_func: RemoteFunctionNoArgs[Any] = ray.remote(func)
        else:
            self.remote_func: RemoteFunctionNoArgs[Any] = ray.remote(**kwargs)(func)

    async def _execute(
        self,
        loop: asyncio.AbstractEventLoop,
    ) -> Any:
        """Execute the CPU-bound task in a process pool."""
        if asyncio.iscoroutinefunction(func=self.func):
            raise ValueError("CPU tasks must be synchronous")
        try:
            logger.debug(f"Task: executing Ray future: {self.func.__name__}")
            future: ray.ObjectRef[Any] = self.remote_func.remote(*self.args)
            result = await loop.run_in_executor(
                None,
                ray.get,  # This will be _execute_cpu_task
                future,  # Args will include the original func and its args
            )
            logger.debug("Task: CPU task executed. Returning result...")
            return result
        except Exception as e:
            logger.error(f"Error type: {type(e)}. Error msg: {e}")
            raise e


class IOTask(Task):
    def __init__(
        self,
        broker: "RedisBroker",
        func: Callable[..., R],
        *args: Any,
        priority: int = 1,
        timeout: int = 30,
        **kwargs: Any,
    ) -> None:
        """Initialize an I/O-bound task.

        Args:
            func: The asynchronous callable to execute directly.
            *args: Variable arguments to pass to the function.
            priority: Priority level of the task (0-9, 0 is highest), defaults to 1.
            timeout: Maximum execution time in seconds, defaults to 30.
        """
        super().__init__(
            broker,
            func,
            *args,
            priority=priority,
            timeout=timeout,
            cpu_bound=False,
            **kwargs,
        )

    async def _execute(
        self,
        loop: asyncio.AbstractEventLoop,
    ) -> Any:
        """Execute the I/O-bound task directly.

        Runs the asynchronous function using await, bypassing the process pool.

        Args:
            loop: The asyncio event loop to use for execution (unused but included for interface consistency).
            pool: The process pool (unused but included for interface consistency).

        Returns:
            Any: The result of the function execution.

        Raises:
            Exception: Propagates any exception raised during execution, after logging.
        """
        try:
            logger.debug(f"Task: executing IO task: {self.func.__name__}")
            return await self.func(*self.args)
        except Exception as e:
            logger.error(f"Error type: {type(e)}. Error msg: {e}")
            raise e
