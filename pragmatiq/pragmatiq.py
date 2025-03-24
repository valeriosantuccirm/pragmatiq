import asyncio
import os
from asyncio import AbstractEventLoop
from asyncio import Task as asyncioTask
from contextlib import asynccontextmanager
from functools import wraps
from types import CoroutineType
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Dict,
    Literal,
    Optional,
    Self,
    Type,
    TypeVar,
)

import ray
from jaeger_client.config import Config
from jaeger_client.tracer import Tracer
from loguru import logger
from opentracing import set_global_tracer
from redis import RedisError

from pragmatiq.broker.redis_broker import RedisBroker
from pragmatiq.config import PragmatiQConfig
from pragmatiq.core.event import Event, EventManager
from pragmatiq.core.queue import Queue
from pragmatiq.core.task import CPUTask, IOTask
from pragmatiq.metrics.settings import start_prometheus, update_task_queue_length
from pragmatiq.types import R

T = TypeVar("T", bound=Event)
HandlerType = Callable[[T], "CoroutineType[Any, Any, None]"]
TaskType = Type[CPUTask | IOTask]
TaskInstanceType = CPUTask | IOTask

TASK_MAP: Dict[str, Type[CPUTask] | Type[IOTask]] = {
    "cpu": CPUTask,
    "io": IOTask,
}


class PragmatiQ:
    """A singleton class for managing asynchronous task queues and event handling with monitoring.

    PragmatiQ provides a framework for enqueuing and executing CPU-bound and I/O-bound tasks
    using a Redis-backed queue, alongside an event management system for dispatching and
    handling events. It integrates Prometheus for metrics and Jaeger for tracing, enabling
    comprehensive monitoring of task execution and event processing. The class operates
    as a singleton to ensure a single instance manages the queue and event system across
    an application.

    Attributes:
        __initialized (bool): Internal flag to prevent re-initialization of the singleton.
        _instance (Optional[PragmatiQ]): The singleton instance of the class.
        tracer (Optional[Tracer]): Jaeger tracer for distributed tracing, initialized on start.
        running (bool): Indicates whether the queue and background tasks are active.
        config (PragmatiQConfig): Configuration object with settings for Redis, Prometheus, etc.
        queue (Queue): Redis-backed task queue for managing task execution.
        event_manager (EventManager): Manager for registering and dispatching events.
        loop (AbstractEventLoop): The asyncio event loop used for task scheduling.
        pool (ProcessPoolExecutor): Process pool for executing CPU-bound tasks.
        func_mapping (Dict[str, Callable[..., Any]]): Mapping of function names to their implementations.

    Examples:
        Basic task and event setup:
        ```python
        async def main():
            prag = PragmatiQ()
            @prag.collector("io")
            async def io_task(data: str) -> str:
                return f"Processed {data}"

            @prag.event_handler(Event)
            async def handle_event(event: Event):
                print(f"Event data: {event.data}")

            async with prag.lifespan():
                await io_task("test")  # Enqueues an I/O task
                prag.dispatch_event(Event("test_event"))  # Dispatches an event
                await asyncio.sleep(1)  # Allow tasks to process

        asyncio.run(main())
        ```
        Advanced usage with binding events to tasks:
        ```python
        from pragmatiq.core.event import Event

        async def main():
            prag = PragmatiQ(config=PragmatiQConfig(redis_host="localhost", redis_port=6379))

            async def process_data(data: str) -> str:
                return f"Processed {data}"

            class CustomEvent(Event):
                pass

            prag.bind_event_to_task("io", CustomEvent, process_data, "example", priority=2)

            async with prag.lifespan():
                prag.dispatch_event(CustomEvent("test"))
                await asyncio.sleep(2)  # Wait for processing

        asyncio.run(main())
        ```

    Raises:
        RuntimeError: If get_instance is called before initialization or if the singleton is misused.
        NotImplementedError: If a CPU task collector is used (pending implementation).

    """

    __initialized: bool = False
    _instance: Optional["PragmatiQ"] = None
    tracer: Tracer | None = None
    running: bool = False

    def __new__(
        cls,
        *args: Any,
        **kwargs: Any,
    ) -> Self | "PragmatiQ":
        """Create or return the singleton instance of PragmatiQ.

        Ensures that only one instance of PragmatiQ exists throughout the application lifecycle.
        If an instance does not exist, it creates one; otherwise, it returns the existing instance.

        Args:
            cls: The class object (PragmatiQ).
            *args: Variable positional arguments (ignored if instance exists).
            **kwargs: Variable keyword arguments (ignored if instance exists).

        Returns:
            PragmatiQ: The singleton instance of the PragmatiQ class.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.__initialized = False
        return cls._instance

    def __init__(
        self,
        config: Optional[PragmatiQConfig] = None,
    ) -> None:
        """Initialize the PragmatiQ instance with configuration.

        Sets up the task queue, event manager, event loop, process pool, and function mapping.
        This method is only executed once for the singleton instance, subsequent calls are no-ops.

        Args:
            config: Optional configuration object; defaults to a new PragmatiQConfig if None.
        """
        if self.__initialized:
            return
        self.__initialized = True
        self.config: PragmatiQConfig = config or PragmatiQConfig()
        self.broker = RedisBroker(
            host=self.config.redis_host,
            port=self.config.redis_port,
            queue_name=self.config.queue_name,
            dlq_name=self.config.dlq_name,
            result_ttl=self.config.result_ttl,
        )
        self.queue = Queue(broker=self.broker)
        self.event_manager = EventManager(task_queue=self.queue)
        self.loop: AbstractEventLoop = asyncio.get_event_loop()
        self.func_mapping: Dict[str, Callable[..., Any]] = {}

    @classmethod
    def get_instance(cls) -> "PragmatiQ":
        """Retrieve the singleton instance of PragmatiQ.

        Provides access to the initialized PragmatiQ instance for use across the application.

        Returns:
            PragmatiQ: The singleton instance of PragmatiQ.

        Raises:
            RuntimeError: If PragmatiQ has not been initialized prior to calling this method.
        """
        if cls._instance is None:
            raise RuntimeError("PragmatiQ not initialized")
        return cls._instance

    @asynccontextmanager
    async def lifespan(self) -> AsyncGenerator[None, Any]:
        """Manage the lifecycle of PragmatiQ asynchronously.

        Starts the queue and background tasks upon entry and shuts them down upon exit.
        This context manager ensures proper resource management for the PragmatiQ instance.

        Yields:
            None: Allows the caller to execute code while PragmatiQ is running.

        Raises:
            Exception: Propagates any exception that occurs during startup or shutdown.
        """
        try:
            await self._start()
            yield
            await self._shutdown()
        except Exception as e:
            raise e

    def _init_tracer(
        self,
        service_name: str,
    ) -> Tracer | None:
        """Initialize the Jaeger tracer for distributed tracing.

        Configures and initializes a Jaeger tracer with constant sampling for the specified service.

        Args:
            service_name: The name of the service to trace.

        Returns:
            Tracer | None: The initialized Jaeger tracer, or None if initialization fails.
        """
        config = Config(
            config={
                "sampler": {
                    "type": "const",
                    "param": 1,
                },
                "logging": True,
            },
            service_name=service_name,
            validate=True,
        )
        tracer: Tracer | None = config.initialize_tracer()
        if tracer:
            set_global_tracer(value=tracer)  # Set as global tracer
            return tracer

    async def _start(self) -> None:
        """Start the PragmatiQ queue and background tasks.

        Initializes Prometheus metrics, Jaeger tracing, and launches tasks for queue processing
        and queue length monitoring. This method is idempotent and only executes if not already running.
        """
        if self.running:
            return
        self.running = True

        # Initialize Ray
        ray.init(num_cpus=self.config.process_pool_size or os.cpu_count())

        start_prometheus(port=self.config.prometheus_port)
        if not self.tracer:
            self.tracer = self._init_tracer(service_name=self.config.service_name)
        self._queue_task: asyncioTask[None] = asyncio.create_task(
            coro=self.queue.run_tasks(
                loop=self.loop,
                func_mapping=self.func_mapping,
            )
        )
        self._queue_length_task: asyncioTask[None] = asyncio.create_task(
            coro=self._update_queue_length_task(),
        )

    async def _shutdown(self) -> None:
        """Shut down the PragmatiQ queue and background tasks.

        Stops the queue, cancels background tasks, shuts down the process pool, and closes the Redis connection.
        This method is idempotent and only executes if currently running.
        """
        if not self.running:
            return
        self.running = False
        self.queue.running = False

        # Shutdown Ray
        ray.shutdown()

        await self.queue.broker.close()
        if hasattr(self, "_queue_task"):
            self._queue_task.cancel()
            try:
                await self._queue_task
            except (asyncio.CancelledError, RedisError):
                pass
        if hasattr(self, "_queue_length_task"):
            self._queue_length_task.cancel()
            try:
                await self._queue_length_task
            except (asyncio.CancelledError, RedisError):
                pass

    async def _update_queue_length_task(self) -> None:
        """Periodically update the task queue length metric.

        Runs an infinite loop while the queue is active, updating Prometheus metrics at intervals
        specified by `config.cron_check_interval`.
        """
        while self.queue.running:
            await update_task_queue_length(task_queue=self.queue)
            await asyncio.sleep(delay=self.config.cron_check_interval)

    def collector(
        self,
        type_: Literal["cpu", "io"],
        *,
        priority: int = 1,
        timeout: int = 30,
        **kwargs: Any,
    ) -> Callable[
        [Callable[..., Any]], Callable[..., "CoroutineType[Any, Any, Dict[str, str]]"]
    ]:
        """Decorate a function to enqueue it as a task.

        Registers the function in the function mapping and returns a wrapper that enqueues it as a task
        of the specified type when called.

        Args:
            type_: Type of task ("cpu" or "io").
            priority: Priority level of the task (0-9, 0 is highest), defaults to 1.
            timeout: Maximum execution time in seconds, defaults to 30.

        Returns:
            Callable: A decorator that wraps the function to enqueue it as a task.

        Raises:
            NotImplementedError: If `type_` is "cpu" (CPU tasks are not yet implemented).
        """

        def decorator(
            func: Callable[..., R],
        ) -> Callable[..., "CoroutineType[Any, Any, Dict[str, str]]"]:
            self.func_mapping[func.__name__] = func
            logger.debug(f"PragmatiQ: mapped func: {func.__name__}")

            @wraps(wrapped=func)
            async def wrapper(*args: Any) -> Dict[str, str]:
                cls_: TaskType = TASK_MAP[type_]
                task: TaskInstanceType = cls_(
                    self.queue.broker,
                    func,
                    *args,
                    priority=priority,
                    timeout=timeout,
                    **kwargs,
                )
                if self.tracer:
                    with self.tracer.start_active_span(
                        operation_name=f"enqueue_{type_}_task.{func.__name__}"
                    ) as scope:
                        scope.span.set_tag(key="priority", value=priority)
                        scope.span.set_tag(key="timeout", value=timeout)
                        scope.span.set_tag(key="function", value=func.__name__)
                        await self.queue.enqueue(task=task)
                        logger.debug(
                            f"PragmatiQ: enqueued with tracer func: {func.__name__}"
                        )
                else:
                    await self.queue.enqueue(task=task)
                    logger.debug(
                        f"PragmatiQ: enqueued without tracer func: {func.__name__}"
                    )
                return {
                    "message": f"{type_.upper()} task enqueued",
                    "task_id": task.task_id,
                }

            return wrapper

        return decorator

    def event_handler(
        self,
        event_type: Type[T],
    ) -> Callable[[Callable[[T], Any]], HandlerType]:
        """Decorate a function to register it as an event handler.

        Wraps the function to handle events of the specified type and registers it with the event manager.

        Args:
            event_type: The type of event to handle.

        Returns:
            Callable: A decorator that registers the function as an event handler.
        """

        def decorator(func: Callable[[T], Any]) -> HandlerType:
            @wraps(wrapped=func)
            async def wrapper(event: T) -> None:
                await func(event)

            self.register_event_handler(
                event_type=event_type,
                handler=wrapper,
            )
            return wrapper

        return decorator

    def bind_event_to_task(
        self,
        type_: Literal["cpu", "io"],
        event_type: Type[Event],
        func: Callable[..., R],
        *args: Any,
        priority: int = 1,
        timeout: int = 30,
    ) -> None:
        """Bind an event type to a task for automatic execution on dispatch.

        Creates a task of the specified type and binds it to the event type, so it executes when the event is dispatched.

        Args:
            type_: Type of task ("cpu" or "io").
            event_type: The type of event to bind to.
            func: The function to execute when the event is dispatched.
            *args: Arguments to pass to the function.
            priority: Priority level of the task (0-9), defaults to 1.
            timeout: Maximum execution time in seconds, defaults to 30.
        """
        cls_: TaskType = TASK_MAP[type_]
        task: TaskInstanceType = cls_(
            self.queue.broker,
            func,
            *args,
            priority=priority,
            timeout=timeout,
        )
        self.event_manager.bind(
            event_type=event_type,
            task=task,
        )

    def register_event_handler(
        self,
        event_type: Type[T],
        handler: HandlerType,
    ) -> None:
        """Register an event handler for a specific event type.

        Adds the handler to the event manager for the given event type.

        Args:
            event_type: The type of event to handle.
            handler: The async function to call when the event is dispatched.
        """
        self.event_manager.register_event(
            event_type=event_type,
            handler=handler,
        )

    def dispatch_event(
        self,
        event: Event,
    ) -> None:
        """Dispatch an event to all registered handlers.

        Triggers the execution of all handlers registered for the event's type.

        Args:
            event: The event instance to dispatch.
        """
        self.event_manager.dispatch(event=event)

    async def get_task_result(
        self,
        task_id: str,
    ) -> Dict[str, Any]:
        """Get task result by ID"""
        return await self.queue.broker.get_result(task_id=task_id)
