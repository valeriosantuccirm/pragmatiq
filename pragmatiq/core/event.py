import asyncio
from types import CoroutineType
from typing import Any, Callable, Dict, List, Self, Type, TypeVar

from pragmatiq.core.queue import Queue
from pragmatiq.core.task import CPUTask, IOTask, Task

TaskType = Type[CPUTask | IOTask]
TaskInstanceType = CPUTask | IOTask


class Event:
    def __init__(
        self,
        data: Any,
    ) -> None:
        """Initialize an Event instance with arbitrary data.

        Args:
            data: Any data associated with the event.
        """
        self.data: Any = data


T = TypeVar("T", bound=Event)
HandlerType = Callable[[T], "CoroutineType[Any, Any, None]"]


class EventManager:
    _instance: Self | None = None
    _handlers: Dict[Type[Event], List[HandlerType]]

    def __new__(
        cls,
        *args: Any,
        **kwargs: Any,
    ) -> Self:
        """Create or return the singleton instance of EventManager.

        Ensures that only one instance of EventManager exists throughout the application lifecycle.
        If an instance does not exist, it creates one; otherwise, it returns the existing instance.

        Args:
            cls: The class object (EventManager).
            *args: Variable positional arguments (ignored if instance exists).
            **kwargs: Variable keyword arguments (ignored if instance exists).

        Returns:
            EventManager: The singleton instance of the class.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._handlers = {}
        return cls._instance

    def __init__(
        self,
        task_queue: Queue,
    ) -> None:
        """Initialize the EventManager with a task queue.

        Sets up the event handler registry and associates it with a task queue for task execution.

        Args:
            task_queue: The Queue instance to use for enqueuing tasks triggered by events.
        """
        self._handlers: Dict[Type[Event], List[HandlerType]] = {}
        self._task_queue: Queue = task_queue

    def register_event(
        self,
        event_type: Type[T],
        handler: HandlerType,
    ) -> None:
        """Register an event handler for a specific event type.

        Adds the handler to the list of handlers for the given event type, creating a new list if
        the event type is not yet registered.

        Args:
            event_type: The type of event to handle.
            handler: The async function to call when the event is dispatched.
        """
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)

    def dispatch(
        self,
        event: Event,
    ) -> None:
        """Dispatch an event to all registered handlers.

        Triggers the execution of all handlers registered for the event's type by creating
        asyncio tasks for each handler coroutine.

        Args:
            event: The Event instance to dispatch.
        """
        handlers: List[HandlerType] = self._handlers.get(type(event), [])
        for handler in handlers:
            asyncio.create_task(coro=handler(event))

    def bind(
        self,
        event_type: Type[T],
        task: Task,
    ) -> None:
        """Bind an event type to a specific task for automatic execution.

        Registers a handler that creates and enqueues a task when the specified event type is
        dispatched.

        Args:
            event_type: The type of event to bind to.
            task: The Task instance to enqueue when the event is dispatched.
        """
        self.register_event(
            event_type=event_type,
            handler=lambda event: self._add_task(
                task=self._create_task(
                    task=task,
                    event=event,
                )
            ),
        )

    def _create_task(
        self,
        task: Task,
        event: Event,
    ) -> TaskInstanceType:
        """Create a task instance based on the task type and event data.

        Constructs a new task (CPUTask or IOTask) by applying event data to callable arguments
        if present, preserving the original task's properties.

        Args:
            task: The Task instance to base the new task on.
            event: The Event instance providing data for argument evaluation.

        Returns:
            TaskInstanceType: A new CPUTask or IOTask instance configured with event data.
        """
        args = tuple(arg(event) if callable(arg) else arg for arg in task.args)
        if task.cpu_bound:
            return CPUTask(
                self._task_queue.broker,
                task.func,
                *args,
                priority=task.priority,
                timeout=task.timeout,
            )
        return IOTask(
            self._task_queue.broker,
            task.func,
            *args,
            priority=task.priority,
            timeout=task.timeout,
        )

    async def _add_task(
        self,
        task: TaskInstanceType,
    ) -> None:
        """Enqueue a task into the associated task queue.

        Asynchronously adds the task to the queue for execution.

        Args:
            task: The Task instance (CPUTask or IOTask) to enqueue.
        """
        await self._task_queue.enqueue(task=task)


class CPUEvent(Event):
    """Event for CPU-bound tasks with additional metadata."""

    def __init__(
        self,
        data: Any,
        cpu_cores_needed: int = 1,
    ) -> None:
        """Initialize a CPUEvent with data and CPU core requirements.

        Args:
            data: Any data associated with the event.
            cpu_cores_needed: Number of CPU cores required, defaults to 1.
        """
        super().__init__(data=data)
        self.cpu_cores_needed: int = cpu_cores_needed


class IOEvent(Event):
    """Event for I/O-bound tasks with additional metadata."""

    def __init__(
        self,
        data: Any,
        rate_limit: float = 1.0,
    ) -> None:
        """Initialize an IOEvent with data and a rate limit.

        Args:
            data: Any data associated with the event.
            rate_limit: Rate limit for I/O operations in operations per second, defaults to 1.0.
        """
        super().__init__(data=data)
        self.rate_limit: float = rate_limit
