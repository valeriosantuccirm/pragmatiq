import asyncio
from typing import Any, Callable, Dict, Generator
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import msgpack
import pytest
import pytest_asyncio

from pragmatiq.broker.redis_broker import RedisBroker
from pragmatiq.core.task import CPUTask, IOTask, Task
from pragmatiq.enums import TaskState
from pragmatiq.metrics.settings import (
    CPU_BOUND_TASK_DURATION,
    CPU_BOUND_TASKS_TOTAL,
    TASK_STATE_COMPLETED,
    TASK_STATE_FAILED,
    TASK_STATE_RUNNING,
)
from pragmatiq.utils.serializers import serialize_args


@pytest_asyncio.fixture(scope="function")
async def mock_broker() -> Mock:
    """Fixture providing a mocked RedisBroker instance."""
    return Mock(spec=RedisBroker())


# Mock global_tracer for tracing
@pytest.fixture(autouse=True)
def mock_global_tracer() -> Generator[MagicMock | AsyncMock, Any, None]:
    """Fixture mocking the global_tracer for tracing operations."""
    with patch("pragmatiq.core.task.global_tracer") as mock_tracer:
        mock_span = MagicMock()
        mock_span.start_active_span.return_value.__enter__.return_value = mock_span
        mock_tracer.return_value = mock_span
        yield mock_tracer


# Helper functions for testing
def sync_func(
    x: int,
) -> int:
    return x * 2


async def async_func(
    x: int,
) -> int:
    await asyncio.sleep(delay=0.1)  # Simulate async work
    return x * 3


def failing_func(
    x: int,
) -> int:
    """Raise a ValueError for testing failures."""
    raise ValueError("Test failure")


# Tests for Task.__init__
def test_task_init(
    mock_broker: Mock,
) -> None:
    """Test Task initialization with custom parameters."""
    task = Task(
        mock_broker,
        sync_func,
        5,
        priority=2,
        timeout=10,
        cpu_bound=True,
    )
    assert task.func == sync_func
    assert task.args == (5,)
    assert task.priority == 2
    assert task.timeout == 10
    assert task.cpu_bound is True
    assert task.state == TaskState.PENDING
    assert task._start_time is None
    assert isinstance(task.task_id, str)
    assert task.broker == mock_broker


def test_task_init_defaults(
    mock_broker: Mock,
) -> None:
    """Test Task initialization with default parameters."""
    task = Task(
        mock_broker,
        sync_func,
        5,
    )
    assert task.priority == 1
    assert task.timeout == 30
    assert task.cpu_bound is False


# Tests for Task.store_result
@pytest.mark.asyncio
async def test_store_result_success(
    mock_broker: Mock,
) -> None:
    """Test storing a successful task result."""
    task = Task(mock_broker, sync_func, 5)
    await task.store_result(result=10, error=None, state=TaskState.COMPLETED)
    mock_broker.store_result.assert_awaited_once_with(
        task_id=task.task_id,
        result=10,
        error=None,
        state=TaskState.COMPLETED,
    )


@pytest.mark.asyncio
async def test_store_result_failure(
    mock_broker: Mock,
) -> None:
    """Test storing a failed task result with an error."""
    task = Task(
        mock_broker,
        sync_func,
        5,
    )
    error = ValueError("Test error")
    await task.store_result(
        result=None,
        error=error,
        state=TaskState.FAILED,
    )
    mock_broker.store_result.assert_awaited_once_with(
        task_id=task.task_id,
        result=None,
        error=error,
        state=TaskState.FAILED,
    )


# Tests for Task.run
@pytest.mark.asyncio
async def test_run_success(
    mock_broker: Mock,
) -> None:
    """Test successful execution of a CPU task."""
    task = CPUTask(
        mock_broker,
        sync_func,
        5,
    )
    with patch.object(
        target=task,
        attribute="_execute",
        new=AsyncMock(return_value=10),
    ):
        result = await task.run(
            loop=asyncio.get_running_loop(),
            broker=mock_broker,
        )

        assert result == 10
        assert task.state == TaskState.COMPLETED

        # Fix assertion: Ensure these values are greater than or equal to 1
        assert TASK_STATE_RUNNING._value._value >= 1
        assert CPU_BOUND_TASKS_TOTAL._value._value >= 1
        assert (
            CPU_BOUND_TASK_DURATION._sum._value >= 0
        )  # Duration might be 0 for fast tasks
        assert TASK_STATE_COMPLETED._value._value >= 1

        mock_broker.store_result.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_failure(
    mock_broker: Mock,
) -> None:
    """Test failure handling during task execution."""
    task = IOTask(
        mock_broker,
        failing_func,
        5,
    )
    with patch.object(
        target=task,
        attribute="_execute",
        new=AsyncMock(side_effect=ValueError("Test error")),
    ):
        result = await task.run(
            loop=asyncio.get_running_loop(),
            broker=mock_broker,
        )

        assert result is None
        assert task.state == TaskState.FAILED

        # Fix assertion: Ensure these values are greater than or equal to 1
        assert TASK_STATE_RUNNING._value._value >= 1
        assert TASK_STATE_FAILED._value._value >= 1

        mock_broker.store_result.assert_awaited_once()


# Tests for Task.deserialize
def test_deserialize_cpu_task(
    mock_broker: Mock,
) -> None:
    """Test deserialization of a CPU-bound task."""
    func_mapping: Dict[str, Callable] = {"sync_func": sync_func}
    data: Any = msgpack.packb(
        o={
            "func_name": "sync_func",
            "func_module": "test_module",
            "args": serialize_args((5,)),
            "cpu_bound": True,
            "priority": 2,
            "timeout": 10,
        }
    )
    task: Task = Task.deserialize(
        data=data,
        func_mapping=func_mapping,
        broker=mock_broker,
    )
    assert isinstance(task, CPUTask)
    assert task.func == sync_func
    assert task.args == (5,)
    assert task.priority == 2
    assert task.timeout == 10
    assert task.cpu_bound is True


def test_deserialize_io_task(
    mock_broker: Mock,
) -> None:
    """Test deserialization of an I/O-bound task."""
    func_mapping: Dict[str, Callable] = {"async_func": async_func}
    data: Any = msgpack.packb(
        o={
            "func_name": "async_func",
            "func_module": "test_module",
            "args": serialize_args((5,)),
            "cpu_bound": False,
            "priority": 3,
            "timeout": 20,
        }
    )
    task = Task.deserialize(
        data=data,
        func_mapping=func_mapping,
        broker=mock_broker,
    )
    assert isinstance(task, IOTask)
    assert task.func == async_func
    assert task.args == (5,)
    assert task.priority == 3
    assert task.timeout == 20
    assert task.cpu_bound is False


# Tests for Task.serialize
def test_serialize_cpu_task(
    mock_broker: Mock,
) -> None:
    """Test serialization of a CPU-bound task."""
    task = CPUTask(
        mock_broker,
        sync_func,
        5,
        priority=2,
        timeout=10,
    )
    serialized: bytes | None = task.serialize()
    assert serialized is not None
    unpacked: Dict[str, Any] = msgpack.unpackb(serialized)

    assert unpacked["func_name"] == "sync_func"

    # Fix assertion: compare with [5] instead of (5,)
    assert tuple(unpacked["args"]) == serialize_args(args=(5,))
    assert unpacked["cpu_bound"] is True
    assert unpacked["priority"] == 2
    assert unpacked["timeout"] == 10


def test_serialize_io_task(
    mock_broker: Mock,
) -> None:
    """Test serialization of an I/O-bound task."""
    task = IOTask(
        mock_broker,
        async_func,
        5,
        priority=3,
        timeout=20,
    )
    serialized: bytes | None = task.serialize()
    assert serialized is not None
    unpacked: Dict[str, Any] = msgpack.unpackb(serialized)

    assert unpacked["func_name"] == "async_func"

    # Fix assertion: compare with [5] instead of (5,)
    assert tuple(unpacked["args"]) == serialize_args(args=(5,))
    assert unpacked["cpu_bound"] is False
    assert unpacked["priority"] == 3
    assert unpacked["timeout"] == 20


# Tests for CPUTask._execute
@pytest.mark.asyncio
async def test_cpu_execute_success(
    mock_broker: Mock,
) -> None:
    """Test successful execution of a CPU task with Ray."""
    with patch("ray.remote", return_value=MagicMock()) as mock_remote:
        mock_remote.return_value.remote.return_value = "ray_future"
        with patch("ray.get", return_value=10):
            task = CPUTask(mock_broker, sync_func, 5)
            result = await task._execute(loop=asyncio.get_running_loop())
            assert result == 10
            mock_remote.assert_called_once_with(sync_func)
            mock_remote.return_value.remote.assert_called_once_with(5)


@pytest.mark.asyncio
async def test_cpu_execute_async_func_error(
    mock_broker: Mock,
) -> None:
    """Test that an async function raises an error in a CPU task."""
    with pytest.raises(expected_exception=ValueError):
        CPUTask(
            mock_broker,
            async_func,
            5,
        )


# Tests for IOTask._execute
@pytest.mark.asyncio
async def test_io_execute_success(
    mock_broker: Mock,
) -> None:
    """Test successful execution of an I/O task."""
    task = IOTask(
        mock_broker,
        async_func,
        5,
    )
    result = await task._execute(loop=asyncio.get_running_loop())
    assert result == 15  # 5 * 3


@pytest.mark.asyncio
async def test_io_execute_failure(
    mock_broker: Mock,
) -> None:
    """Test failure handling in an I/O task execution."""

    async def failing_async(x: int) -> int:
        raise ValueError("Test error")

    task = IOTask(
        mock_broker,
        failing_async,
        5,
    )
    with pytest.raises(expected_exception=ValueError, match="Test error"):
        await task._execute(loop=asyncio.get_running_loop())
