import json
from datetime import datetime
from typing import Any, Dict, Self, overload

from loguru import logger
from redis.asyncio import Redis

from pragmatiq.broker.base import Broker
from pragmatiq.core.task import Task
from pragmatiq.enums import TaskState


class RedisBroker(Broker):
    _instance: Self | None = None

    def __new__(
        cls,
        *args: Any,
        **kwargs: Any,
    ) -> Self:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        queue_name: str = "pragmatiq:queue",
        dlq_name: str = "pragmatiq:queue:dlq",
        result_ttl: int = 3600,
    ) -> None:
        self.host: str = host
        self.port: int = port
        self.redis: Redis = Redis(
            host=self.host,
            port=self.port,
        )
        self.queue_name: str = queue_name
        self.dlq_name: str = dlq_name
        self.result_ttl: int = result_ttl

    @overload
    async def rpush(
        self,
        *,
        task: Task,
    ) -> None:
        await self.redis.rpush(f"{self.queue_name}:{task.priority}", task.serialize())  # type: ignore[awaitable]

    @overload
    async def rpush(
        self,
        task: Task,
        *,
        queue_key: str | None = None,
    ) -> None:
        await self.redis.rpush(f"{queue_key}:{task.priority}", task.serialize())  # type: ignore[awaitable]

    async def rpush(
        self,
        task: Task,
        *,
        queue_key: str | None = None,
    ) -> None:
        if queue_key:
            await self.redis.rpush(f"{queue_key}:{task.priority}", task.serialize())  # type: ignore[awaitable]
        await self.redis.rpush(f"{self.queue_name}:{task.priority}", task.serialize())  # type: ignore[awaitable]
        logger.debug(
            f"RedisBroker: pushed task '{task.func.__name__}' to {queue_key if queue_key else self.queue_name}"
        )

    async def close(self) -> None:
        await self.redis.close()

    async def lpop(
        self,
        priority: int,
    ) -> bytes | None:
        return await self.redis.lpop(f"{self.queue_name}:{priority}")  # type: ignore[awaitable]

    async def store_result(
        self,
        task_id: str,
        result: Any,
        error: Exception | None,
        state: TaskState,
    ) -> None:
        """Store task result in Redis."""
        logger.debug(f"RedisBroker: storing result of task ID: {task_id}")
        result_data: Dict[str, Any] = {
            "task_id": task_id,
            "status": state.value,
            "result": result,
            "error": str(error) if error else None,
            "timestamp": datetime.now().isoformat(),
        }
        await self.redis.setex(
            name=f"{self.queue_name}:result:{task_id}",
            time=self.result_ttl,
            value=json.dumps(result_data),
        )

    async def get_result(
        self,
        task_id: str,
    ) -> Dict[str, Any]:
        """Retrieve task result from Redis."""
        data: Any = await self.redis.get(name=f"{self.queue_name}:result:{task_id}")
        if not data:
            return {
                "status": "pending",
                "result": None,
                "error": None,
                "timestamp": None,
            }
        return json.loads(data)
