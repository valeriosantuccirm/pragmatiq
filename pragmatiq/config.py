from dataclasses import dataclass
from typing import Any, Generator, Tuple


@dataclass
class PragmatiQConfig:
    """Configuration settings for the PragmatiQ task and event management system.

    Attributes:
        cron_check_interval (int): Interval in seconds for periodic queue length checks. Defaults to 5.
        default_timeout (int): Default timeout for tasks in seconds. Defaults to 30.
        dlq_name (str): Name of the dead-letter queue in Redis. Defaults to "pragmatiq:queue:dlq".
        jaeger_auth (Tuple[str, str] | None): Tuple of (username, password) for Jaeger basic auth. Defaults to None.
        jaeger_host (str): Hostname or IP address of the Jaeger server. Defaults to "localhost".
        jaeger_port (int | None): Port number of the Jaeger query service. Defaults to 16686.
        process_pool_size (int): Number of workers in the process pool. Defaults to 4.
        prometheus_auth (Tuple[str, str] | None): Tuple of (username, password) for Prometheus basic auth. Defaults to None.
        prometheus_host (str): Hostname or IP address of the Prometheus server. Defaults to "localhost".
        prometheus_port (int | None): Port number of the Prometheus metrics server. Defaults to 9090.
        queue_name (str): Unique name for the task queue. Defaults to "pragmatiq:queue".
        redis_host (str): Hostname or IP address of the Redis server. Defaults to "localhost".
        redis_port (int): Port number of the Redis server. Defaults to 6379.
        result_tt (int): Time-to-live in seconds for task results in Redis. Defaults to 3600.
        service_name (str): Name of the service for Jaeger tracing. Defaults to "pragmatiq".
    """

    cron_check_interval: int = 5
    default_timeout: int = 30
    dlq_name: str = "pragmatiq:queue:dlq"
    jaeger_auth: Tuple[str, str] | None = None
    jaeger_host: str | None = None
    jaeger_port: int | None = None
    process_pool_size: int = 4
    prometheus_auth: Tuple[str, str] | None = None
    prometheus_host: str | None = None
    prometheus_port: int = 9090
    queue_name: str = "pragmatiq:queue"
    redis_host: str = "localhost"
    redis_port: int = 6379
    result_ttl: int = 3600
    service_name: str = "pragmatiq"

    def __post_init__(self) -> None:
        """Validates attribute types after initialization.

        Raises:
            ValueError: If any attribute does not match type criteria.
        """
        for attr, type_ in self.__annotations__.items():
            if attr in ("jaeger_auth", "prometheus_auth"):
                tuple_or_none: Tuple[str, str] | None = getattr(self, attr)
                if tuple_or_none and (
                    len(tuple_or_none) != 2
                    or not all(
                        [isinstance(value, str) for value in getattr(self, attr)]
                    )
                ):
                    raise ValueError(
                        f"Attribute '{attr}' must be a tuple containing exactly two strings (username and password)"
                    )
            elif isinstance(getattr(self, attr), int) and getattr(self, attr) < 0:
                raise ValueError(f"Attribute '{attr}' must be a positive integer")
            else:
                if not isinstance(getattr(self, attr), type_):
                    raise ValueError(f"Attribute '{attr}' must be of type '{type_}'")

    def __iter__(self) -> Generator[tuple[str, Any], Any, None]:
        for attr in self.__annotations__.keys():
            yield attr, getattr(self, attr)
