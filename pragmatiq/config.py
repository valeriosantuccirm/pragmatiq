from dataclasses import dataclass


@dataclass
class PragmatiqConfig:
    """Configuration settings for the Pragmatiq task and event management system.

    Holds configuration parameters for Redis, Prometheus, process pool, and other system settings.
    Uses dataclass to simplify initialization and provide default values.

    Attributes:
        redis_host (str): Hostname or IP address of the Redis server, defaults to "localhost".
        redis_port (int): Port number of the Redis server, defaults to 6379.
        queue_name (str): Unique name for the task queue, defaults to "pragmatiq_queue".
        prometheus_port (int): Port number for the Prometheus metrics server, defaults to 9090.
        process_pool_size (int): Number of workers in the process pool, defaults to 4.
        default_timeout (int): Default timeout for tasks in seconds, defaults to 30.
        cron_check_interval (int): Interval in seconds for periodic queue length checks, defaults to 5.
        service_name (str): Name of the service for Jaeger tracing, defaults to "pragmatiq".
    """

    redis_host: str = "localhost"
    redis_port: int = 6379
    queue_name: str = "pragmatiq:queue"
    dlq_name: str = "pragmatiq:queue:dlq"
    prometheus_port: int = 9090
    process_pool_size: int = 4
    default_timeout: int = 30
    cron_check_interval: int = 5
    service_name: str = "pragmatiq"
    result_ttl: int = 3600

    def __post_init__(self) -> None:
        for attr, type_ in self.__annotations__.items():
            if not isinstance(getattr(self, attr), type_):
                raise ValueError(f"Attribute '{attr}' must be of type '{type_}'")
