import os
from typing import Any, Dict, Generator, Tuple, Type
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
import pytest_asyncio
from testcontainers.generic import ServerContainer

from pragmatiq.broker.redis_broker import RedisBroker
from pragmatiq.config import PragmatiQConfig
from pragmatiq.metrics.monitor import Monitor, _MonitorConfig


@pytest_asyncio.fixture(scope="function")
async def mock_broker() -> Mock:
    """Provide a mocked RedisBroker instance for testing."""
    return Mock(spec=RedisBroker())


@pytest.fixture(autouse=True)
def mock_global_tracer() -> Generator[MagicMock | AsyncMock, Any, None]:
    """Mock the global_tracer for Jaeger tracing in all tests."""
    with patch("pragmatiq.core.task.global_tracer") as mock_tracer:
        mock_span: MagicMock = MagicMock()
        mock_span.start_active_span.return_value.__enter__.return_value = mock_span
        mock_tracer.return_value = mock_span
        yield mock_tracer


@pytest.fixture
def default_config() -> Generator[PragmatiQConfig, Any, None]:
    """Provide a default PragmatiQConfig instance for testing."""
    yield PragmatiQConfig()


@pytest.fixture
def monitor_config() -> Generator[_MonitorConfig, Any, None]:
    """Provide a preconfigured _MonitorConfig instance for testing."""
    yield _MonitorConfig(
        service="test_service",
        prometheus_host="localhost",
        prometheus_port=9090,
        jaeger_host="localhost",
        jaeger_port=16686,
        prometheus_auth=("user", "pass"),
        jaeger_auth=("admin", "secret"),
    )


@pytest.fixture
def pragmatiq_config(
    default_config: PragmatiQConfig,
) -> Generator[PragmatiQConfig, Any, None]:
    """Provide a customized PragmatiQConfig instance with test-specific settings."""
    generic_host: str = "localhost"
    is_gh_actions: bool = os.getenv(key="GITHUB_ACTIONS") == "true"
    # Detect if running in GitHub Actions
    generic_host_map: Dict[str, str] = {
        "redis_host": "redis" if is_gh_actions else generic_host,
        "prometheus_host": "prometheus" if is_gh_actions else generic_host,
        "jaeger_host": "jaeger" if is_gh_actions else generic_host,
    }
    generic_auth: Tuple[str, str] = ("test_user", "test_pswd")
    params: Dict[str, Any] = {
        **generic_host_map,
        "redis_port": 6379,
        "prometheus_port": 9090,
        "jaeger_port": 16686,
        "service_name": "pragmaticq-test",
        "prometheus_auth": generic_auth,
        "jaeger_auth": generic_auth,
    }
    yield PragmatiQConfig(**{**dict(default_config), **params})


@pytest.fixture
def monitor(
    pragmatiq_config: PragmatiQConfig,
) -> Monitor:
    """Provide a Monitor instance initialized with a test config."""
    return Monitor(config=pragmatiq_config)


@pytest.fixture
def prometheus_container_class() -> Generator[Type[ServerContainer], Any, None]:
    """Yield a PrometheusContainer class for creating Prometheus container instances."""

    class PrometheusContainer(ServerContainer):
        def __init__(self, image="prom/prometheus:v2.50.0", **kwargs) -> None:
            super().__init__(
                port=9090,
                image=image,
                **kwargs,
            )

        @property
        def address(self) -> str:
            return f"http://{self.get_container_host_ip()}:{(self.get_exposed_port(port=9090),)}"

    yield PrometheusContainer


@pytest.fixture
def jaeger_container_class() -> Generator[Type[ServerContainer], Any, None]:
    """Yield a JaegerContainer class for creating Jaeger container instances."""

    class JaegerContainer(ServerContainer):
        def __init__(
            self,
            image="jaegertracing/all-in-one:1.49",
            **kwargs,
        ) -> None:
            super().__init__(
                port=16686,
                image=image,
                **kwargs,
            )

        @property
        def query_address(self) -> str:
            return f"http://{self.get_container_host_ip()}:{self.get_exposed_port(port=16686)}"

    yield JaegerContainer
