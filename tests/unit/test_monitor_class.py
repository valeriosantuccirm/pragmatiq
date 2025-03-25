from typing import Any, Dict
from unittest.mock import AsyncMock

import aiohttp
import pytest
from aiohttp import BasicAuth, ClientSession
from pytest_mock import MockerFixture

from pragmatiq.metrics.monitor import Monitor, PragmatiQConfig, _MonitorConfig


@pytest.mark.asyncio
async def test_monitor_config_prometheus_base_url() -> None:
    """Test that prometheus_base_url constructs correctly or returns None when incomplete."""
    config = _MonitorConfig(
        prometheus_host="localhost",
        prometheus_port=9090,
    )
    assert config.prometheus_base_url == "http://localhost:9090"

    config = _MonitorConfig(prometheus_host="localhost")
    assert config.prometheus_base_url is None

    config = _MonitorConfig(prometheus_port=9090)
    assert config.prometheus_base_url is None


@pytest.mark.asyncio
async def test_monitor_config_jaeger_base_url() -> None:
    """Test that jaeger_base_url constructs correctly or returns None when incomplete."""
    config = _MonitorConfig(
        jaeger_host="localhost",
        jaeger_port=16686,
    )
    assert config.jaeger_base_url == "http://localhost:16686"

    config = _MonitorConfig(jaeger_host="localhost")
    assert config.jaeger_base_url is None

    config = _MonitorConfig(jaeger_port=16686)
    assert config.jaeger_base_url is None


@pytest.mark.asyncio
async def test_monitor_config_mapped_params() -> None:
    """Test that mapped_params returns correct URL and auth for known clients, None for unknown."""
    config = _MonitorConfig(
        prometheus_host="prom",
        prometheus_port=9090,
        jaeger_host="jaeger",
        jaeger_port=16686,
        prometheus_auth=("user", "pass"),
        jaeger_auth=("j_user", "j_pass"),
    )

    prom_url, prom_auth = config.mapped_params(client="prometheus")
    assert prom_url == "http://prom:9090"
    assert isinstance(prom_auth, BasicAuth)
    assert prom_auth.login == "user"
    assert prom_auth.password == "pass"

    jaeger_url, jaeger_auth = config.mapped_params(client="jaeger")
    assert jaeger_url == "http://jaeger:16686"
    assert isinstance(jaeger_auth, BasicAuth)
    assert jaeger_auth.login == "j_user"
    assert jaeger_auth.password == "j_pass"

    unknown_url, unknown_auth = config.mapped_params(client="unknown")  # type: ignore[awaitable]
    assert unknown_url is None
    assert unknown_auth is None


def test_monitor_config_prometheus_base_url_success(
    monitor_config: _MonitorConfig,
) -> None:
    """Test passing case: Both host and port are set."""
    # Test passing case: Both host and port are set
    assert monitor_config.prometheus_base_url == "http://localhost:9090"


def test_monitor_config_prometheus_base_url_fail() -> None:
    """Test failing case: Missing port."""
    config = _MonitorConfig(prometheus_host="localhost")
    assert config.prometheus_base_url is None  # Should fail if expecting a URL


def test_monitor_config_jaeger_base_url_success(
    monitor_config: _MonitorConfig,
) -> None:
    """Test passing case: Both host and port are set."""
    assert monitor_config.jaeger_base_url == "http://localhost:16686"


def test_monitor_config_jaeger_base_url_fail() -> None:
    """Test failing case: Missing host."""
    config = _MonitorConfig(jaeger_port=16686)
    assert config.jaeger_base_url is None  # Should fail if expecting a URL


def test_monitor_config_mapped_params_prometheus_success(
    monitor_config: _MonitorConfig,
) -> None:
    """Test passing case: Prometheus params correctly mapped."""
    base_url, auth = monitor_config.mapped_params(client="prometheus")
    assert base_url == "http://localhost:9090"
    assert isinstance(auth, BasicAuth)
    assert auth.login == "user"
    assert auth.password == "pass"


def test_monitor_config_mapped_params_invalid_client_fail(
    monitor_config: _MonitorConfig,
) -> None:
    """Test failing case: Invalid client name."""
    base_url, auth = monitor_config.mapped_params(client="invalid")  # type: ignore[awaitable]
    assert base_url is None
    assert auth is None  # Should fail if expecting valid params


@pytest.mark.asyncio
async def test_monitor_context_manager_success(
    default_config: PragmatiQConfig,
) -> None:
    """Test passing case: Properly using context manager."""
    async with Monitor(config=default_config) as monitor:
        assert isinstance(monitor.session, ClientSession)
        assert not monitor.session.closed
    assert monitor.session is None  # Session should be closed after exiting


@pytest.mark.asyncio
async def test_monitor_request_outside_context_fail(
    default_config: PragmatiQConfig,
) -> None:
    """Test failing case: Using request outside context manager."""
    monitor = Monitor(config=default_config)
    with pytest.raises(
        expected_exception=RuntimeError,
        match="Monitor must be used within an async context manager",
    ):
        await monitor._Monitor__request(  # type: ignore[awaitable]
            "GET",
            "http://example.com",
        )


@pytest.mark.asyncio
async def test_monitor_query_prometheus_success(
    default_config: PragmatiQConfig,
    mocker: MockerFixture,
) -> None:
    """Test passing case: Mock successful Prometheus query."""
    monitor = Monitor(config=default_config)
    mock_response: Dict[str, Any] = {
        "status": "success",
        "data": {
            "result": [],
        },
    }
    mock_session = AsyncMock(spec=ClientSession)
    mock_session.request.return_value.__aenter__.return_value.json = AsyncMock(
        return_value=mock_response
    )
    mocker.patch.object(
        target=aiohttp,
        attribute="ClientSession",
        return_value=mock_session,
    )

    async with monitor:
        result: Dict[str, Any] = await monitor.query(
            client="prometheus",
            method="GET",
            api_endpoint="/api/v1/query",
            params={
                "query": "up",
            },
        )
    assert result == mock_response


@pytest.mark.asyncio
async def test_monitor_query_invalid_endpoint_fail(
    default_config: PragmatiQConfig,
) -> None:
    """Test failing case: Invalid endpoint (assuming server not running locally for this test)."""
    monitor = Monitor(config=default_config)
    monitor.session = AsyncMock(spec=ClientSession())
    async with monitor:
        with pytest.raises(expected_exception=aiohttp.InvalidUrlClientError):
            await monitor.query(
                client="prometheus",
                method="GET",
                api_endpoint="invalid/endpoint",
                params={
                    "query": "up",
                },
            )


def test_monitor_config_init_no_auth_success() -> None:
    """Test passing case: No auth provided, should initialize with None."""
    config = _MonitorConfig(service="no_auth_service")
    assert config.prometheus_auth is None
    assert config.jaeger_auth is None


def test_monitor_config_mapped_params_jaeger_success(
    monitor_config: _MonitorConfig,
) -> None:
    """Test passing case: Jaeger params correctly mapped."""
    base_url, auth = monitor_config.mapped_params(client="jaeger")
    assert base_url == "http://localhost:16686"
    assert isinstance(auth, BasicAuth)
    assert auth.login == "admin"
    assert auth.password == "secret"


@pytest.mark.asyncio
async def test_monitor_request_post_json_success(
    default_config: PragmatiQConfig,
    mocker: MockerFixture,
) -> None:
    """Test passing case: Successful POST request with JSON data."""
    monitor = Monitor(config=default_config)
    mock_response: Dict[str, Any] = {
        "status": "success",
        "data": {
            "result": [],
        },
    }
    mock_session = AsyncMock(spec=ClientSession)
    mock_session.request.return_value.__aenter__.return_value.json = AsyncMock(
        return_value=mock_response
    )
    mocker.patch.object(
        target=aiohttp,
        attribute="ClientSession",
        return_value=mock_session,
    )

    async with monitor:
        result: Dict[str, Any] = await monitor.query(
            client="prometheus",
            method="POST",
            api_endpoint="api/v1/extended_query",
            json_data={"promql": "up", "timeout": "5s"},
        )
    assert result == mock_response


@pytest.mark.asyncio
async def test_monitor_query_missing_base_url_fail() -> None:
    """Test failing case: Query with unconfigured client (no host/port)."""
    config = PragmatiQConfig(
        service_name="test_service"
    )  # No prometheus/jaeger host/port
    monitor = Monitor(config=config)
    async with monitor:
        with pytest.raises(
            expected_exception=Exception,
        ):  # Due to None + str concatenation
            await monitor.query(
                client="prometheus",
                method="GET",
                api_endpoint="/api/v1/query",
                params={
                    "query": "up",
                },
            )


@pytest.mark.asyncio
async def test_monitor_query_invalid_method_fail(
    default_config: PragmatiQConfig,
) -> None:
    """Test failing case: Invalid HTTP method."""
    monitor = Monitor(config=default_config)
    async with monitor:
        with pytest.raises(
            expected_exception=ValueError,
        ):  # Assuming aiohttp validates this
            await monitor.query(
                client="prometheus",
                method="PUT",  # type: ignore[awaitable]
                api_endpoint="/api/v1/query",
                params={"query": "up"},
            )
