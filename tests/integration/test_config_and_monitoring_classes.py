from typing import Any, Dict, NoReturn
from unittest.mock import AsyncMock, patch

import aiohttp
import pytest
from aiohttp import BasicAuth, ClientSession
from pytest_mock import MockFixture

from pragmatiq.config import PragmatiQConfig
from pragmatiq.metrics.monitor import Monitor


@pytest.mark.asyncio
async def test_monitor_query_prometheus_integration(
    pragmatiq_config: PragmatiQConfig,
) -> None:
    """Test Monitor querying Prometheus successfully with a valid config."""
    monitor = Monitor(config=pragmatiq_config)
    async with monitor:
        response: Dict[str, Any] = await monitor.query(
            client="prometheus",
            method="GET",
            api_endpoint="/api/v1/query",
            params={"query": "up"},
        )
        assert "status" in response
        assert response["status"] == "success"
        assert "data" in response


@pytest.mark.asyncio
async def test_monitor_query_jaeger_integration(
    pragmatiq_config: PragmatiQConfig,
) -> None:
    """Test Monitor querying Jaeger services endpoint returns a dictionary."""
    monitor = Monitor(config=pragmatiq_config)
    async with monitor:
        response: Dict[str, Any] = await monitor.query(
            client="jaeger",
            method="GET",
            api_endpoint="/api/services",
        )
        assert isinstance(response, dict)


@pytest.mark.asyncio
async def test_monitor_query_prometheus_auth_integration(
    pragmatiq_config: PragmatiQConfig,
    mocker: MockFixture,
) -> None:
    """Test Monitor with Prometheus auth config raises exception and retains auth details."""

    def raise_exc() -> NoReturn:
        raise Exception

    config = PragmatiQConfig(
        **{
            **dict(pragmatiq_config),
            "prometheus_auth": ("testuser", "testpassword"),
        }
    )
    mock_session = AsyncMock(spec=ClientSession)
    mock_session.request.return_value.__aenter__.return_value.json = AsyncMock(side_effect=raise_exc)
    mocker.patch.object(
        target=aiohttp,
        attribute="ClientSession",
        return_value=mock_session,
    )
    monitor = Monitor(config=config)
    with pytest.raises(expected_exception=Exception):  # Expecting a connection error due to no auth server
        async with monitor:
            await monitor.query(
                client="prometheus",
                method="GET",
                api_endpoint="/api/v1/query",
                params={"query": "up"},
            )
    assert monitor.monitor_config.prometheus_auth is not None
    assert isinstance(monitor.monitor_config.prometheus_auth, BasicAuth)
    assert monitor.monitor_config.prometheus_auth.login == "testuser"
    assert monitor.monitor_config.prometheus_auth.password == "testpassword"


@pytest.mark.asyncio
async def test_auth_success_mocked(
    pragmatiq_config: PragmatiQConfig,
) -> None:
    """Test successful authentication with mocked response"""
    config = PragmatiQConfig(
        **{
            **dict(pragmatiq_config),
            "prometheus_auth": (
                "valid_user",
                "valid_pass",
            ),
        }
    )
    with patch("aiohttp.ClientSession.request") as mock_request:
        # Mock successful response
        mock_request.return_value.__aenter__.return_value.status = 200
        mock_request.return_value.__aenter__.return_value.json.return_value = {"status": "success"}

        async with Monitor(config=config) as monitor:
            response: Dict[str, Any] = await monitor.query(
                client="prometheus",
                method="GET",
                api_endpoint="/api/v1/query",
                params={"query": "up,"},
            )
        # Verify auth header was sent
        _, kwargs = mock_request.call_args
        assert "auth" in kwargs
        assert kwargs["auth"].login == "valid_user"
        assert kwargs["auth"].password == "valid_pass"
        assert response["status"] == "success"
