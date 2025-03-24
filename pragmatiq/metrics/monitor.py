import json
import traceback
from types import TracebackType
from typing import Any, Dict, Literal, Tuple, Type

import aiohttp
from aiohttp import BasicAuth, ClientSession
from loguru import logger

from pragmatiq.config import PragmatiQConfig


class _MonitorConfig:
    """Configuration class for monitoring endpoints (Prometheus and Jaeger).

    Attributes:
        service (str): The name of the service being monitored. Defaults to "pragmatiq".
        prometheus_host (str | None): The hostname or IP address of the Prometheus server. Defaults to None.
        prometheus_port (int | None): The port number of the Prometheus server. Defaults to None.
        jaeger_host (str | None): The hostname or IP address of the Jaeger query service. Defaults to None.
        jaeger_port (int | None): The port number of the Jaeger query service. Defaults to None.
        prometheus_auth Tuple[str, str] | None: Tuple of (username, password) for Prometheus basic authentication. Defaults to None.
        jaeger_auth Tuple[str, str] | None: Tuple of (username, password) for Jaeger basic authentication. Defaults to None.
    """

    def __init__(
        self,
        service: str = "pragmatiq",
        prometheus_host: str | None = None,
        prometheus_port: int | None = None,
        jaeger_host: str | None = None,
        jaeger_port: int | None = None,
        prometheus_auth: Tuple[str, str] | None = None,
        jaeger_auth: Tuple[str, str] | None = None,
    ) -> None:
        self.service: str = service
        self.prometheus_host: str | None = prometheus_host
        self.prometheus_port: int | None = prometheus_port
        self.jaeger_host: str | None = jaeger_host
        self.jaeger_port: int | None = jaeger_port
        self.prometheus_auth: BasicAuth | None = (
            BasicAuth(*prometheus_auth) if prometheus_auth else None
        )
        self.jaeger_auth: BasicAuth | None = (
            BasicAuth(*jaeger_auth) if jaeger_auth else None
        )

    @property
    def prometheus_base_url(self) -> str | None:
        """Constructs the base URL for the Prometheus server.

        Returns:
            The base URL string if both host and port are configured, otherwise None.
        """
        if not self.prometheus_host or not self.prometheus_port:
            return
        return f"http://{self.prometheus_host}:{self.prometheus_port}"

    @property
    def jaeger_base_url(self) -> str | None:
        """Constructs the base URL for the Jaeger query service.

        Returns:
            str | None: The base URL string if both host and port are configured, otherwise None.
        """
        if not self.jaeger_host or not self.jaeger_port:
            return
        return f"http://{self.jaeger_host}:{self.jaeger_port}"

    def mapped_params(
        self,
        client: Literal["prometheus", "jaeger"],
    ) -> Tuple[str | None, BasicAuth | None] | Tuple[None, None]:
        """Map the base URL and authentication details for the specified monitoring client.

        Args:
            client (Literal["prometheus", "jaeger"]): The monitoring client ("prometheus" or "jaeger").

        Returns:
            Tuple[str | None, BasicAuth | None] | Tuple[None, None]: A tuple containing the base URL
            (str or None) and BasicAuth object (or None) for the specified client.
            Returns (None, None) if the client is not recognized.
        """
        if client == "prometheus":
            return self.prometheus_base_url, self.prometheus_auth
        elif client == "jaeger":
            return self.jaeger_base_url, self.jaeger_auth
        return None, None


class Monitor:
    def __init__(
        self,
        config: PragmatiQConfig = PragmatiQConfig(),
    ) -> None:
        """Initialize the monitoring client based on the provided configuration.

        Args:
            config (PragmatiQConfig): Configuration object containing settings for
                the service name, Prometheus, and Jaeger endpoints and authentication details.
                Defaults to PragmatiQConfig().
        """
        self.monitor_config = _MonitorConfig(
            service=config.service_name,
            prometheus_host=config.prometheus_host,
            prometheus_port=config.prometheus_port,
            jaeger_host=config.jaeger_host,
            jaeger_port=config.jaeger_port,
            prometheus_auth=config.prometheus_auth,
            jaeger_auth=config.jaeger_auth,
        )
        self.session: ClientSession | None = None

    async def __aenter__(self) -> "Monitor":
        """Asynchronously enters the context manager.

        Returns:
            Monitor: The `Monitor` instance.
        """
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(
        self,
        exc_type: Type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> None:
        """Asynchronously exits the context manager.

        Args:
            exc_type (Type[BaseException] | None): The type of the exception that caused the context to exit, if any. Defaults to None.
            exc_val: (BaseException | None) The exception instance that caused the context to exit, if any. Defaults to None.
            exc_tb: (TracebackType | None) The traceback object associated with the exception, if any. Defaults to None.
        """
        try:
            if exc_type:
                logger.opt(exception=exc_val).error(
                    f"Context manager exited with exception: {exc_type.__name__}: {str(exc_val)}",
                    exc_info=(
                        exc_type,
                        exc_val,
                        exc_tb,
                    ),
                )
        finally:
            if self.session:
                await self.session.close()
                self.session = None

    async def __request(
        self,
        method: Literal["GET", "POST"],
        url: str,
        params: Dict[str, Any] = {},
        json_data: Any = None,
        auth: BasicAuth | None = None,
    ) -> Dict[str, Any]:
        """Internal asynchronous method to make HTTP requests to a given URL.

        Args:
            method (Literal["GET", "POST"]): The HTTP method to use ("GET" or "POST").
            url (str): The full URL to make the request to.
            params (Dict[str, Any]): A dictionary of query parameters to include in the URL. Defaults to {}.
            json_data (Any): An optional JSON-serializable object to include in the request body (for POST). Defaults to None.
            auth (BasicAuth | None): An optional Aiohttp BasicAuth object for authentication. Defaults to None.

        Returns:
            Dict[str, Any]: A dictionary representing the JSON response from the server.

        Raises:
            RuntimeError: If the `Monitor` is not used within an async context manager (i.e., `self.session` is None).
            aiohttp.ClientResponseError: If the HTTP request returns an error status code (>= 400).
            Exception: For other errors during the request process, with error details logged.
        """
        if not self.session:
            raise RuntimeError("Monitor must be used within an async context manager")
        try:
            async with self.session.request(
                method=method,
                url=url,
                params=params,
                json=json.dumps(obj=json_data),
                auth=BasicAuth(*auth) if auth else None,
            ) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            logger.error(f"Error: {e}. Traceback: {traceback.print_exc()}")
            raise e

    async def query(
        self,
        client: Literal["prometheus", "jaeger"],
        method: Literal["GET", "POST"],
        api_endpoint: str,
        params: Dict[str, Any] = {},
        json_data: Any = None,
    ) -> Dict[str, Any]:
        """Handle querying various monitoring backends (Prometheus, Jaeger) using a unified interface.

        Args:
            client (Literal["prometheus", "jaeger"]): The monitoring client to query, either "prometheus" or "jaeger".
            method (Literal["GET", "POST"]): The HTTP method to use, either "GET" or "POST".
            api_endpoint (str): The specific API endpoint path for the client (e.g., "/api/v1/query", "api/traces").
            params (Dict[str, Any]): A dictionary of query parameters to be included in the URL. Defaults to {}.
            json_data (Any): An optional JSON serializable object to be sent in the request body for POST requests. Defaults to None.

        Returns:
            Dict[str, Any]: ClientResponse object response.

        Raises:
            RuntimeError: If the Monitor is not used within an async context manager.
            ClientResponseError: If the HTTP request to the monitoring backend fails (status code >= 400).
            Any other exception raised during the request process.

        # Prometheus Examples:

        ## Querying an instant metric value using GET:
        >>> response = await self.query(
        ...     client="prometheus",
        ...     method="GET",
        ...     api_endpoint="/api/v1/query",
        ...     params={"query": "up"}
        ... )
        >>> print(response.status)
        200
        >>> data = await response.json()
        >>> print(data)
        {'status': 'success', 'data': {'resultType': 'vector', 'result': [...]}}

        >>> response_rate = await self.query(
        ...     client="prometheus",
        ...     method="GET",
        ...     api_endpoint="/api/v1/query",
        ...     params={"query": 'rate(http_requests_total{job="pragmatiq"}[5m])'}
        ... )
        >>> print(response_rate.status)
        200
        >>> rate_data = await response_rate.json()
        >>> print(rate_data)
        {'status': 'success', 'data': {'resultType': 'vector', 'result': [...]}}

        ## Querying a range of metric values using GET:
        >>> import time
        >>> end_time = int(time.time())
        >>> start_time = end_time - 3600  # Look back 1 hour
        >>> range_response = await self.query(
        ...     client="prometheus",
        ...     method="GET",
        ...     api_endpoint="/api/v1/query_range",
        ...     params={
        ...         "query": "cpu_usage_seconds_total",
        ...         "start": str(start_time),
        ...         "end": str(end_time),
        ...         "step": "30s"
        ...     }
        ... )
        >>> print(range_response.status)
        200
        >>> range_data = await range_response.json()
        >>> print(range_data)
        {'status': 'success', 'data': {'resultType': 'matrix', 'result': [...]}}

        ## Example of a potentially different Prometheus API call using POST (hypothetical - check Prometheus API docs):
        >>> # Assuming a hypothetical POST endpoint for querying with more complex options
        >>> post_query_response = await self.query(
        ...     client="prometheus",
        ...     method="POST",
        ...     api_endpoint="/api/v1/extended_query",
        ...     json_data={"promql": 'up{instance="my-instance"}', "timeout": "5s"}
        ... )
        >>> print(post_query_response.status)
        200
        >>> post_query_data = await post_query_response.json()
        >>> print(post_query_data)
        {'status': 'success', 'data': {...}}

        # Jaeger Examples:

        ## Fetching traces for a specific service using GET:
        >>> traces_response = await self.query(
        ...     client="jaeger",
        ...     method="GET",
        ...     api_endpoint="api/traces",
        ...     params={"service": "my-web-app"}
        ... )
        >>> print(traces_response.status)
        200
        >>> traces_data = await traces_response.json()
        >>> print(traces_data)
        {'data': [...], 'total': 123, 'limit': 100, 'offset': 0, 'errors': None}

        ## Fetching traces with limit and lookback:
        >>> limited_traces_response = await self.query(
        ...     client="jaeger",
        ...     method="GET",
        ...     api_endpoint="api/traces",
        ...     params={"service": "auth-service", "limit": "50", "lookback": "1h"}
        ... )
        >>> print(limited_traces_response.status)
        200
        >>> limited_traces_data = await limited_traces_response.json()
        >>> print(limited_traces_data)
        {'data': [...], 'total': 87, 'limit': 50, 'offset': 0, 'errors': None}

        ## Fetching traces with tags:
        >>> error_traces_response = await self.query(
        ...     client="jaeger",
        ...     method="GET",
        ...     api_endpoint="api/traces",
        ...     params={"service": "api-gateway", "tags": '{"http.status_code": "500"}'}
        ... )
        >>> print(error_traces_response.status)
        200
        >>> error_traces_data = await error_traces_response.json()
        >>> print(error_traces_data)
        {'data': [...], 'total': 5, 'limit': 100, 'offset': 0, 'errors': None}

        ## Fetching the list of services using GET:
        >>> services_response = await self.query(
        ...     client="jaeger",
        ...     method="GET",
        ...     api_endpoint="api/services"
        ... )
        >>> print(services_response.status)
        200
        >>> services_data = await services_response.json()
        >>> print(services_data)
        ['my-web-app', 'auth-service', 'order-processor', 'api-gateway', 'user-service']

        ## Fetching service dependencies using GET:
        >>> dependencies_response = await self.query(
        ...     client="jaeger",
        ...     method="GET",
        ...     api_endpoint="api/dependencies",
        ...     params={"lookback": "24h"}
        ... )
        >>> print(dependencies_response.status)
        200
        >>> dependencies_data = await dependencies_response.json()
        >>> print(dependencies_data)
        [{'parent': 'my-web-app', 'child': 'auth-service', 'callCount': 150}, ...]
        """
        base_url, auth = self.monitor_config.mapped_params(client=client)
        params["service"] = self.monitor_config.service
        api_endpoint = (
            api_endpoint[1:] if api_endpoint.startswith("/") else api_endpoint
        )
        return await self.__request(
            method=method,
            url=f"{base_url}/{api_endpoint}",
            params=params,
            json_data=json_data,
            auth=auth,
        )
