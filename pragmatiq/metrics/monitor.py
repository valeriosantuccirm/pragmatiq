from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import aiohttp
from aiohttp import ClientSession


class Monitor:
    """A utility class for querying Prometheus metrics and Jaeger traces asynchronously.

    This class provides a simple interface for developers to monitor application performance
    and behavior using Prometheus for metrics and Jaeger for distributed tracing. It supports
    both instant and range queries for Prometheus metrics via PromQL, as well as trace retrieval,
    service discovery, and dependency analysis for Jaeger. All operations are asynchronous,
    leveraging `aiohttp` for non-blocking HTTP requests, making it suitable for integration
    into async Python applications.

    Attributes:
        prometheus_url (str): Base URL of the Prometheus server (e.g., "http://prometheus:9090").
        jaeger_url (str): Base URL of the Jaeger query service (e.g., "http://jaeger:16686").
        prometheus_auth (Optional[Tuple[str, str]]): Optional (username, password) for Prometheus basic auth.
        jaeger_auth (Optional[Tuple[str, str]]): Optional (username, password) for Jaeger basic auth.
        session (Optional[ClientSession]): The aiohttp session used for HTTP requests, managed by the context manager.

    Examples:
        Basic usage with default endpoints:
        ```python
        async def monitor():
                async with MonitoringClient() as client:
                    # Query Prometheus for current up status
                    up_status = await client.query_prometheus('up{job="my-service"}')
                    print("Service Up Status:", up_status)

                    # Get traces for a service with errors
                    traces = await client.get_jaeger_traces("my-service", tags={"error": "true"})
                    print("Error Traces:", traces)

        asyncio.run(monitor())
        ```
        Advanced example:
        ```python
        import time

        async def detailed_monitor():
            auth = ("admin", "secret")
            async with MonitoringClient(
                prometheus_url="http://prometheus:9090",
                jaeger_url="http://jaeger:16686",
                prometheus_auth=auth,
                jaeger_auth=auth,
            ) as client:
                # Instant Prometheus query
                requests_total = await client.query_prometheus("http_requests_total")
                print("Total Requests:", requests_total)

                # Range query over the last hour
                rate = await client.query_range_prometheus(
                    query="rate(http_requests_total[5m])",
                    start=int(time.time()) - 3600,
                    end=int(time.time()),
                    step="30s",
                )
                print("Request Rate Over Time:", rate)


                # Fetch Jaeger traces with specific tags
                traces = await client.get_jaeger_traces(
                    service="my-service",
                    limit=50,
                    lookback="2h",
                    tags={"http.status_code": "500"},
                )
                print("Recent Error Traces:", traces)

                # List all services in Jaeger
                services = await client.get_jaeger_services()
                print("Tracked Services:", services)

                # Get service dependencies
                deps = await client.get_jaeger_dependencies(lookback="24h")
                print("Service Dependencies:", deps)

        asyncio.run(detailed_monitor())
        ```

    Raises:
        RuntimeError: If methods are called outside an async context manager without an active session.
        aiohttp.ClientResponseError: If HTTP requests to Prometheus or Jaeger fail (e.g., 404, 503).
    """

    def __init__(
        self,
        prometheus_url: str = "http://localhost:9090",
        jaeger_url: str = "http://localhost:16686",
        prometheus_auth: Optional[Tuple[str, str]] = None,  # (username, password)
        jaeger_auth: Optional[Tuple[str, str]] = None,  # (username, password)
    ) -> None:
        """
        Initialize the monitoring client with Prometheus and Jaeger endpoints.

        Args:
            prometheus_url: Base URL of the Prometheus server (e.g., "http://prometheus:9090").
            jaeger_url: Base URL of the Jaeger query service (e.g., "http://jaeger:16686").
            prometheus_auth: Optional tuple of (username, password) for Prometheus basic auth.
            jaeger_auth: Optional tuple of (username, password) for Jaeger basic auth.
        """
        self.prometheus_url: str = prometheus_url.rstrip("/")
        self.jaeger_url: str = jaeger_url.rstrip("/")
        self.prometheus_auth: Optional[Tuple[str, str]] = prometheus_auth
        self.jaeger_auth: Optional[Tuple[str, str]] = jaeger_auth
        self.session: Optional[ClientSession] = None

    async def __aenter__(self) -> "Monitor":
        """Async context manager entry: create an HTTP session."""
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(
        self,
        exc_type: Any,
        exc_val: Any,
        exc_tb: Any,
    ) -> None:
        """Async context manager exit: close the HTTP session."""
        if self.session:
            await self.session.close()
            self.session = None

    async def _request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        auth: Optional[Tuple[str, str]] = None,
    ) -> Dict[str, Any]:
        """Internal method to make HTTP requests."""
        if not self.session:
            raise RuntimeError("Monitor must be used within an async context manager")
        async with self.session.request(
            method=method,
            url=url,
            params=params,
            json=json,
            auth=aiohttp.BasicAuth(*auth) if auth else None,
        ) as response:
            response.raise_for_status()
            return await response.json()

    # Prometheus Methods

    async def query_prometheus(
        self,
        query: str,
    ) -> Dict[str, Any]:
        """Query Prometheus for an instant metric value.

        Args:
            query: PromQL query string (e.g., "rate(http_requests_total[5m])").

        Returns:
            Dictionary containing the query result (Prometheus API response format).
        """
        url: str = urljoin(base=self.prometheus_url, url="/api/v1/query")
        params: Dict[str, str] = {"query": query}
        return await self._request(
            method="GET",
            url=url,
            params=params,
            auth=self.prometheus_auth,
        )

    async def query_range_prometheus(
        self,
        query: str,
        start: int,
        end: int,
        step: str = "15s",
    ) -> Dict[str, Any]:
        """Query Prometheus for a range of metric values over time.

        Args:
            query: PromQL query string (e.g., "rate(http_requests_total[5m])").
            start: Unix timestamp (seconds) for the start of the range.
            end: Unix timestamp (seconds) for the end of the range.
            step: Query resolution step width (e.g., "15s", "1m").

        Returns:
            Dictionary containing the range query result (Prometheus API response format).
        """
        url: str = urljoin(base=self.prometheus_url, url="/api/v1/query_range")
        params: Dict[str, str] = {
            "query": query,
            "start": str(start),
            "end": str(end),
            "step": step,
        }
        return await self._request(
            method="GET",
            url=url,
            params=params,
            auth=self.prometheus_auth,
        )

    # Jaeger Methods

    async def get_jaeger_traces(
        self,
        service: str,
        limit: int = 100,
        lookback: str = "1h",
        tags: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Fetch traces for a specific service from Jaeger.

        Args:
            service: Name of the service to query traces for.
            limit: Maximum number of traces to return (default: 100).
            lookback: Lookback period (e.g., "1h", "24h", "7d").
            tags: Optional dictionary of tags to filter traces (e.g., {"http.status_code": "500"}).

        Returns:
            Dictionary containing the traces data (Jaeger API response format).
        """
        url: str = urljoin(base=self.jaeger_url, url="/api/traces")
        params: Dict[str, Any] = {
            "service": service,
            "limit": str(limit),
            "lookback": lookback,
        }
        if tags:
            params["tags"] = str(tags).replace(
                "'", '"'
            )  # Jaeger expects JSON-like string
        return await self._request(
            method="GET",
            url=url,
            params=params,
            auth=self.jaeger_auth,
        )

    async def get_jaeger_services(self) -> List[str]:
        """Fetch the list of services tracked by Jaeger.

        Returns:
            List of service names.
        """
        url: str = urljoin(base=self.jaeger_url, url="/api/services")
        response: Dict[str, Any] = await self._request(
            method="GET",
            url=url,
            auth=self.jaeger_auth,
        )
        return response.get("data", [])

    async def get_jaeger_dependencies(
        self, lookback: str = "24h"
    ) -> List[Dict[str, Any]]:
        """Fetch service dependencies from Jaeger.

        Args:
            lookback: Lookback period for dependency data (e.g., "24h", "7d").

        Returns:
            List of dependency dictionaries (Jaeger API response format).
        """
        url: str = urljoin(base=self.jaeger_url, url="/api/dependencies")
        params: Dict[str, str] = {"lookback": lookback}
        response: Dict[str, Any] = await self._request(
            method="GET",
            url=url,
            params=params,
            auth=self.jaeger_auth,
        )
        return response.get("data", [])
