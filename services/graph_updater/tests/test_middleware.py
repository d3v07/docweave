"""
Tests for Middleware
====================

Unit tests for FastAPI middleware covering:
- Request logging middleware
- Correlation ID middleware
- Rate limiting middleware
- Error handling middleware
"""

import pytest
import time
import uuid
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

# Import from conftest's path setup and mocking
from middleware import (
    RequestLoggingMiddleware,
    CorrelationIdMiddleware,
    RateLimitMiddleware,
    ErrorHandlingMiddleware,
    get_correlation_id,
    correlation_id_var,
    setup_middleware,
)


class MockRequest:
    """Mock FastAPI Request."""

    def __init__(
        self,
        method="GET",
        path="/test",
        client_host="127.0.0.1",
        headers=None,
        query_params=None,
    ):
        self.method = method
        self.url = MagicMock()
        self.url.path = path
        self.client = MagicMock()
        self.client.host = client_host
        self.headers = headers or {}
        self.query_params = query_params or {}
        self.state = MagicMock()

    def __getitem__(self, key):
        return self.headers.get(key)

    def get(self, key, default=None):
        return self.headers.get(key, default)


class MockResponse:
    """Mock FastAPI Response."""

    def __init__(self, status_code=200, headers=None):
        self.status_code = status_code
        self.headers = headers if headers is not None else {}


class MockScope:
    """Mock ASGI scope."""

    def __init__(self, type_="http", path="/test", method="GET", client=("127.0.0.1", 8000)):
        self.scope = {
            "type": type_,
            "path": path,
            "method": method,
            "client": client,
            "headers": [],
        }

    def __getitem__(self, key):
        return self.scope.get(key)

    def __setitem__(self, key, value):
        self.scope[key] = value

    def get(self, key, default=None):
        return self.scope.get(key, default)


class TestCorrelationIdMiddleware:
    """Tests for CorrelationIdMiddleware."""

    def test_middleware_creation(self):
        """Test that CorrelationIdMiddleware can be created."""
        middleware = CorrelationIdMiddleware(app=MagicMock())
        assert middleware is not None
        assert middleware.HEADER_NAME == "X-Correlation-ID"

    def test_header_name_constant(self):
        """Test the header name constant."""
        middleware = CorrelationIdMiddleware(app=MagicMock())
        assert middleware.HEADER_NAME == "X-Correlation-ID"

    def test_get_correlation_id_function(self):
        """Test the get_correlation_id function."""
        # When no correlation ID is set, should return empty string
        correlation_id = get_correlation_id()
        assert isinstance(correlation_id, str)

    @pytest.mark.asyncio
    async def test_dispatch_generates_correlation_id_when_missing(self):
        """Test that dispatch generates a new correlation ID when none provided."""
        middleware = CorrelationIdMiddleware(app=MagicMock())
        request = MockRequest(headers={})
        response = MockResponse()

        async def call_next(req):
            # Verify correlation ID is set during request processing
            corr_id = get_correlation_id()
            assert corr_id != ""
            assert len(corr_id) == 36  # UUID format
            return response

        result = await middleware.dispatch(request, call_next)

        assert "X-Correlation-ID" in result.headers
        # UUID is 36 chars including hyphens
        assert len(result.headers["X-Correlation-ID"]) == 36

    @pytest.mark.asyncio
    async def test_dispatch_uses_provided_correlation_id(self):
        """Test that dispatch uses correlation ID from header when provided."""
        middleware = CorrelationIdMiddleware(app=MagicMock())
        provided_id = "test-correlation-id-12345"
        request = MockRequest(headers={"X-Correlation-ID": provided_id})
        response = MockResponse()

        captured_id = None

        async def call_next(req):
            nonlocal captured_id
            captured_id = get_correlation_id()
            return response

        result = await middleware.dispatch(request, call_next)

        assert captured_id == provided_id
        assert result.headers["X-Correlation-ID"] == provided_id

    @pytest.mark.asyncio
    async def test_dispatch_resets_context_after_request(self):
        """Test that correlation ID context is reset after request."""
        middleware = CorrelationIdMiddleware(app=MagicMock())
        request = MockRequest(headers={"X-Correlation-ID": "request-specific-id"})
        response = MockResponse()

        async def call_next(req):
            return response

        await middleware.dispatch(request, call_next)

        # After dispatch, correlation ID should be reset to default
        # This tests the context variable cleanup
        assert get_correlation_id() == ""

    @pytest.mark.asyncio
    async def test_dispatch_handles_exception_and_resets_context(self):
        """Test that context is reset even when exception occurs."""
        middleware = CorrelationIdMiddleware(app=MagicMock())
        request = MockRequest(headers={"X-Correlation-ID": "error-request-id"})

        async def call_next(req):
            raise ValueError("Test error")

        with pytest.raises(ValueError):
            await middleware.dispatch(request, call_next)

        # Context should still be reset
        assert get_correlation_id() == ""


class TestRequestLoggingMiddleware:
    """Tests for RequestLoggingMiddleware."""

    def test_middleware_creation(self):
        """Test that RequestLoggingMiddleware can be created."""
        middleware = RequestLoggingMiddleware(app=MagicMock())
        assert middleware is not None

    def test_middleware_is_base_http_middleware(self):
        """Test that middleware inherits from BaseHTTPMiddleware."""
        from starlette.middleware.base import BaseHTTPMiddleware
        middleware = RequestLoggingMiddleware(app=MagicMock())
        assert isinstance(middleware, BaseHTTPMiddleware)

    @pytest.mark.asyncio
    async def test_dispatch_logs_and_adds_timing_header(self):
        """Test that dispatch logs request and adds timing header."""
        middleware = RequestLoggingMiddleware(app=MagicMock())
        request = MockRequest(method="GET", path="/api/test")
        response = MockResponse(status_code=200)

        async def call_next(req):
            await asyncio.sleep(0.01)  # 10ms delay
            return response

        import asyncio

        with patch("middleware.logger") as mock_logger:
            result = await middleware.dispatch(request, call_next)

        assert "X-Response-Time-Ms" in result.headers
        response_time = float(result.headers["X-Response-Time-Ms"])
        assert response_time >= 10  # At least 10ms

        # Check logging was called
        assert mock_logger.info.call_count >= 2  # Start and completion logs

    @pytest.mark.asyncio
    async def test_dispatch_logs_error_on_exception(self):
        """Test that dispatch logs error when exception occurs."""
        middleware = RequestLoggingMiddleware(app=MagicMock())
        request = MockRequest(method="POST", path="/api/error")

        async def call_next(req):
            raise RuntimeError("Test runtime error")

        with patch("middleware.logger") as mock_logger:
            with pytest.raises(RuntimeError):
                await middleware.dispatch(request, call_next)

            mock_logger.error.assert_called_once()
            call_args = mock_logger.error.call_args
            assert "failed" in call_args[0][0].lower()

    @pytest.mark.asyncio
    async def test_dispatch_includes_correlation_id_in_logs(self):
        """Test that correlation ID is included in log extra data."""
        middleware = RequestLoggingMiddleware(app=MagicMock())
        request = MockRequest()
        response = MockResponse()

        # Set correlation ID in context
        token = correlation_id_var.set("test-corr-123")

        try:
            async def call_next(req):
                return response

            with patch("middleware.logger") as mock_logger:
                await middleware.dispatch(request, call_next)

            # Check that extra contains correlation_id
            for call in mock_logger.info.call_args_list:
                extra = call.kwargs.get("extra", {})
                assert extra.get("correlation_id") == "test-corr-123"
        finally:
            correlation_id_var.reset(token)


class TestRateLimitMiddleware:
    """Tests for RateLimitMiddleware."""

    @pytest.fixture
    def rate_limit_middleware(self):
        """Create RateLimitMiddleware for testing."""
        return RateLimitMiddleware(
            app=MagicMock(),
            requests_per_minute=60,
            burst_limit=10,
        )

    def test_allows_request_under_limit(self, rate_limit_middleware):
        """Test that requests under the limit are allowed."""
        client_ip = "192.168.1.1"
        current_time = time.time()

        is_allowed = rate_limit_middleware._is_allowed(client_ip, current_time)

        assert is_allowed is True

    def test_blocks_request_over_limit(self, rate_limit_middleware):
        """Test that requests over the limit are blocked."""
        client_ip = "192.168.1.2"
        current_time = time.time()

        # Exceed the rate limit
        for _ in range(65):  # More than 60 per minute
            rate_limit_middleware._record_request(client_ip, current_time)

        is_allowed = rate_limit_middleware._is_allowed(client_ip, current_time)

        # Should be blocked
        assert is_allowed is False

    def test_different_clients_have_separate_limits(self, rate_limit_middleware):
        """Test that different clients have separate rate limits."""
        client1 = "192.168.1.10"
        client2 = "192.168.1.11"
        current_time = time.time()

        # Make requests from client1
        for _ in range(5):
            rate_limit_middleware._record_request(client1, current_time)

        # Client2 should still be allowed
        is_allowed = rate_limit_middleware._is_allowed(client2, current_time)
        assert is_allowed is True

    def test_get_retry_after_header(self, rate_limit_middleware):
        """Test getting retry-after header value."""
        client_ip = "192.168.1.4"
        current_time = time.time()

        # Record some requests
        for _ in range(5):
            rate_limit_middleware._record_request(client_ip, current_time)

        retry_after = rate_limit_middleware._get_retry_after(client_ip, current_time)

        assert retry_after >= 0

    def test_get_remaining_requests(self, rate_limit_middleware):
        """Test getting remaining request count."""
        client_ip = "192.168.1.5"
        current_time = time.time()

        # Make 10 requests
        for _ in range(10):
            rate_limit_middleware._record_request(client_ip, current_time)

        remaining = rate_limit_middleware._get_remaining(client_ip, current_time)

        assert remaining == 50  # 60 - 10

    def test_get_remaining_returns_full_limit_for_new_client(self, rate_limit_middleware):
        """Test that new clients have full limit available."""
        remaining = rate_limit_middleware._get_remaining("new-client-ip", time.time())
        assert remaining == 60

    def test_get_retry_after_returns_zero_for_new_client(self, rate_limit_middleware):
        """Test retry after is 0 for new clients."""
        retry = rate_limit_middleware._get_retry_after("new-client", time.time())
        assert retry == 0

    def test_old_requests_expire(self, rate_limit_middleware):
        """Test that old requests outside window are cleaned up."""
        client_ip = "192.168.1.20"
        old_time = time.time() - 120  # 2 minutes ago
        current_time = time.time()

        # Record old requests
        for _ in range(70):
            rate_limit_middleware._record_request(client_ip, old_time)

        # Should be allowed because old requests expired
        is_allowed = rate_limit_middleware._is_allowed(client_ip, current_time)
        assert is_allowed is True

    def test_get_client_ip_direct(self, rate_limit_middleware):
        """Test getting client IP directly from request."""
        request = MockRequest(client_host="10.0.0.1")
        ip = rate_limit_middleware._get_client_ip(request)
        assert ip == "10.0.0.1"

    def test_get_client_ip_from_forwarded_header(self, rate_limit_middleware):
        """Test getting client IP from X-Forwarded-For header."""
        request = MockRequest(
            client_host="127.0.0.1",
            headers={"X-Forwarded-For": "203.0.113.1, 70.41.3.18, 150.172.238.178"}
        )
        ip = rate_limit_middleware._get_client_ip(request)
        assert ip == "203.0.113.1"

    def test_get_client_ip_no_client(self, rate_limit_middleware):
        """Test getting client IP when client is None."""
        request = MockRequest()
        request.client = None
        ip = rate_limit_middleware._get_client_ip(request)
        assert ip == "unknown"

    @pytest.mark.asyncio
    async def test_dispatch_skips_health_endpoints(self, rate_limit_middleware):
        """Test that health endpoints bypass rate limiting."""
        for path in ["/health", "/ready", "/metrics"]:
            request = MockRequest(path=path, client_host="192.168.1.100")
            response = MockResponse()

            async def call_next(req):
                return response

            # Even after many requests, health endpoints should work
            for _ in range(100):
                rate_limit_middleware._record_request("192.168.1.100", time.time())

            result = await rate_limit_middleware.dispatch(request, call_next)
            assert result.status_code == 200

    @pytest.mark.asyncio
    async def test_dispatch_returns_429_when_rate_limited(self, rate_limit_middleware):
        """Test that dispatch returns 429 when rate limited."""
        client_ip = "192.168.1.50"
        current_time = time.time()

        # Exceed rate limit
        for _ in range(65):
            rate_limit_middleware._record_request(client_ip, current_time)

        request = MockRequest(path="/api/data", client_host=client_ip)

        async def call_next(req):
            return MockResponse()

        result = await rate_limit_middleware.dispatch(request, call_next)

        assert result.status_code == 429
        # JSONResponse content is a dict
        assert "Retry-After" in result.headers

    @pytest.mark.asyncio
    async def test_dispatch_adds_rate_limit_headers(self, rate_limit_middleware):
        """Test that dispatch adds rate limit headers to response."""
        request = MockRequest(path="/api/data", client_host="192.168.1.60")
        response = MockResponse()

        async def call_next(req):
            return response

        result = await rate_limit_middleware.dispatch(request, call_next)

        assert "X-RateLimit-Limit" in result.headers
        assert "X-RateLimit-Remaining" in result.headers
        assert "X-RateLimit-Reset" in result.headers
        assert result.headers["X-RateLimit-Limit"] == "60"


class TestErrorHandlingMiddleware:
    """Tests for ErrorHandlingMiddleware."""

    def test_middleware_creation(self):
        """Test that ErrorHandlingMiddleware can be created."""
        middleware = ErrorHandlingMiddleware(app=MagicMock())
        assert middleware is not None

    def test_middleware_is_base_http_middleware(self):
        """Test that middleware inherits from BaseHTTPMiddleware."""
        from starlette.middleware.base import BaseHTTPMiddleware
        middleware = ErrorHandlingMiddleware(app=MagicMock())
        assert isinstance(middleware, BaseHTTPMiddleware)

    @pytest.mark.asyncio
    async def test_dispatch_passes_through_on_success(self):
        """Test that successful requests pass through unchanged."""
        middleware = ErrorHandlingMiddleware(app=MagicMock())
        request = MockRequest()
        response = MockResponse(status_code=200)

        async def call_next(req):
            return response

        result = await middleware.dispatch(request, call_next)

        assert result.status_code == 200

    @pytest.mark.asyncio
    async def test_dispatch_handles_exception_returns_500(self):
        """Test that exceptions are caught and return 500."""
        middleware = ErrorHandlingMiddleware(app=MagicMock())
        request = MockRequest()

        async def call_next(req):
            raise ValueError("Something went wrong")

        with patch("middleware.logger"):
            result = await middleware.dispatch(request, call_next)

        assert result.status_code == 500

    @pytest.mark.asyncio
    async def test_dispatch_error_response_has_correlation_id(self):
        """Test that error response includes correlation ID."""
        middleware = ErrorHandlingMiddleware(app=MagicMock())
        request = MockRequest()

        # Set correlation ID
        token = correlation_id_var.set("error-corr-id-456")

        try:
            async def call_next(req):
                raise RuntimeError("Test error")

            with patch("middleware.logger"):
                result = await middleware.dispatch(request, call_next)

            assert result.headers.get("X-Correlation-ID") == "error-corr-id-456"
        finally:
            correlation_id_var.reset(token)

    @pytest.mark.asyncio
    async def test_dispatch_logs_exception(self):
        """Test that exceptions are logged."""
        middleware = ErrorHandlingMiddleware(app=MagicMock())
        request = MockRequest()

        async def call_next(req):
            raise Exception("Critical error")

        with patch("middleware.logger") as mock_logger:
            await middleware.dispatch(request, call_next)

            mock_logger.exception.assert_called_once()


class TestRateLimitMiddlewareConfig:
    """Tests for RateLimitMiddleware configuration."""

    def test_custom_limits(self):
        """Test middleware with custom rate limits."""
        middleware = RateLimitMiddleware(
            app=MagicMock(),
            requests_per_minute=100,
            burst_limit=20,
        )

        assert middleware.requests_per_minute == 100
        assert middleware.burst_limit == 20

    def test_default_window(self):
        """Test default window is 60 seconds."""
        middleware = RateLimitMiddleware(
            app=MagicMock(),
            requests_per_minute=100,
        )

        assert middleware._window_seconds == 60

    def test_default_burst_limit(self):
        """Test default burst limit is 20."""
        middleware = RateLimitMiddleware(
            app=MagicMock(),
            requests_per_minute=100,
        )
        assert middleware.burst_limit == 20

    def test_internal_clients_dict_initialized(self):
        """Test that clients dict is initialized."""
        middleware = RateLimitMiddleware(
            app=MagicMock(),
            requests_per_minute=100,
        )
        assert middleware._clients == {}


class TestMiddlewareIntegration:
    """Integration tests for middleware stack."""

    def test_middleware_stack(self):
        """Test that middleware can be stacked together."""
        app = MagicMock()

        correlation_middleware = CorrelationIdMiddleware(app=app)
        logging_middleware = RequestLoggingMiddleware(app=correlation_middleware)
        rate_limit_middleware = RateLimitMiddleware(
            app=logging_middleware,
            requests_per_minute=100,
        )
        error_middleware = ErrorHandlingMiddleware(app=rate_limit_middleware)

        assert error_middleware is not None
        assert rate_limit_middleware is not None
        assert logging_middleware is not None
        assert correlation_middleware is not None


class TestTimingMiddleware:
    """Tests for request timing functionality."""

    def test_calculates_duration(self):
        """Test that request duration is calculated correctly."""
        middleware = RequestLoggingMiddleware(app=MagicMock())

        start_time = time.time()
        time.sleep(0.01)  # 10ms
        end_time = time.time()

        duration_ms = (end_time - start_time) * 1000

        assert duration_ms >= 10
        assert duration_ms < 100  # Should be quick


class TestSetupMiddleware:
    """Tests for setup_middleware function."""

    def test_setup_middleware_with_rate_limiting(self):
        """Test setup_middleware with rate limiting enabled."""
        app = MagicMock()
        app.add_middleware = MagicMock()

        with patch("middleware.logger"):
            setup_middleware(app, enable_rate_limiting=True)

        # Should add 4 middlewares
        assert app.add_middleware.call_count == 4

    def test_setup_middleware_without_rate_limiting(self):
        """Test setup_middleware with rate limiting disabled."""
        app = MagicMock()
        app.add_middleware = MagicMock()

        with patch("middleware.logger"):
            setup_middleware(app, enable_rate_limiting=False)

        # Should add 3 middlewares (no rate limiting)
        assert app.add_middleware.call_count == 3

    def test_setup_middleware_order(self):
        """Test that middleware is added in correct order."""
        app = MagicMock()
        added_middlewares = []

        def track_middleware(middleware_class, **kwargs):
            added_middlewares.append(middleware_class)

        app.add_middleware = track_middleware

        with patch("middleware.logger"):
            setup_middleware(app, enable_rate_limiting=True)

        # Order: Error, Logging, Correlation, RateLimit
        assert added_middlewares[0] == ErrorHandlingMiddleware
        assert added_middlewares[1] == RequestLoggingMiddleware
        assert added_middlewares[2] == CorrelationIdMiddleware
        assert added_middlewares[3] == RateLimitMiddleware


class TestCorrelationIdContextVar:
    """Tests for correlation ID context variable."""

    def test_context_var_default_empty_string(self):
        """Test that context var default is empty string."""
        # Create new context to ensure clean state
        import contextvars
        ctx = contextvars.copy_context()
        result = ctx.run(get_correlation_id)
        assert result == ""

    def test_context_var_set_and_get(self):
        """Test setting and getting correlation ID."""
        test_id = "test-id-789"
        token = correlation_id_var.set(test_id)

        try:
            assert get_correlation_id() == test_id
        finally:
            correlation_id_var.reset(token)

    def test_context_var_reset(self):
        """Test resetting correlation ID."""
        test_id = "temp-id"
        token = correlation_id_var.set(test_id)
        correlation_id_var.reset(token)

        assert get_correlation_id() == ""
