"""
FastAPI Middleware for Graph Updater Service
=============================================

This module provides middleware components for the graph-updater service:
- Request logging and tracing
- Error handling and standardization
- Rate limiting
- Request validation
- Correlation ID propagation
"""

import logging
import time
import uuid
from contextvars import ContextVar
from datetime import datetime
from typing import Any, Callable, Optional

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)

# Context variable for request correlation ID
correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="")


def get_correlation_id() -> str:
    """Get the current correlation ID."""
    return correlation_id_var.get()


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """
    Middleware to propagate or generate correlation IDs.

    Extracts correlation ID from request headers or generates a new one.
    The correlation ID is made available via context variable and added
    to response headers.
    """

    HEADER_NAME = "X-Correlation-ID"

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process the request and add correlation ID."""
        # Get or generate correlation ID
        correlation_id = request.headers.get(self.HEADER_NAME)
        if not correlation_id:
            correlation_id = str(uuid.uuid4())

        # Set context variable
        token = correlation_id_var.set(correlation_id)

        try:
            response = await call_next(request)
            response.headers[self.HEADER_NAME] = correlation_id
            return response
        finally:
            correlation_id_var.reset(token)


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware to log all requests with timing information.

    Logs request method, path, status code, and duration.
    Includes correlation ID for request tracing.
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process the request and log details."""
        start_time = time.perf_counter()
        correlation_id = get_correlation_id()

        # Log request start
        logger.info(
            f"Request started: {request.method} {request.url.path}",
            extra={
                "correlation_id": correlation_id,
                "method": request.method,
                "path": request.url.path,
                "query_params": str(request.query_params),
            }
        )

        try:
            response = await call_next(request)
            duration_ms = (time.perf_counter() - start_time) * 1000

            # Log request completion
            logger.info(
                f"Request completed: {request.method} {request.url.path} - {response.status_code} ({duration_ms:.2f}ms)",
                extra={
                    "correlation_id": correlation_id,
                    "method": request.method,
                    "path": request.url.path,
                    "status_code": response.status_code,
                    "duration_ms": duration_ms,
                }
            )

            # Add timing header
            response.headers["X-Response-Time-Ms"] = f"{duration_ms:.2f}"

            return response

        except Exception as e:
            duration_ms = (time.perf_counter() - start_time) * 1000
            logger.error(
                f"Request failed: {request.method} {request.url.path} - {e}",
                extra={
                    "correlation_id": correlation_id,
                    "method": request.method,
                    "path": request.url.path,
                    "duration_ms": duration_ms,
                    "error": str(e),
                },
                exc_info=True,
            )
            raise


class ErrorHandlingMiddleware(BaseHTTPMiddleware):
    """
    Middleware to catch and standardize error responses.

    Ensures all errors return a consistent JSON format with
    correlation ID for debugging.
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process the request and handle errors."""
        correlation_id = get_correlation_id()

        try:
            return await call_next(request)

        except Exception as e:
            logger.exception(
                f"Unhandled exception: {e}",
                extra={"correlation_id": correlation_id}
            )

            return JSONResponse(
                status_code=500,
                content={
                    "error": "Internal Server Error",
                    "message": str(e) if not isinstance(e, Exception) else "An unexpected error occurred",
                    "correlation_id": correlation_id,
                    "timestamp": datetime.utcnow().isoformat(),
                },
                headers={"X-Correlation-ID": correlation_id},
            )


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Simple in-memory rate limiting middleware.

    Limits requests per client based on IP address.
    For production, use Redis-based rate limiting.
    """

    def __init__(
        self,
        app: FastAPI,
        requests_per_minute: int = 100,
        burst_limit: int = 20,
    ):
        """
        Initialize rate limiter.

        Args:
            app: FastAPI application
            requests_per_minute: Maximum requests per minute per client
            burst_limit: Maximum burst requests allowed
        """
        super().__init__(app)
        self.requests_per_minute = requests_per_minute
        self.burst_limit = burst_limit
        self._clients: dict[str, list[float]] = {}
        self._window_seconds = 60

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process the request with rate limiting."""
        # Skip rate limiting for health endpoints
        if request.url.path in ("/health", "/ready", "/metrics"):
            return await call_next(request)

        client_ip = self._get_client_ip(request)
        current_time = time.time()

        # Clean old entries and check rate limit
        if not self._is_allowed(client_ip, current_time):
            correlation_id = get_correlation_id()
            return JSONResponse(
                status_code=429,
                content={
                    "error": "Too Many Requests",
                    "message": f"Rate limit exceeded. Maximum {self.requests_per_minute} requests per minute.",
                    "correlation_id": correlation_id,
                    "retry_after_seconds": self._get_retry_after(client_ip, current_time),
                },
                headers={
                    "X-Correlation-ID": correlation_id,
                    "Retry-After": str(self._get_retry_after(client_ip, current_time)),
                },
            )

        # Record this request
        self._record_request(client_ip, current_time)

        response = await call_next(request)

        # Add rate limit headers
        remaining = self._get_remaining(client_ip, current_time)
        response.headers["X-RateLimit-Limit"] = str(self.requests_per_minute)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        response.headers["X-RateLimit-Reset"] = str(int(current_time + self._window_seconds))

        return response

    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP from request."""
        # Check for forwarded header (behind proxy)
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()

        # Fall back to direct client
        return request.client.host if request.client else "unknown"

    def _is_allowed(self, client_ip: str, current_time: float) -> bool:
        """Check if request is allowed under rate limit."""
        if client_ip not in self._clients:
            return True

        # Clean old entries
        window_start = current_time - self._window_seconds
        self._clients[client_ip] = [
            t for t in self._clients[client_ip] if t > window_start
        ]

        return len(self._clients[client_ip]) < self.requests_per_minute

    def _record_request(self, client_ip: str, current_time: float) -> None:
        """Record a request timestamp."""
        if client_ip not in self._clients:
            self._clients[client_ip] = []
        self._clients[client_ip].append(current_time)

    def _get_remaining(self, client_ip: str, current_time: float) -> int:
        """Get remaining requests in window."""
        if client_ip not in self._clients:
            return self.requests_per_minute

        window_start = current_time - self._window_seconds
        recent_requests = len([t for t in self._clients[client_ip] if t > window_start])
        return max(0, self.requests_per_minute - recent_requests)

    def _get_retry_after(self, client_ip: str, current_time: float) -> int:
        """Get seconds until rate limit resets."""
        if client_ip not in self._clients or not self._clients[client_ip]:
            return 0

        oldest_in_window = min(self._clients[client_ip])
        return max(1, int(oldest_in_window + self._window_seconds - current_time))


def setup_middleware(app: FastAPI, enable_rate_limiting: bool = True) -> None:
    """
    Set up all middleware for the FastAPI application.

    Args:
        app: FastAPI application instance
        enable_rate_limiting: Whether to enable rate limiting
    """
    # Order matters - outermost middleware runs first

    # Error handling (catches all unhandled exceptions)
    app.add_middleware(ErrorHandlingMiddleware)

    # Request logging
    app.add_middleware(RequestLoggingMiddleware)

    # Correlation ID
    app.add_middleware(CorrelationIdMiddleware)

    # Rate limiting (optional)
    if enable_rate_limiting:
        app.add_middleware(
            RateLimitMiddleware,
            requests_per_minute=200,
            burst_limit=50,
        )

    logger.info("Middleware configured successfully")
