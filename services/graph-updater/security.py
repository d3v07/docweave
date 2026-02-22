"""
Security Utilities for Graph Updater Service
=============================================

Provides security middleware, rate limiting, input validation, and API key handling.
Follows OWASP best practices for API security.

Security Features:
    - IP-based and user-based rate limiting
    - API key authentication for admin endpoints
    - Input sanitization and validation
    - Security headers middleware
    - Request ID tracking for audit trails
"""

import hashlib
import hmac
import os
import re
import secrets
import time
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Callable, Optional

from fastapi import HTTPException, Request, Response, Depends
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field, field_validator
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)


# =============================================================================
# Configuration - Load from Environment Variables
# =============================================================================

class SecurityConfig:
    """
    Security configuration loaded from environment variables.

    All sensitive values MUST be set via environment variables.
    Never hardcode API keys, secrets, or credentials.
    """

    # Rate limiting defaults (can be overridden via env vars)
    RATE_LIMIT_REQUESTS_PER_MINUTE: int = int(os.getenv("RATE_LIMIT_RPM", "60"))
    RATE_LIMIT_REQUESTS_PER_HOUR: int = int(os.getenv("RATE_LIMIT_RPH", "1000"))
    RATE_LIMIT_BURST: int = int(os.getenv("RATE_LIMIT_BURST", "10"))

    # Admin API key (REQUIRED for admin endpoints in production)
    # Generate with: python -c "import secrets; print(secrets.token_urlsafe(32))"
    ADMIN_API_KEY: Optional[str] = os.getenv("GRAPH_UPDATER_ADMIN_API_KEY")

    # API key header name
    API_KEY_HEADER: str = "X-API-Key"

    # Allowed origins for CORS (comma-separated in env var)
    ALLOWED_ORIGINS: list[str] = os.getenv(
        "ALLOWED_ORIGINS",
        "http://localhost:3000,http://127.0.0.1:3000"
    ).split(",")

    # Input validation limits
    MAX_ENTITY_NAME_LENGTH: int = 500
    MAX_PREDICATE_LENGTH: int = 200
    MAX_QUERY_LENGTH: int = 1000
    MAX_REASON_LENGTH: int = 2000
    MAX_BATCH_SIZE: int = 100
    MAX_ALIASES_COUNT: int = 50

    # Sanitization patterns
    # Disallow common injection patterns
    DANGEROUS_PATTERNS: list[str] = [
        r'<script',  # XSS
        r'javascript:',  # XSS
        r'on\w+\s*=',  # Event handlers
        r'\$\{',  # Template injection
        r'\{\{',  # Template injection
        r'`',  # Backtick (template literals)
    ]

    @classmethod
    def validate(cls) -> list[str]:
        """
        Validate security configuration.
        Returns list of warnings for non-critical issues.
        """
        warnings = []

        if not cls.ADMIN_API_KEY:
            warnings.append(
                "GRAPH_UPDATER_ADMIN_API_KEY not set. "
                "Admin endpoints will be disabled in production mode."
            )
        elif len(cls.ADMIN_API_KEY) < 32:
            warnings.append(
                "ADMIN_API_KEY should be at least 32 characters. "
                "Generate with: python -c \"import secrets; print(secrets.token_urlsafe(32))\""
            )

        return warnings


# =============================================================================
# Rate Limiting
# =============================================================================

class RateLimiter:
    """
    Token bucket rate limiter with IP and user-based limits.

    Provides:
    - Per-IP rate limiting (default)
    - Per-user rate limiting (when API key is provided)
    - Burst allowance for legitimate traffic spikes
    - Graceful 429 responses with Retry-After header
    """

    def __init__(
        self,
        requests_per_minute: int = 60,
        requests_per_hour: int = 1000,
        burst: int = 10,
    ):
        self.rpm = requests_per_minute
        self.rph = requests_per_hour
        self.burst = burst

        # Track requests per identifier (IP or user)
        # Format: {identifier: [(timestamp, count), ...]}
        self._minute_buckets: dict[str, list[float]] = defaultdict(list)
        self._hour_buckets: dict[str, list[float]] = defaultdict(list)

        # Last cleanup time
        self._last_cleanup = time.time()

    def _get_identifier(self, request: Request) -> str:
        """
        Get rate limit identifier from request.
        Uses API key if present, otherwise client IP.
        """
        # Check for API key (user-based limiting)
        api_key = request.headers.get(SecurityConfig.API_KEY_HEADER)
        if api_key:
            # Hash the API key so we don't store it in memory
            return f"user:{hashlib.sha256(api_key.encode()).hexdigest()[:16]}"

        # Fall back to IP-based limiting
        # Handle X-Forwarded-For for proxied requests
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            # Take first IP (original client)
            client_ip = forwarded.split(",")[0].strip()
        else:
            client_ip = request.client.host if request.client else "unknown"

        return f"ip:{client_ip}"

    def _cleanup_old_entries(self, now: float) -> None:
        """Remove entries older than the tracking window."""
        # Only cleanup every 60 seconds to avoid overhead
        if now - self._last_cleanup < 60:
            return

        self._last_cleanup = now
        minute_ago = now - 60
        hour_ago = now - 3600

        # Cleanup minute buckets
        for key in list(self._minute_buckets.keys()):
            self._minute_buckets[key] = [
                ts for ts in self._minute_buckets[key] if ts > minute_ago
            ]
            if not self._minute_buckets[key]:
                del self._minute_buckets[key]

        # Cleanup hour buckets
        for key in list(self._hour_buckets.keys()):
            self._hour_buckets[key] = [
                ts for ts in self._hour_buckets[key] if ts > hour_ago
            ]
            if not self._hour_buckets[key]:
                del self._hour_buckets[key]

    def check_rate_limit(self, request: Request) -> tuple[bool, Optional[int]]:
        """
        Check if request is within rate limits.

        Returns:
            (allowed, retry_after_seconds)
            - allowed: True if request should proceed
            - retry_after_seconds: Seconds until limit resets (if blocked)
        """
        now = time.time()
        identifier = self._get_identifier(request)

        # Cleanup old entries periodically
        self._cleanup_old_entries(now)

        # Check minute limit
        minute_ago = now - 60
        minute_requests = [
            ts for ts in self._minute_buckets[identifier] if ts > minute_ago
        ]

        if len(minute_requests) >= self.rpm + self.burst:
            # Calculate retry-after
            oldest = min(minute_requests) if minute_requests else now
            retry_after = int(60 - (now - oldest)) + 1
            return False, max(1, retry_after)

        # Check hour limit
        hour_ago = now - 3600
        hour_requests = [
            ts for ts in self._hour_buckets[identifier] if ts > hour_ago
        ]

        if len(hour_requests) >= self.rph:
            oldest = min(hour_requests) if hour_requests else now
            retry_after = int(3600 - (now - oldest)) + 1
            return False, max(1, retry_after)

        # Record this request
        self._minute_buckets[identifier].append(now)
        self._hour_buckets[identifier].append(now)

        return True, None

    def get_limit_headers(self, request: Request) -> dict[str, str]:
        """Get rate limit headers for response."""
        now = time.time()
        identifier = self._get_identifier(request)

        minute_ago = now - 60
        minute_requests = len([
            ts for ts in self._minute_buckets.get(identifier, []) if ts > minute_ago
        ])

        return {
            "X-RateLimit-Limit": str(self.rpm),
            "X-RateLimit-Remaining": str(max(0, self.rpm - minute_requests)),
            "X-RateLimit-Reset": str(int(now) + 60),
        }


# Global rate limiter instance
rate_limiter = RateLimiter(
    requests_per_minute=SecurityConfig.RATE_LIMIT_REQUESTS_PER_MINUTE,
    requests_per_hour=SecurityConfig.RATE_LIMIT_REQUESTS_PER_HOUR,
    burst=SecurityConfig.RATE_LIMIT_BURST,
)


# =============================================================================
# API Key Authentication
# =============================================================================

api_key_header = APIKeyHeader(
    name=SecurityConfig.API_KEY_HEADER,
    auto_error=False
)


async def verify_admin_api_key(
    api_key: Optional[str] = Depends(api_key_header)
) -> bool:
    """
    Verify admin API key for protected endpoints.

    SECURITY: Uses constant-time comparison to prevent timing attacks.
    """
    if not SecurityConfig.ADMIN_API_KEY:
        # In development, warn but allow if no key is configured
        if os.getenv("ENVIRONMENT", "development") == "production":
            raise HTTPException(
                status_code=503,
                detail="Admin endpoints disabled - API key not configured"
            )
        logger.warning("Admin endpoint accessed without API key configured")
        return True

    if not api_key:
        raise HTTPException(
            status_code=401,
            detail="Missing API key",
            headers={"WWW-Authenticate": "ApiKey"}
        )

    # Constant-time comparison to prevent timing attacks
    if not hmac.compare_digest(api_key, SecurityConfig.ADMIN_API_KEY):
        logger.warning(f"Invalid admin API key attempt")
        raise HTTPException(
            status_code=403,
            detail="Invalid API key"
        )

    return True


# =============================================================================
# Input Sanitization
# =============================================================================

class InputSanitizer:
    """
    Input sanitization utilities.

    Provides defense against:
    - XSS attacks
    - Template injection
    - Cypher injection (for Neo4j)
    """

    # Characters that could be used for Cypher injection
    CYPHER_SPECIAL_CHARS = re.compile(r'[`\'\"\\;\-\[\]\(\)\{\}]')

    @classmethod
    def sanitize_string(cls, value: str, max_length: int = 1000) -> str:
        """
        Sanitize a string input.

        - Truncates to max length
        - Strips leading/trailing whitespace
        - Checks for dangerous patterns
        """
        if not isinstance(value, str):
            return str(value)[:max_length]

        # Truncate and strip
        value = value[:max_length].strip()

        # Check for dangerous patterns
        lower_value = value.lower()
        for pattern in SecurityConfig.DANGEROUS_PATTERNS:
            if re.search(pattern, lower_value, re.IGNORECASE):
                logger.warning(f"Dangerous pattern detected in input: {pattern}")
                raise HTTPException(
                    status_code=400,
                    detail="Invalid input: potentially dangerous content detected"
                )

        return value

    @classmethod
    def sanitize_for_cypher(cls, value: str) -> str:
        """
        Sanitize string for safe use in Cypher queries.

        Note: Always use parameterized queries. This is an additional safety layer.
        """
        # Escape single quotes (Cypher string delimiter)
        return value.replace("'", "\\'").replace("\\", "\\\\")

    @classmethod
    def sanitize_entity_id(cls, entity_id: str) -> str:
        """
        Validate and sanitize entity ID format.

        Expected format: ent_<alphanumeric>
        """
        if not entity_id:
            raise HTTPException(status_code=400, detail="Entity ID required")

        # Allow common ID formats: alphanumeric with underscores and hyphens
        if not re.match(r'^[a-zA-Z0-9_-]+$', entity_id):
            raise HTTPException(
                status_code=400,
                detail="Invalid entity ID format"
            )

        return entity_id[:100]  # Max length

    @classmethod
    def sanitize_predicate(cls, predicate: str) -> str:
        """
        Validate and sanitize predicate format.

        Predicates should be snake_case identifiers.
        """
        if not predicate:
            raise HTTPException(status_code=400, detail="Predicate required")

        # Allow snake_case with alphanumeric
        predicate = predicate.strip()[:SecurityConfig.MAX_PREDICATE_LENGTH]

        if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', predicate):
            raise HTTPException(
                status_code=400,
                detail="Invalid predicate format. Use snake_case (e.g., 'employee_count')"
            )

        return predicate


# =============================================================================
# Security Headers Middleware
# =============================================================================

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """
    Add security headers to all responses.

    Headers added:
    - X-Content-Type-Options: nosniff
    - X-Frame-Options: DENY
    - X-XSS-Protection: 1; mode=block
    - Strict-Transport-Security (for HTTPS)
    - Content-Security-Policy
    - X-Request-ID (for tracing)
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Generate request ID for tracing
        request_id = secrets.token_hex(8)

        # Store request ID in request state for logging
        request.state.request_id = request_id

        # Process request
        response = await call_next(request)

        # Add security headers
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["X-Request-ID"] = request_id

        # Add HSTS for HTTPS requests
        if request.url.scheme == "https":
            response.headers["Strict-Transport-Security"] = (
                "max-age=31536000; includeSubDomains"
            )

        # Content Security Policy for API responses
        response.headers["Content-Security-Policy"] = (
            "default-src 'none'; frame-ancestors 'none'"
        )

        # Prevent caching of sensitive data
        if "/admin" in request.url.path:
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
            response.headers["Pragma"] = "no-cache"

        return response


# =============================================================================
# Rate Limit Middleware
# =============================================================================

class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Apply rate limiting to all requests.

    Returns 429 Too Many Requests when limit is exceeded,
    with Retry-After header indicating when to retry.
    """

    # Endpoints exempt from rate limiting
    EXEMPT_PATHS = {"/health", "/ready", "/docs", "/openapi.json", "/redoc"}

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Skip rate limiting for health checks and docs
        if request.url.path in self.EXEMPT_PATHS:
            return await call_next(request)

        # Check rate limit
        allowed, retry_after = rate_limiter.check_rate_limit(request)

        if not allowed:
            logger.warning(
                f"Rate limit exceeded for {rate_limiter._get_identifier(request)}"
            )
            return Response(
                content='{"detail": "Too many requests. Please slow down."}',
                status_code=429,
                media_type="application/json",
                headers={
                    "Retry-After": str(retry_after),
                    **rate_limiter.get_limit_headers(request),
                }
            )

        # Process request and add rate limit headers
        response = await call_next(request)

        # Add rate limit headers to response
        for key, value in rate_limiter.get_limit_headers(request).items():
            response.headers[key] = value

        return response


# =============================================================================
# Validated Request Models
# =============================================================================

class SecureClaimRequest(BaseModel):
    """
    Secure claim request with strict validation.

    All fields have length limits and format validation.
    """
    subject_entity_id: str = Field(
        ...,
        min_length=1,
        max_length=100,
        pattern=r'^[a-zA-Z0-9_-]+$',
        description="Entity ID in format: ent_xxx"
    )
    predicate: str = Field(
        ...,
        min_length=1,
        max_length=SecurityConfig.MAX_PREDICATE_LENGTH,
        pattern=r'^[a-zA-Z][a-zA-Z0-9_]*$',
        description="Predicate in snake_case format"
    )
    object_value: Any = Field(..., description="Claim value")
    source_id: str = Field(
        ...,
        min_length=1,
        max_length=200,
        description="Source document ID"
    )
    confidence: float = Field(
        default=0.8,
        ge=0.0,
        le=1.0,
        description="Confidence score between 0 and 1"
    )
    extracted_text: Optional[str] = Field(
        default=None,
        max_length=10000,
        description="Original extracted text"
    )
    object_entity_id: Optional[str] = Field(
        default=None,
        max_length=100,
        pattern=r'^[a-zA-Z0-9_-]*$'
    )
    valid_from: Optional[datetime] = None
    valid_until: Optional[datetime] = None
    check_conflicts: bool = Field(default=True)
    auto_supersede: bool = Field(default=False)

    model_config = {"extra": "forbid"}  # Reject unexpected fields

    @field_validator('object_value')
    @classmethod
    def validate_object_value(cls, v: Any) -> Any:
        """Validate and sanitize object value."""
        if isinstance(v, str):
            if len(v) > 10000:
                raise ValueError("Value too long (max 10000 characters)")
            # Check for dangerous patterns
            InputSanitizer.sanitize_string(v, max_length=10000)
        return v


class SecureBatchClaimRequest(BaseModel):
    """Secure batch claim request with validation."""
    claims: list[dict[str, Any]] = Field(
        ...,
        max_length=SecurityConfig.MAX_BATCH_SIZE,
        description=f"List of claims (max {SecurityConfig.MAX_BATCH_SIZE})"
    )
    check_conflicts: bool = Field(default=True)
    auto_resolve: bool = Field(default=False)
    resolution_strategy: Optional[str] = Field(
        default=None,
        max_length=50,
        pattern=r'^[a-z_]+$'
    )
    source_id: Optional[str] = Field(default=None, max_length=200)

    model_config = {"extra": "forbid"}


class SecureEntityMergeRequest(BaseModel):
    """Secure entity merge request."""
    target_entity_id: str = Field(
        ...,
        min_length=1,
        max_length=100,
        pattern=r'^[a-zA-Z0-9_-]+$'
    )
    source_entity_ids: list[str] = Field(
        ...,
        min_length=1,
        max_length=50,
        description="Entity IDs to merge (max 50)"
    )
    merge_aliases: bool = Field(default=True)
    merge_claims: bool = Field(default=True)
    resolved_by: str = Field(
        default="api_user",
        max_length=100,
        pattern=r'^[a-zA-Z0-9_-]+$'
    )

    model_config = {"extra": "forbid"}

    @field_validator('source_entity_ids')
    @classmethod
    def validate_source_ids(cls, v: list[str]) -> list[str]:
        """Validate source entity IDs."""
        for eid in v:
            if not re.match(r'^[a-zA-Z0-9_-]+$', eid):
                raise ValueError(f"Invalid entity ID format: {eid}")
            if len(eid) > 100:
                raise ValueError(f"Entity ID too long: {eid}")
        return v


class SecureResolveConflictRequest(BaseModel):
    """Secure conflict resolution request."""
    strategy: Optional[str] = Field(
        default=None,
        max_length=50,
        pattern=r'^[A-Z_]+$'
    )
    winning_claim_id: Optional[str] = Field(
        default=None,
        max_length=100,
        pattern=r'^[a-zA-Z0-9_-]*$'
    )
    reason: Optional[str] = Field(
        default=None,
        max_length=SecurityConfig.MAX_REASON_LENGTH
    )
    resolved_by: str = Field(
        default="api_user",
        max_length=100,
        pattern=r'^[a-zA-Z0-9_-]+$'
    )
    force: bool = Field(default=False)

    model_config = {"extra": "forbid"}

    @field_validator('reason')
    @classmethod
    def validate_reason(cls, v: Optional[str]) -> Optional[str]:
        """Sanitize reason text."""
        if v:
            return InputSanitizer.sanitize_string(
                v,
                max_length=SecurityConfig.MAX_REASON_LENGTH
            )
        return v


# =============================================================================
# Utility Functions
# =============================================================================

def generate_api_key() -> str:
    """
    Generate a secure API key.

    Usage: python -c "from security import generate_api_key; print(generate_api_key())"
    """
    return secrets.token_urlsafe(32)


def validate_security_config() -> None:
    """
    Validate security configuration on startup.
    Logs warnings for potential security issues.
    """
    warnings = SecurityConfig.validate()
    for warning in warnings:
        logger.warning(f"SECURITY WARNING: {warning}")

    if not warnings:
        logger.info("Security configuration validated successfully")


# Run validation on import
validate_security_config()
