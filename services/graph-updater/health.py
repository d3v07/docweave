"""
Health Check and Monitoring Module
===================================

This module provides comprehensive health checks, readiness probes,
and liveness probes for the graph-updater service. It includes
detailed diagnostics for all service dependencies.

These endpoints are designed to work with Kubernetes health probes
and container orchestration systems.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class HealthStatus(str, Enum):
    """Health status levels."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class ComponentHealth:
    """Health status of a service component."""
    name: str
    status: HealthStatus
    latency_ms: Optional[float] = None
    message: Optional[str] = None
    last_checked: datetime = field(default_factory=datetime.utcnow)
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "status": self.status.value,
            "latency_ms": self.latency_ms,
            "message": self.message,
            "last_checked": self.last_checked.isoformat(),
            "details": self.details,
        }


@dataclass
class ServiceHealth:
    """Overall service health."""
    status: HealthStatus
    service_name: str = "graph-updater"
    version: str = "0.4.0"
    uptime_seconds: float = 0.0
    components: list[ComponentHealth] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "status": self.status.value,
            "service_name": self.service_name,
            "version": self.version,
            "uptime_seconds": self.uptime_seconds,
            "components": [c.to_dict() for c in self.components],
            "timestamp": self.timestamp.isoformat(),
        }


class HealthChecker:
    """
    Comprehensive health checker for the graph-updater service.

    Performs health checks on all service dependencies and provides
    aggregated health status for Kubernetes probes.

    Example:
        checker = HealthChecker(neo4j_client, kafka_consumer)
        health = await checker.check_all()

        if health.status == HealthStatus.HEALTHY:
            print("Service is healthy")
    """

    def __init__(
        self,
        neo4j_client=None,
        claims_consumer=None,
        updates_consumer=None,
        truth_layer=None,
    ):
        """
        Initialize the health checker.

        Args:
            neo4j_client: Neo4j client instance
            claims_consumer: Claims Kafka consumer
            updates_consumer: Graph update Kafka consumer
            truth_layer: Truth layer instance
        """
        self.neo4j_client = neo4j_client
        self.claims_consumer = claims_consumer
        self.updates_consumer = updates_consumer
        self.truth_layer = truth_layer
        self._start_time = datetime.utcnow()

    async def check_all(self) -> ServiceHealth:
        """
        Check health of all components.

        Returns:
            ServiceHealth with aggregated status
        """
        components = []

        # Check Neo4j
        neo4j_health = await self._check_neo4j()
        components.append(neo4j_health)

        # Check Kafka consumers
        claims_health = self._check_claims_consumer()
        components.append(claims_health)

        updates_health = self._check_updates_consumer()
        components.append(updates_health)

        # Check truth layer cache
        truth_health = self._check_truth_layer()
        components.append(truth_health)

        # Determine overall status
        overall_status = self._aggregate_status(components)

        uptime = (datetime.utcnow() - self._start_time).total_seconds()

        return ServiceHealth(
            status=overall_status,
            uptime_seconds=uptime,
            components=components,
        )

    async def check_liveness(self) -> bool:
        """
        Liveness probe - is the service running?

        Returns:
            True if service is alive
        """
        # For liveness, we just check if the service is running
        # This should be a very fast check
        return True

    async def check_readiness(self) -> bool:
        """
        Readiness probe - is the service ready to accept traffic?

        Returns:
            True if service is ready
        """
        # For readiness, we need Neo4j to be connected
        if not self.neo4j_client:
            return False

        try:
            return await self.neo4j_client.health_check()
        except Exception:
            return False

    async def _check_neo4j(self) -> ComponentHealth:
        """Check Neo4j connectivity and latency."""
        if not self.neo4j_client:
            return ComponentHealth(
                name="neo4j",
                status=HealthStatus.UNHEALTHY,
                message="Neo4j client not initialized",
            )

        start = datetime.utcnow()
        try:
            is_healthy = await self.neo4j_client.health_check()
            latency = (datetime.utcnow() - start).total_seconds() * 1000

            if is_healthy:
                status = HealthStatus.HEALTHY if latency < 100 else HealthStatus.DEGRADED
                return ComponentHealth(
                    name="neo4j",
                    status=status,
                    latency_ms=latency,
                    message="Connected" if latency < 100 else f"High latency: {latency:.0f}ms",
                )
            else:
                return ComponentHealth(
                    name="neo4j",
                    status=HealthStatus.UNHEALTHY,
                    message="Health check failed",
                )

        except Exception as e:
            return ComponentHealth(
                name="neo4j",
                status=HealthStatus.UNHEALTHY,
                message=str(e),
            )

    def _check_claims_consumer(self) -> ComponentHealth:
        """Check claims consumer status."""
        if not self.claims_consumer:
            return ComponentHealth(
                name="claims_consumer",
                status=HealthStatus.UNKNOWN,
                message="Consumer not initialized",
            )

        running = getattr(self.claims_consumer, "_running", False)
        stats = {}

        if hasattr(self.claims_consumer, "get_stats"):
            stats = self.claims_consumer.get_stats()

        if running:
            return ComponentHealth(
                name="claims_consumer",
                status=HealthStatus.HEALTHY,
                message="Running",
                details=stats,
            )
        else:
            return ComponentHealth(
                name="claims_consumer",
                status=HealthStatus.DEGRADED,
                message="Consumer not running",
                details=stats,
            )

    def _check_updates_consumer(self) -> ComponentHealth:
        """Check updates consumer status."""
        if not self.updates_consumer:
            return ComponentHealth(
                name="updates_consumer",
                status=HealthStatus.UNKNOWN,
                message="Consumer not initialized",
            )

        running = getattr(self.updates_consumer, "_running", False)

        if running:
            return ComponentHealth(
                name="updates_consumer",
                status=HealthStatus.HEALTHY,
                message="Running",
            )
        else:
            return ComponentHealth(
                name="updates_consumer",
                status=HealthStatus.DEGRADED,
                message="Consumer not running",
            )

    def _check_truth_layer(self) -> ComponentHealth:
        """Check truth layer cache status."""
        if not self.truth_layer:
            return ComponentHealth(
                name="truth_layer",
                status=HealthStatus.UNKNOWN,
                message="Truth layer not initialized",
            )

        cache_size = len(getattr(self.truth_layer, "_cache", {}))

        return ComponentHealth(
            name="truth_layer",
            status=HealthStatus.HEALTHY,
            message=f"Cache entries: {cache_size}",
            details={"cache_size": cache_size},
        )

    def _aggregate_status(self, components: list[ComponentHealth]) -> HealthStatus:
        """
        Aggregate component statuses into overall status.

        Args:
            components: List of component health statuses

        Returns:
            Overall health status
        """
        if not components:
            return HealthStatus.UNKNOWN

        statuses = [c.status for c in components]

        # If any component is unhealthy, the service is unhealthy
        if HealthStatus.UNHEALTHY in statuses:
            # But only if it's a critical component (neo4j)
            for c in components:
                if c.name == "neo4j" and c.status == HealthStatus.UNHEALTHY:
                    return HealthStatus.UNHEALTHY
            return HealthStatus.DEGRADED

        # If any component is degraded, the service is degraded
        if HealthStatus.DEGRADED in statuses:
            return HealthStatus.DEGRADED

        # If any component is unknown, check others
        if HealthStatus.UNKNOWN in statuses:
            # All known components should be healthy
            known_components = [c for c in components if c.status != HealthStatus.UNKNOWN]
            if all(c.status == HealthStatus.HEALTHY for c in known_components):
                return HealthStatus.HEALTHY
            return HealthStatus.DEGRADED

        return HealthStatus.HEALTHY


class MetricsCollector:
    """
    Collects and exposes metrics for the graph-updater service.

    Provides Prometheus-compatible metrics for monitoring dashboards.
    """

    def __init__(self):
        """Initialize metrics collector."""
        self._start_time = datetime.utcnow()
        self._claims_processed = 0
        self._conflicts_detected = 0
        self._conflicts_resolved = 0
        self._errors = 0
        self._cache_hits = 0
        self._cache_misses = 0

    def increment_claims_processed(self, count: int = 1) -> None:
        """Increment claims processed counter."""
        self._claims_processed += count

    def increment_conflicts_detected(self, count: int = 1) -> None:
        """Increment conflicts detected counter."""
        self._conflicts_detected += count

    def increment_conflicts_resolved(self, count: int = 1) -> None:
        """Increment conflicts resolved counter."""
        self._conflicts_resolved += count

    def increment_errors(self, count: int = 1) -> None:
        """Increment error counter."""
        self._errors += count

    def record_cache_hit(self) -> None:
        """Record a cache hit."""
        self._cache_hits += 1

    def record_cache_miss(self) -> None:
        """Record a cache miss."""
        self._cache_misses += 1

    def get_metrics(self) -> dict[str, Any]:
        """
        Get all metrics.

        Returns:
            Dictionary of metric values
        """
        uptime = (datetime.utcnow() - self._start_time).total_seconds()
        cache_total = self._cache_hits + self._cache_misses
        cache_hit_rate = self._cache_hits / cache_total if cache_total > 0 else 0.0

        return {
            "uptime_seconds": uptime,
            "claims_processed_total": self._claims_processed,
            "conflicts_detected_total": self._conflicts_detected,
            "conflicts_resolved_total": self._conflicts_resolved,
            "errors_total": self._errors,
            "cache_hits_total": self._cache_hits,
            "cache_misses_total": self._cache_misses,
            "cache_hit_rate": cache_hit_rate,
        }

    def to_prometheus_format(self) -> str:
        """
        Export metrics in Prometheus format.

        Returns:
            Prometheus-formatted metrics string
        """
        metrics = self.get_metrics()
        lines = [
            "# HELP docweave_graph_updater_uptime_seconds Service uptime in seconds",
            "# TYPE docweave_graph_updater_uptime_seconds gauge",
            f"docweave_graph_updater_uptime_seconds {metrics['uptime_seconds']:.2f}",
            "",
            "# HELP docweave_graph_updater_claims_processed_total Total claims processed",
            "# TYPE docweave_graph_updater_claims_processed_total counter",
            f"docweave_graph_updater_claims_processed_total {metrics['claims_processed_total']}",
            "",
            "# HELP docweave_graph_updater_conflicts_detected_total Total conflicts detected",
            "# TYPE docweave_graph_updater_conflicts_detected_total counter",
            f"docweave_graph_updater_conflicts_detected_total {metrics['conflicts_detected_total']}",
            "",
            "# HELP docweave_graph_updater_conflicts_resolved_total Total conflicts resolved",
            "# TYPE docweave_graph_updater_conflicts_resolved_total counter",
            f"docweave_graph_updater_conflicts_resolved_total {metrics['conflicts_resolved_total']}",
            "",
            "# HELP docweave_graph_updater_errors_total Total errors",
            "# TYPE docweave_graph_updater_errors_total counter",
            f"docweave_graph_updater_errors_total {metrics['errors_total']}",
            "",
            "# HELP docweave_graph_updater_cache_hit_rate Cache hit rate",
            "# TYPE docweave_graph_updater_cache_hit_rate gauge",
            f"docweave_graph_updater_cache_hit_rate {metrics['cache_hit_rate']:.4f}",
            "",
        ]
        return "\n".join(lines)
