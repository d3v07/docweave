"""
Tests for Health Check Module
=============================

Unit tests for the health check functionality covering:
- Component health checks (Neo4j, Kafka)
- Overall service health aggregation
- Metrics collection
- Health status transitions
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

# Import from conftest's path setup and mocking
from health import (
    HealthChecker,
    HealthStatus,
    ComponentHealth,
    ServiceHealth,
    MetricsCollector,
)


class MockQueryResult:
    """Mock Neo4j query result."""

    def __init__(self, records=None):
        self.records = records or []


@pytest.fixture
def mock_neo4j_client():
    """Create a mock Neo4j client."""
    client = AsyncMock()
    client.execute_query = AsyncMock(return_value=MockQueryResult([{"1": 1}]))
    client.verify_connectivity = AsyncMock(return_value=True)
    client.health_check = AsyncMock(return_value=True)
    return client


@pytest.fixture
def mock_kafka_producer():
    """Create a mock Kafka producer."""
    producer = AsyncMock()
    producer.send_and_wait = AsyncMock()
    return producer


@pytest.fixture
def mock_kafka_consumer():
    """Create a mock Kafka consumer."""
    consumer = AsyncMock()
    consumer.committed = AsyncMock(return_value={})
    return consumer


@pytest.fixture
def mock_claims_consumer():
    """Create a mock claims consumer."""
    consumer = MagicMock()
    consumer._running = True
    consumer.get_stats = MagicMock(return_value={
        "processed_count": 100,
        "error_count": 5,
        "conflict_count": 10,
    })
    return consumer


@pytest.fixture
def mock_updates_consumer():
    """Create a mock updates consumer."""
    consumer = MagicMock()
    consumer._running = True
    return consumer


@pytest.fixture
def mock_truth_layer():
    """Create a mock truth layer."""
    truth_layer = MagicMock()
    truth_layer._cache = {"entity_1": {}, "entity_2": {}, "entity_3": {}}
    return truth_layer


@pytest.fixture
def health_checker(mock_neo4j_client):
    """Create HealthChecker with mock dependencies."""
    checker = HealthChecker(
        neo4j_client=mock_neo4j_client,
    )
    return checker


@pytest.fixture
def full_health_checker(mock_neo4j_client, mock_claims_consumer, mock_updates_consumer, mock_truth_layer):
    """Create HealthChecker with all dependencies."""
    return HealthChecker(
        neo4j_client=mock_neo4j_client,
        claims_consumer=mock_claims_consumer,
        updates_consumer=mock_updates_consumer,
        truth_layer=mock_truth_layer,
    )


class TestHealthStatus:
    """Tests for HealthStatus enum."""

    def test_healthy_status(self):
        """Test healthy status value."""
        assert HealthStatus.HEALTHY.value == "healthy"

    def test_unhealthy_status(self):
        """Test unhealthy status value."""
        assert HealthStatus.UNHEALTHY.value == "unhealthy"

    def test_degraded_status(self):
        """Test degraded status value."""
        assert HealthStatus.DEGRADED.value == "degraded"

    def test_unknown_status(self):
        """Test unknown status value."""
        assert HealthStatus.UNKNOWN.value == "unknown"

    def test_health_status_is_string_enum(self):
        """Test that HealthStatus is a string enum."""
        assert isinstance(HealthStatus.HEALTHY, str)
        assert HealthStatus.HEALTHY == "healthy"

    def test_all_status_values(self):
        """Test all status values are unique."""
        values = [status.value for status in HealthStatus]
        assert len(values) == len(set(values))


class TestComponentHealth:
    """Tests for ComponentHealth dataclass."""

    def test_healthy_component(self):
        """Test creating healthy component health."""
        health = ComponentHealth(
            name="neo4j",
            status=HealthStatus.HEALTHY,
            latency_ms=15.5,
            message="Connection successful",
            last_checked=datetime.utcnow(),
        )

        assert health.name == "neo4j"
        assert health.status == HealthStatus.HEALTHY
        assert health.latency_ms == 15.5

    def test_unhealthy_component(self):
        """Test creating unhealthy component health."""
        health = ComponentHealth(
            name="kafka",
            status=HealthStatus.UNHEALTHY,
            latency_ms=None,
            message="Connection refused",
            last_checked=datetime.utcnow(),
            details={"error": "ConnectionError: Connection refused"},
        )

        assert health.status == HealthStatus.UNHEALTHY
        assert "error" in health.details

    def test_to_dict(self):
        """Test serialization to dictionary."""
        health = ComponentHealth(
            name="neo4j",
            status=HealthStatus.HEALTHY,
            latency_ms=10.0,
            message="OK",
            last_checked=datetime(2024, 1, 15, 12, 0, 0),
        )

        data = health.to_dict()

        assert data["name"] == "neo4j"
        assert data["status"] == "healthy"
        assert data["latency_ms"] == 10.0

    def test_to_dict_includes_all_fields(self):
        """Test that to_dict includes all fields."""
        health = ComponentHealth(
            name="test",
            status=HealthStatus.DEGRADED,
            latency_ms=50.0,
            message="High latency",
            last_checked=datetime(2024, 1, 15, 12, 0, 0),
            details={"warning": "slow response"},
        )

        data = health.to_dict()

        assert "name" in data
        assert "status" in data
        assert "latency_ms" in data
        assert "message" in data
        assert "last_checked" in data
        assert "details" in data
        assert data["details"]["warning"] == "slow response"

    def test_default_values(self):
        """Test default values for optional fields."""
        health = ComponentHealth(
            name="test",
            status=HealthStatus.UNKNOWN,
        )

        assert health.latency_ms is None
        assert health.message is None
        assert health.details == {}

    def test_last_checked_default(self):
        """Test that last_checked has default value."""
        before = datetime.utcnow()
        health = ComponentHealth(
            name="test",
            status=HealthStatus.HEALTHY,
        )
        after = datetime.utcnow()

        assert before <= health.last_checked <= after


class TestServiceHealth:
    """Tests for ServiceHealth dataclass."""

    def test_service_health_all_healthy(self):
        """Test service health when all components are healthy."""
        components = [
            ComponentHealth(name="neo4j", status=HealthStatus.HEALTHY, latency_ms=10.0, message="OK"),
            ComponentHealth(name="kafka", status=HealthStatus.HEALTHY, latency_ms=5.0, message="OK"),
        ]

        service_health = ServiceHealth(
            service_name="graph-updater",
            status=HealthStatus.HEALTHY,
            components=components,
            version="0.4.0",
            uptime_seconds=3600,
        )

        assert service_health.status == HealthStatus.HEALTHY
        assert len(service_health.components) == 2

    def test_service_health_degraded(self):
        """Test service health when some components are unhealthy."""
        components = [
            ComponentHealth(name="neo4j", status=HealthStatus.HEALTHY, latency_ms=10.0, message="OK"),
            ComponentHealth(name="kafka", status=HealthStatus.UNHEALTHY, latency_ms=None, message="Connection failed"),
        ]

        service_health = ServiceHealth(
            service_name="graph-updater",
            status=HealthStatus.DEGRADED,
            components=components,
            version="0.4.0",
            uptime_seconds=3600,
        )

        assert service_health.status == HealthStatus.DEGRADED

    def test_to_dict(self):
        """Test serialization to dictionary."""
        components = [
            ComponentHealth(name="neo4j", status=HealthStatus.HEALTHY, latency_ms=10.0, message="OK"),
        ]

        service_health = ServiceHealth(
            service_name="graph-updater",
            status=HealthStatus.HEALTHY,
            components=components,
            version="0.4.0",
            uptime_seconds=7200,
        )

        data = service_health.to_dict()

        assert data["service_name"] == "graph-updater"
        assert data["status"] == "healthy"
        assert "components" in data
        assert data["uptime_seconds"] == 7200

    def test_to_dict_includes_timestamp(self):
        """Test that to_dict includes timestamp."""
        service_health = ServiceHealth(
            status=HealthStatus.HEALTHY,
        )

        data = service_health.to_dict()

        assert "timestamp" in data

    def test_default_values(self):
        """Test default values for ServiceHealth."""
        service_health = ServiceHealth(
            status=HealthStatus.HEALTHY,
        )

        assert service_health.service_name == "graph-updater"
        assert service_health.version == "0.4.0"
        assert service_health.uptime_seconds == 0.0
        assert service_health.components == []


class TestHealthChecker:
    """Tests for HealthChecker class."""

    def test_health_checker_creation(self, mock_neo4j_client):
        """Test that HealthChecker can be created."""
        checker = HealthChecker(neo4j_client=mock_neo4j_client)
        assert checker is not None

    def test_health_checker_with_no_dependencies(self):
        """Test that HealthChecker can be created without dependencies."""
        checker = HealthChecker()
        assert checker is not None
        assert checker.neo4j_client is None
        assert checker.claims_consumer is None
        assert checker.updates_consumer is None
        assert checker.truth_layer is None

    def test_health_checker_stores_dependencies(self, mock_neo4j_client, mock_claims_consumer):
        """Test that HealthChecker stores dependencies."""
        checker = HealthChecker(
            neo4j_client=mock_neo4j_client,
            claims_consumer=mock_claims_consumer,
        )

        assert checker.neo4j_client == mock_neo4j_client
        assert checker.claims_consumer == mock_claims_consumer

    @pytest.mark.asyncio
    async def test_check_all_returns_service_health(self, full_health_checker):
        """Test that check_all returns ServiceHealth."""
        result = await full_health_checker.check_all()

        assert isinstance(result, ServiceHealth)
        assert len(result.components) == 4  # neo4j, claims, updates, truth

    @pytest.mark.asyncio
    async def test_check_all_calculates_uptime(self, full_health_checker):
        """Test that check_all calculates uptime correctly."""
        # Set start time to 1 hour ago
        full_health_checker._start_time = datetime.utcnow() - timedelta(hours=1)

        result = await full_health_checker.check_all()

        # Uptime should be approximately 3600 seconds
        assert result.uptime_seconds >= 3590  # Allow small margin

    @pytest.mark.asyncio
    async def test_check_liveness_returns_true(self, health_checker):
        """Test that check_liveness returns True."""
        result = await health_checker.check_liveness()
        assert result is True

    @pytest.mark.asyncio
    async def test_check_readiness_with_healthy_neo4j(self, health_checker, mock_neo4j_client):
        """Test check_readiness when Neo4j is healthy."""
        mock_neo4j_client.health_check = AsyncMock(return_value=True)

        result = await health_checker.check_readiness()

        assert result is True

    @pytest.mark.asyncio
    async def test_check_readiness_with_no_neo4j(self):
        """Test check_readiness when Neo4j client is None."""
        checker = HealthChecker(neo4j_client=None)

        result = await checker.check_readiness()

        assert result is False

    @pytest.mark.asyncio
    async def test_check_readiness_with_unhealthy_neo4j(self, health_checker, mock_neo4j_client):
        """Test check_readiness when Neo4j health check fails."""
        mock_neo4j_client.health_check = AsyncMock(return_value=False)

        result = await health_checker.check_readiness()

        assert result is False

    @pytest.mark.asyncio
    async def test_check_readiness_handles_exception(self, health_checker, mock_neo4j_client):
        """Test check_readiness handles exception gracefully."""
        mock_neo4j_client.health_check = AsyncMock(side_effect=Exception("Connection failed"))

        result = await health_checker.check_readiness()

        assert result is False

    @pytest.mark.asyncio
    async def test_check_neo4j_healthy(self, health_checker, mock_neo4j_client):
        """Test _check_neo4j when healthy."""
        mock_neo4j_client.health_check = AsyncMock(return_value=True)

        result = await health_checker._check_neo4j()

        assert result.name == "neo4j"
        assert result.status == HealthStatus.HEALTHY
        assert result.latency_ms is not None

    @pytest.mark.asyncio
    async def test_check_neo4j_unhealthy(self, health_checker, mock_neo4j_client):
        """Test _check_neo4j when unhealthy."""
        mock_neo4j_client.health_check = AsyncMock(return_value=False)

        result = await health_checker._check_neo4j()

        assert result.name == "neo4j"
        assert result.status == HealthStatus.UNHEALTHY

    @pytest.mark.asyncio
    async def test_check_neo4j_degraded_high_latency(self, health_checker, mock_neo4j_client):
        """Test _check_neo4j returns degraded for high latency."""
        import asyncio

        async def slow_health_check():
            await asyncio.sleep(0.15)  # 150ms
            return True

        mock_neo4j_client.health_check = slow_health_check

        result = await health_checker._check_neo4j()

        assert result.name == "neo4j"
        assert result.status == HealthStatus.DEGRADED
        assert "latency" in result.message.lower()

    @pytest.mark.asyncio
    async def test_check_neo4j_no_client(self):
        """Test _check_neo4j when client is None."""
        checker = HealthChecker(neo4j_client=None)

        result = await checker._check_neo4j()

        assert result.name == "neo4j"
        assert result.status == HealthStatus.UNHEALTHY
        assert "not initialized" in result.message.lower()

    @pytest.mark.asyncio
    async def test_check_neo4j_exception(self, health_checker, mock_neo4j_client):
        """Test _check_neo4j handles exception."""
        mock_neo4j_client.health_check = AsyncMock(side_effect=Exception("Connection error"))

        result = await health_checker._check_neo4j()

        assert result.status == HealthStatus.UNHEALTHY
        assert "Connection error" in result.message

    def test_check_claims_consumer_running(self, full_health_checker):
        """Test _check_claims_consumer when running."""
        result = full_health_checker._check_claims_consumer()

        assert result.name == "claims_consumer"
        assert result.status == HealthStatus.HEALTHY
        assert result.message == "Running"
        assert "processed_count" in result.details

    def test_check_claims_consumer_not_running(self, full_health_checker, mock_claims_consumer):
        """Test _check_claims_consumer when not running."""
        mock_claims_consumer._running = False

        result = full_health_checker._check_claims_consumer()

        assert result.status == HealthStatus.DEGRADED
        assert "not running" in result.message.lower()

    def test_check_claims_consumer_not_initialized(self):
        """Test _check_claims_consumer when consumer is None."""
        checker = HealthChecker(claims_consumer=None)

        result = checker._check_claims_consumer()

        assert result.status == HealthStatus.UNKNOWN
        assert "not initialized" in result.message.lower()

    def test_check_claims_consumer_no_stats(self):
        """Test _check_claims_consumer when get_stats not available."""
        consumer = MagicMock()
        consumer._running = True
        del consumer.get_stats

        checker = HealthChecker(claims_consumer=consumer)
        result = checker._check_claims_consumer()

        assert result.status == HealthStatus.HEALTHY

    def test_check_updates_consumer_running(self, full_health_checker):
        """Test _check_updates_consumer when running."""
        result = full_health_checker._check_updates_consumer()

        assert result.name == "updates_consumer"
        assert result.status == HealthStatus.HEALTHY
        assert result.message == "Running"

    def test_check_updates_consumer_not_running(self, full_health_checker, mock_updates_consumer):
        """Test _check_updates_consumer when not running."""
        mock_updates_consumer._running = False

        result = full_health_checker._check_updates_consumer()

        assert result.status == HealthStatus.DEGRADED

    def test_check_updates_consumer_not_initialized(self):
        """Test _check_updates_consumer when consumer is None."""
        checker = HealthChecker(updates_consumer=None)

        result = checker._check_updates_consumer()

        assert result.status == HealthStatus.UNKNOWN

    def test_check_truth_layer_healthy(self, full_health_checker):
        """Test _check_truth_layer when healthy."""
        result = full_health_checker._check_truth_layer()

        assert result.name == "truth_layer"
        assert result.status == HealthStatus.HEALTHY
        assert result.details["cache_size"] == 3

    def test_check_truth_layer_not_initialized(self):
        """Test _check_truth_layer when not initialized."""
        checker = HealthChecker(truth_layer=None)

        result = checker._check_truth_layer()

        assert result.status == HealthStatus.UNKNOWN

    def test_check_truth_layer_empty_cache(self):
        """Test _check_truth_layer with empty cache."""
        truth_layer = MagicMock()
        truth_layer._cache = {}
        checker = HealthChecker(truth_layer=truth_layer)

        result = checker._check_truth_layer()

        assert result.status == HealthStatus.HEALTHY
        assert result.details["cache_size"] == 0


class TestHealthCheckerAggregation:
    """Tests for health status aggregation."""

    def test_aggregate_status_all_healthy(self):
        """Test aggregation when all components are healthy."""
        checker = HealthChecker()
        components = [
            ComponentHealth(name="neo4j", status=HealthStatus.HEALTHY),
            ComponentHealth(name="kafka", status=HealthStatus.HEALTHY),
        ]

        result = checker._aggregate_status(components)

        assert result == HealthStatus.HEALTHY

    def test_aggregate_status_one_unhealthy_non_critical(self):
        """Test aggregation when non-critical component is unhealthy."""
        checker = HealthChecker()
        components = [
            ComponentHealth(name="neo4j", status=HealthStatus.HEALTHY),
            ComponentHealth(name="claims_consumer", status=HealthStatus.UNHEALTHY),
        ]

        result = checker._aggregate_status(components)

        assert result == HealthStatus.DEGRADED

    def test_aggregate_status_neo4j_unhealthy(self):
        """Test aggregation when neo4j (critical) is unhealthy."""
        checker = HealthChecker()
        components = [
            ComponentHealth(name="neo4j", status=HealthStatus.UNHEALTHY),
            ComponentHealth(name="kafka", status=HealthStatus.HEALTHY),
        ]

        result = checker._aggregate_status(components)

        assert result == HealthStatus.UNHEALTHY

    def test_aggregate_status_one_degraded(self):
        """Test aggregation when one component is degraded."""
        checker = HealthChecker()
        components = [
            ComponentHealth(name="neo4j", status=HealthStatus.HEALTHY),
            ComponentHealth(name="kafka", status=HealthStatus.DEGRADED),
        ]

        result = checker._aggregate_status(components)

        assert result == HealthStatus.DEGRADED

    def test_aggregate_status_unknown_with_healthy(self):
        """Test aggregation when unknown with healthy components."""
        checker = HealthChecker()
        components = [
            ComponentHealth(name="neo4j", status=HealthStatus.HEALTHY),
            ComponentHealth(name="optional", status=HealthStatus.UNKNOWN),
        ]

        result = checker._aggregate_status(components)

        assert result == HealthStatus.HEALTHY

    def test_aggregate_status_unknown_with_degraded(self):
        """Test aggregation when unknown with degraded components."""
        checker = HealthChecker()
        components = [
            ComponentHealth(name="neo4j", status=HealthStatus.DEGRADED),
            ComponentHealth(name="optional", status=HealthStatus.UNKNOWN),
        ]

        result = checker._aggregate_status(components)

        assert result == HealthStatus.DEGRADED

    def test_aggregate_status_empty_components(self):
        """Test aggregation with no components."""
        checker = HealthChecker()

        result = checker._aggregate_status([])

        assert result == HealthStatus.UNKNOWN

    def test_aggregate_status_all_unknown(self):
        """Test aggregation when all components are unknown."""
        checker = HealthChecker()
        components = [
            ComponentHealth(name="a", status=HealthStatus.UNKNOWN),
            ComponentHealth(name="b", status=HealthStatus.UNKNOWN),
        ]

        result = checker._aggregate_status(components)

        assert result == HealthStatus.HEALTHY  # No known unhealthy components


class TestMetricsCollector:
    """Tests for MetricsCollector class."""

    def test_metrics_collector_creation(self):
        """Test MetricsCollector can be created."""
        collector = MetricsCollector()
        assert collector is not None

    def test_initial_values(self):
        """Test initial metric values are zero."""
        collector = MetricsCollector()

        assert collector._claims_processed == 0
        assert collector._conflicts_detected == 0
        assert collector._conflicts_resolved == 0
        assert collector._errors == 0
        assert collector._cache_hits == 0
        assert collector._cache_misses == 0

    def test_increment_claims_processed(self):
        """Test incrementing claims processed."""
        collector = MetricsCollector()

        collector.increment_claims_processed()
        assert collector._claims_processed == 1

        collector.increment_claims_processed(5)
        assert collector._claims_processed == 6

    def test_increment_conflicts_detected(self):
        """Test incrementing conflicts detected."""
        collector = MetricsCollector()

        collector.increment_conflicts_detected()
        assert collector._conflicts_detected == 1

        collector.increment_conflicts_detected(3)
        assert collector._conflicts_detected == 4

    def test_increment_conflicts_resolved(self):
        """Test incrementing conflicts resolved."""
        collector = MetricsCollector()

        collector.increment_conflicts_resolved()
        assert collector._conflicts_resolved == 1

        collector.increment_conflicts_resolved(2)
        assert collector._conflicts_resolved == 3

    def test_increment_errors(self):
        """Test incrementing errors."""
        collector = MetricsCollector()

        collector.increment_errors()
        assert collector._errors == 1

        collector.increment_errors(10)
        assert collector._errors == 11

    def test_record_cache_hit(self):
        """Test recording cache hit."""
        collector = MetricsCollector()

        collector.record_cache_hit()
        assert collector._cache_hits == 1

        collector.record_cache_hit()
        assert collector._cache_hits == 2

    def test_record_cache_miss(self):
        """Test recording cache miss."""
        collector = MetricsCollector()

        collector.record_cache_miss()
        assert collector._cache_misses == 1

        collector.record_cache_miss()
        assert collector._cache_misses == 2

    def test_get_metrics(self):
        """Test getting metrics."""
        collector = MetricsCollector()

        collector.increment_claims_processed(10)
        collector.increment_conflicts_detected(2)
        collector.increment_conflicts_resolved(1)
        collector.increment_errors(3)
        collector.record_cache_hit()
        collector.record_cache_hit()
        collector.record_cache_miss()

        metrics = collector.get_metrics()

        assert metrics["claims_processed_total"] == 10
        assert metrics["conflicts_detected_total"] == 2
        assert metrics["conflicts_resolved_total"] == 1
        assert metrics["errors_total"] == 3
        assert metrics["cache_hits_total"] == 2
        assert metrics["cache_misses_total"] == 1
        assert "uptime_seconds" in metrics

    def test_get_metrics_cache_hit_rate(self):
        """Test cache hit rate calculation."""
        collector = MetricsCollector()

        collector.record_cache_hit()
        collector.record_cache_hit()
        collector.record_cache_hit()
        collector.record_cache_miss()

        metrics = collector.get_metrics()

        assert metrics["cache_hit_rate"] == 0.75

    def test_get_metrics_cache_hit_rate_zero_total(self):
        """Test cache hit rate when no cache operations."""
        collector = MetricsCollector()

        metrics = collector.get_metrics()

        assert metrics["cache_hit_rate"] == 0.0

    def test_get_metrics_uptime(self):
        """Test uptime calculation."""
        collector = MetricsCollector()
        collector._start_time = datetime.utcnow() - timedelta(seconds=100)

        metrics = collector.get_metrics()

        assert metrics["uptime_seconds"] >= 99  # Allow small margin

    def test_to_prometheus_format(self):
        """Test Prometheus format export."""
        collector = MetricsCollector()

        collector.increment_claims_processed(50)
        collector.increment_conflicts_detected(5)

        prometheus_output = collector.to_prometheus_format()

        assert "docweave_graph_updater_claims_processed_total 50" in prometheus_output
        assert "docweave_graph_updater_conflicts_detected_total 5" in prometheus_output
        assert "# HELP" in prometheus_output
        assert "# TYPE" in prometheus_output

    def test_to_prometheus_format_contains_all_metrics(self):
        """Test that Prometheus format contains all metrics."""
        collector = MetricsCollector()
        prometheus_output = collector.to_prometheus_format()

        expected_metrics = [
            "uptime_seconds",
            "claims_processed_total",
            "conflicts_detected_total",
            "conflicts_resolved_total",
            "errors_total",
            "cache_hit_rate",
        ]

        for metric in expected_metrics:
            assert f"docweave_graph_updater_{metric}" in prometheus_output

    def test_to_prometheus_format_types(self):
        """Test that Prometheus format includes correct types."""
        collector = MetricsCollector()
        prometheus_output = collector.to_prometheus_format()

        # Gauges
        assert "# TYPE docweave_graph_updater_uptime_seconds gauge" in prometheus_output
        assert "# TYPE docweave_graph_updater_cache_hit_rate gauge" in prometheus_output

        # Counters
        assert "# TYPE docweave_graph_updater_claims_processed_total counter" in prometheus_output
        assert "# TYPE docweave_graph_updater_errors_total counter" in prometheus_output


class TestMetricsCollectorEdgeCases:
    """Edge case tests for MetricsCollector."""

    def test_increment_with_zero(self):
        """Test incrementing with zero."""
        collector = MetricsCollector()

        collector.increment_claims_processed(0)
        assert collector._claims_processed == 0

    def test_increment_with_large_number(self):
        """Test incrementing with large number."""
        collector = MetricsCollector()

        collector.increment_claims_processed(1000000)
        assert collector._claims_processed == 1000000

    def test_multiple_metric_updates(self):
        """Test multiple metric updates."""
        collector = MetricsCollector()

        for _ in range(100):
            collector.increment_claims_processed()
            collector.record_cache_hit()

        for _ in range(50):
            collector.record_cache_miss()
            collector.increment_errors()

        metrics = collector.get_metrics()

        assert metrics["claims_processed_total"] == 100
        assert metrics["cache_hits_total"] == 100
        assert metrics["cache_misses_total"] == 50
        assert metrics["errors_total"] == 50
        assert metrics["cache_hit_rate"] == pytest.approx(0.6667, rel=0.01)
