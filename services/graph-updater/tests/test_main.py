"""
Integration Tests for Graph Updater FastAPI Application
========================================================

Tests all endpoints using TestClient with mocked Neo4j client and Kafka consumers.

Run from project root:
    PYTHONPATH=. pytest services/graph-updater/tests/test_main.py -v
"""

import sys
from pathlib import Path
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

# Setup path for imports - add project root
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(Path(__file__).parent))

# Mock shared modules before importing main
mock_claim_status = MagicMock()
mock_claim_status.CURRENT = MagicMock()
mock_claim_status.CURRENT.value = "current"
mock_claim_status.SUPERSEDED = MagicMock()
mock_claim_status.SUPERSEDED.value = "superseded"
mock_claim_status.CONFLICTING = MagicMock()
mock_claim_status.CONFLICTING.value = "conflicting"

# Setup mock modules
sys.modules['shared'] = MagicMock()
sys.modules['shared.config'] = MagicMock()
sys.modules['shared.config.settings'] = MagicMock()
sys.modules['shared.config.settings'].settings = MagicMock()
sys.modules['shared.config.settings'].get_settings = MagicMock()
sys.modules['shared.models'] = MagicMock()
sys.modules['shared.models.claim'] = MagicMock()
sys.modules['shared.models.claim'].ClaimStatus = mock_claim_status
sys.modules['shared.utils'] = MagicMock()
sys.modules['shared.utils.neo4j_client'] = MagicMock()
sys.modules['shared.utils.neo4j_client'].Neo4jClient = MagicMock()
sys.modules['shared.utils.kafka_client'] = MagicMock()
sys.modules['shared.utils.kafka_client'].KafkaConsumer = MagicMock()
sys.modules['shared.utils.kafka_client'].KafkaProducer = MagicMock()
sys.modules['shared.utils.kafka_client'].KafkaTopics = MagicMock()

from fastapi.testclient import TestClient


# =============================================================================
# Mock Classes (imported from conftest pattern)
# =============================================================================

class MockQueryResult:
    """Mock Neo4j query result."""
    def __init__(self, records=None):
        self.records = records or []


class MockNeo4jClient:
    """Mock Neo4j client for testing."""
    def __init__(self):
        self.execute_query = AsyncMock(return_value=MockQueryResult([]))
        self._query_history = []

    async def init(self):
        pass

    async def close(self):
        pass

    async def health_check(self) -> bool:
        return True

    def set_query_response(self, records):
        """Set the response for the next query."""
        self.execute_query.return_value = MockQueryResult(records)

    def set_query_responses(self, responses):
        """Set multiple responses for sequential queries."""
        self.execute_query.side_effect = [
            MockQueryResult(records) for records in responses
        ]


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def mock_neo4j():
    """Create a mock Neo4j client."""
    return MockNeo4jClient()


@pytest.fixture
def mock_claims_consumer():
    """Create a mock claims consumer."""
    consumer = MagicMock()
    consumer._running = True
    consumer.get_stats = MagicMock(return_value={
        "running": True,
        "group_id": "test-group",
        "processed_count": 100,
        "error_count": 2,
        "conflict_count": 5,
    })
    consumer.start = AsyncMock()
    consumer.stop = AsyncMock()
    return consumer


@pytest.fixture
def mock_updates_consumer():
    """Create a mock updates consumer."""
    consumer = MagicMock()
    consumer._running = True
    consumer.start = AsyncMock()
    consumer.stop = AsyncMock()
    return consumer


@pytest.fixture
def mock_graph_ops(mock_neo4j):
    """Create mock graph operations."""
    ops = MagicMock()
    ops.client = mock_neo4j
    ops.merge_entity = AsyncMock(return_value="ent_test123")
    ops.merge_entities = AsyncMock()
    ops.add_claim = AsyncMock(return_value="claim_test123")
    ops.add_claim_versioned = AsyncMock(return_value=("claim_test123", []))
    ops.add_claim_with_conflict_check = AsyncMock(return_value=("claim_test123", []))
    ops.get_claim_history = AsyncMock(return_value=[])
    ops.get_claim_versions = AsyncMock(return_value=[])
    ops.get_unresolved_conflicts = AsyncMock(return_value=[])
    return ops


@pytest.fixture
def mock_conflict_detector():
    """Create mock conflict detector."""
    detector = MagicMock()
    detector.detect_conflicts_for_claim = AsyncMock(return_value=[])
    detector.detect_all_conflicts = AsyncMock(return_value=[])
    return detector


@pytest.fixture
def mock_conflict_resolver():
    """Create mock conflict resolver."""
    resolver = MagicMock()
    resolver.resolve_conflict = AsyncMock()
    resolver.resolve_by_manual_selection = AsyncMock()
    resolver.get_audit_trail = AsyncMock(return_value=[])
    return resolver


@pytest.fixture
def mock_truth_layer():
    """Create mock truth layer."""
    layer = MagicMock()
    layer.get_entity_truth = AsyncMock(return_value=None)
    layer.get_truth_at_time = AsyncMock(return_value=None)
    layer.refresh_truth = AsyncMock(return_value=None)
    layer.materialize_all_truths = AsyncMock(return_value=100)
    layer.invalidate_cache = MagicMock()
    return layer


@pytest.fixture
def client_with_mocks(
    mock_neo4j,
    mock_graph_ops,
    mock_conflict_detector,
    mock_conflict_resolver,
    mock_truth_layer,
    mock_claims_consumer,
    mock_updates_consumer,
):
    """Create test client with all mocked dependencies."""
    from contextlib import asynccontextmanager
    from fastapi import FastAPI

    # Import main module
    from services.graph_updater import main as main_module

    # Store original values
    originals = {
        'neo4j_client': getattr(main_module, 'neo4j_client', None),
        'graph_ops': getattr(main_module, 'graph_ops', None),
        'conflict_detector': getattr(main_module, 'conflict_detector', None),
        'conflict_resolver': getattr(main_module, 'conflict_resolver', None),
        'truth_layer': getattr(main_module, 'truth_layer', None),
        'claims_consumer': getattr(main_module, 'claims_consumer', None),
        'updates_consumer': getattr(main_module, 'updates_consumer', None),
    }

    # Set mocked values BEFORE creating client
    main_module.neo4j_client = mock_neo4j
    main_module.graph_ops = mock_graph_ops
    main_module.conflict_detector = mock_conflict_detector
    main_module.conflict_resolver = mock_conflict_resolver
    main_module.truth_layer = mock_truth_layer
    main_module.claims_consumer = mock_claims_consumer
    main_module.updates_consumer = mock_updates_consumer

    # Create a test-only FastAPI app that doesn't use the lifespan
    # We copy the routes from the original app
    from fastapi import APIRouter

    # Create a new app without lifespan for testing
    test_app = FastAPI(
        title="Test Graph Updater",
        version="0.4.0",
    )

    # Copy all routes from the original app
    for route in main_module.app.routes:
        test_app.routes.append(route)

    # Create client with the test app
    with TestClient(test_app, raise_server_exceptions=False) as client:
        yield client, {
            "neo4j": mock_neo4j,
            "graph_ops": mock_graph_ops,
            "conflict_detector": mock_conflict_detector,
            "conflict_resolver": mock_conflict_resolver,
            "truth_layer": mock_truth_layer,
            "claims_consumer": mock_claims_consumer,
            "updates_consumer": mock_updates_consumer,
        }

    # Restore original values
    for key, value in originals.items():
        setattr(main_module, key, value)


# =============================================================================
# Health Endpoint Tests
# =============================================================================

class TestHealthEndpoints:
    """Tests for health check endpoints."""

    def test_health_check_returns_healthy_status(self, client_with_mocks):
        """GET /health returns healthy status with service info."""
        client, _ = client_with_mocks

        response = client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "graph-updater"
        assert data["version"] == "0.4.0"
        assert "timestamp" in data

    def test_readiness_check_returns_component_status(self, client_with_mocks):
        """GET /ready returns readiness status with component checks."""
        client, mocks = client_with_mocks

        # Mock neo4j health check
        mocks["neo4j"].health_check = AsyncMock(return_value=True)

        response = client.get("/ready")

        assert response.status_code == 200
        data = response.json()
        assert "ready" in data
        assert "checks" in data
        assert "timestamp" in data
        # Check that component checks are present
        assert "neo4j" in data["checks"]
        assert "kafka_consumer" in data["checks"]
        assert "graph_operations" in data["checks"]
        assert "conflict_detector" in data["checks"]
        assert "truth_layer" in data["checks"]


# =============================================================================
# Entity Endpoint Tests
# =============================================================================

class TestEntityEndpoints:
    """Tests for entity management endpoints."""

    def test_create_entity_success(self, client_with_mocks):
        """POST /entity creates a new entity."""
        client, mocks = client_with_mocks
        mocks["graph_ops"].merge_entity = AsyncMock(return_value="ent_abc123")

        response = client.post(
            "/entity",
            params={
                "name": "Acme Corp",
                "entity_type": "ORGANIZATION",
                "aliases": ["ACME", "Acme Inc"],
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["entity_id"] == "ent_abc123"
        assert data["name"] == "Acme Corp"
        assert data["type"] == "ORGANIZATION"
        assert "ACME" in data["aliases"]

    def test_create_entity_without_aliases(self, client_with_mocks):
        """POST /entity works without aliases."""
        client, mocks = client_with_mocks
        mocks["graph_ops"].merge_entity = AsyncMock(return_value="ent_xyz789")

        response = client.post(
            "/entity",
            params={
                "name": "Simple Entity",
                "entity_type": "PERSON",
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["entity_id"] == "ent_xyz789"
        assert data["aliases"] == []

    def test_merge_entities_success(self, client_with_mocks):
        """POST /entity/merge merges multiple entities."""
        client, mocks = client_with_mocks

        # Import for the result model
        from services.graph_updater.operations import EntityMergeResult
        mocks["graph_ops"].merge_entities = AsyncMock(return_value=EntityMergeResult(
            target_entity_id="ent_target",
            merged_entity_ids=["ent_src1", "ent_src2"],
            aliases_added=["Alias1", "Alias2"],
            claims_transferred=5,
            relationships_updated=2,
            success=True,
            message="Successfully merged 2 entities",
        ))

        response = client.post(
            "/entity/merge",
            json={
                "target_entity_id": "ent_target",
                "source_entity_ids": ["ent_src1", "ent_src2"],
                "merge_aliases": True,
                "merge_claims": True,
                "resolved_by": "test_user",
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["target_entity_id"] == "ent_target"
        assert data["merged_entity_ids"] == ["ent_src1", "ent_src2"]
        assert data["claims_transferred"] == 5
        assert data["success"] is True

    def test_merge_entities_failure_returns_400(self, client_with_mocks):
        """POST /entity/merge returns 400 on failure."""
        client, mocks = client_with_mocks

        from services.graph_updater.operations import EntityMergeResult
        mocks["graph_ops"].merge_entities = AsyncMock(return_value=EntityMergeResult(
            target_entity_id="ent_target",
            merged_entity_ids=[],
            aliases_added=[],
            claims_transferred=0,
            relationships_updated=0,
            success=False,
            message="Target entity not found",
        ))

        response = client.post(
            "/entity/merge",
            json={
                "target_entity_id": "ent_nonexistent",
                "source_entity_ids": ["ent_src1"],
            }
        )

        assert response.status_code == 400
        data = response.json()
        assert "detail" in data


# =============================================================================
# Truth Endpoint Tests
# =============================================================================

class TestTruthEndpoints:
    """Tests for truth layer endpoints."""

    def test_get_entity_truth_success(self, client_with_mocks):
        """GET /entity/{id}/truth returns entity truth."""
        client, mocks = client_with_mocks

        from services.graph_updater.truth_layer import EntityTruth, TruthValue
        mocks["truth_layer"].get_entity_truth = AsyncMock(return_value=EntityTruth(
            entity_id="ent_test",
            entity_name="Test Entity",
            entity_type="ORGANIZATION",
            truths={
                "ceo": TruthValue(
                    entity_id="ent_test",
                    predicate="ceo",
                    value="John Doe",
                    confidence=0.95,
                    supporting_claim_ids=["claim_1"],
                    source_count=2,
                ),
                "employee_count": TruthValue(
                    entity_id="ent_test",
                    predicate="employee_count",
                    value="1500",
                    confidence=0.88,
                    supporting_claim_ids=["claim_2"],
                    source_count=1,
                ),
            },
            has_conflicts=False,
            conflict_count=0,
        ))

        response = client.get("/entity/ent_test/truth")

        assert response.status_code == 200
        data = response.json()
        assert data["entity_id"] == "ent_test"
        assert data["entity_name"] == "Test Entity"
        assert "ceo" in data["truths"]
        assert data["truths"]["ceo"]["value"] == "John Doe"
        assert data["has_conflicts"] is False

    def test_get_entity_truth_not_found(self, client_with_mocks):
        """GET /entity/{id}/truth returns 404 for unknown entity."""
        client, mocks = client_with_mocks
        mocks["truth_layer"].get_entity_truth = AsyncMock(return_value=None)

        response = client.get("/entity/ent_nonexistent/truth")

        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"].lower()

    def test_get_entity_history_success(self, client_with_mocks):
        """GET /entity/{id}/history returns claim history."""
        client, mocks = client_with_mocks

        mocks["graph_ops"].get_claim_history = AsyncMock(return_value=[
            {
                "claim_id": "claim_1",
                "value": "1500",
                "status": "current",
                "confidence": 0.95,
                "created_at": "2024-01-15T00:00:00",
            },
            {
                "claim_id": "claim_2",
                "value": "1200",
                "status": "superseded",
                "confidence": 0.85,
                "created_at": "2023-06-01T00:00:00",
            },
        ])

        from services.graph_updater.operations import ClaimVersionInfo
        mocks["graph_ops"].get_claim_versions = AsyncMock(return_value=[
            ClaimVersionInfo(
                claim_id="claim_1",
                version=2,
                status="current",
                object_value="1500",
                confidence=0.95,
                created_at="2024-01-15T00:00:00",
            ),
            ClaimVersionInfo(
                claim_id="claim_2",
                version=1,
                status="superseded",
                object_value="1200",
                confidence=0.85,
                created_at="2023-06-01T00:00:00",
                superseded_by="claim_1",
            ),
        ])

        response = client.get(
            "/entity/ent_test/history",
            params={"predicate": "employee_count", "include_superseded": True}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["entity_id"] == "ent_test"
        assert data["predicate"] == "employee_count"
        assert len(data["history"]) == 2
        assert len(data["versions"]) == 2

    def test_get_truth_at_time_success(self, client_with_mocks):
        """GET /entity/{id}/truth/at returns historical truth."""
        client, mocks = client_with_mocks

        from services.graph_updater.truth_layer import HistoricalTruth
        mocks["truth_layer"].get_truth_at_time = AsyncMock(return_value=HistoricalTruth(
            entity_id="ent_test",
            predicate="employee_count",
            value="1200",
            as_of=datetime(2023, 6, 1),
            claim_id="claim_old",
            source_id="src_old_report",
            confidence=0.85,
        ))

        response = client.get(
            "/entity/ent_test/truth/at",
            params={
                "predicate": "employee_count",
                "as_of": "2023-06-01T00:00:00",
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["entity_id"] == "ent_test"
        assert data["value"] == "1200"
        assert data["claim_id"] == "claim_old"

    def test_get_truth_at_time_not_found(self, client_with_mocks):
        """GET /entity/{id}/truth/at returns 404 when no truth at time."""
        client, mocks = client_with_mocks
        mocks["truth_layer"].get_truth_at_time = AsyncMock(return_value=None)

        response = client.get(
            "/entity/ent_test/truth/at",
            params={
                "predicate": "nonexistent_predicate",
                "as_of": "2020-01-01T00:00:00",
            }
        )

        assert response.status_code == 404


# =============================================================================
# Claim Endpoint Tests
# =============================================================================

class TestClaimEndpoints:
    """Tests for claim management endpoints."""

    def test_add_claim_simple_success(self, client_with_mocks):
        """POST /claim adds a claim without conflict checking."""
        client, mocks = client_with_mocks
        mocks["graph_ops"].add_claim = AsyncMock(return_value="claim_new123")

        response = client.post(
            "/claim",
            json={
                "subject_entity_id": "ent_test",
                "predicate": "employee_count",
                "object_value": "1500",
                "source_id": "src_report_2024",
                "confidence": 0.9,
                "check_conflicts": False,
                "auto_supersede": False,
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["claim_id"] == "claim_new123"
        assert data["status"] == "added"
        assert data["conflicts_detected"] == 0

    def test_add_claim_with_conflict_check(self, client_with_mocks):
        """POST /claim detects conflicts when enabled."""
        client, mocks = client_with_mocks

        mocks["graph_ops"].add_claim_with_conflict_check = AsyncMock(return_value=(
            "claim_conflict123",
            [
                {
                    "id": "conflict_1",
                    "conflict_type": "VALUE_MISMATCH",
                    "existing_claim_id": "claim_old",
                    "existing_value": "1200",
                },
            ]
        ))

        response = client.post(
            "/claim",
            json={
                "subject_entity_id": "ent_test",
                "predicate": "employee_count",
                "object_value": "1500",
                "source_id": "src_report_2024",
                "confidence": 0.9,
                "check_conflicts": True,
                "auto_supersede": False,
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["claim_id"] == "claim_conflict123"
        assert data["status"] == "conflicting"
        assert data["conflicts_detected"] == 1

    def test_add_claim_with_auto_supersede(self, client_with_mocks):
        """POST /claim supersedes existing claims when enabled."""
        client, mocks = client_with_mocks

        mocks["graph_ops"].add_claim_versioned = AsyncMock(return_value=(
            "claim_new_version",
            ["claim_old_1", "claim_old_2"]
        ))

        response = client.post(
            "/claim",
            json={
                "subject_entity_id": "ent_test",
                "predicate": "ceo",
                "object_value": "Jane Smith",
                "source_id": "src_press_release",
                "confidence": 0.95,
                "auto_supersede": True,
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["claim_id"] == "claim_new_version"
        assert data["status"] == "added_with_supersede"
        assert len(data["superseded_claims"]) == 2

    def test_get_claim_success(self, client_with_mocks):
        """GET /claim/{id} returns claim details."""
        client, mocks = client_with_mocks

        mocks["neo4j"].set_query_response([{
            "c": {
                "id": "claim_test",
                "predicate": "employee_count",
                "object_value": "1500",
                "confidence": 0.9,
                "status": "current",
            },
            "entity_id": "ent_test",
            "entity_name": "Test Entity",
            "source_id": "src_report",
            "source_uri": "https://example.com/report.pdf",
            "supersedes_id": None,
            "superseded_by": None,
        }])

        response = client.get("/claim/claim_test")

        assert response.status_code == 200
        data = response.json()
        assert data["entity_id"] == "ent_test"

    def test_get_claim_not_found(self, client_with_mocks):
        """GET /claim/{id} returns 404 for unknown claim."""
        client, mocks = client_with_mocks
        mocks["neo4j"].set_query_response([])

        response = client.get("/claim/claim_nonexistent")

        assert response.status_code == 404


# =============================================================================
# Conflict Endpoint Tests
# =============================================================================

class TestConflictEndpoints:
    """Tests for conflict management endpoints."""

    def test_list_conflicts_success(self, client_with_mocks):
        """GET /conflicts returns unresolved conflicts."""
        client, mocks = client_with_mocks

        mocks["graph_ops"].get_unresolved_conflicts = AsyncMock(return_value=[
            {
                "conflict_id": "conflict_1",
                "conflict_type": "VALUE_MISMATCH",
                "claim1_id": "claim_a",
                "claim2_id": "claim_b",
                "predicate": "employee_count",
                "value1": "1200",
                "value2": "1500",
            },
            {
                "conflict_id": "conflict_2",
                "conflict_type": "TEMPORAL",
                "claim1_id": "claim_c",
                "claim2_id": "claim_d",
                "predicate": "ceo",
                "value1": "John",
                "value2": "Jane",
            },
        ])

        response = client.get("/conflicts")

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 2
        assert len(data["conflicts"]) == 2
        assert data["has_more"] is False

    def test_list_conflicts_with_filters(self, client_with_mocks):
        """GET /conflicts supports filtering by entity and type."""
        client, mocks = client_with_mocks

        mocks["graph_ops"].get_unresolved_conflicts = AsyncMock(return_value=[
            {
                "conflict_id": "conflict_1",
                "conflict_type": "value_mismatch",
                "entity_id": "ent_filter",
            },
        ])

        response = client.get(
            "/conflicts",
            params={
                "entity_id": "ent_filter",
                "conflict_type": "value_mismatch",
                "limit": 50,
                "offset": 0,
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1

    def test_get_conflict_success(self, client_with_mocks):
        """GET /conflicts/{id} returns conflict details."""
        client, mocks = client_with_mocks

        mocks["neo4j"].set_query_response([{
            "conflict_id": "conflict_test",
            "status": "UNRESOLVED",
            "conflict_type": "VALUE_MISMATCH",
            "detected_at": "2024-01-15T00:00:00",
            "claim1_id": "claim_a",
            "predicate": "employee_count",
            "value1": "1200",
            "confidence1": 0.85,
            "claim2_id": "claim_b",
            "value2": "1500",
            "confidence2": 0.95,
            "entity_id": "ent_test",
            "entity_name": "Test Entity",
            "reliability1": 0.8,
            "reliability2": 0.9,
        }])

        response = client.get("/conflicts/conflict_test")

        assert response.status_code == 200
        data = response.json()
        assert data["conflict_id"] == "conflict_test"
        assert data["conflict_type"] == "VALUE_MISMATCH"

    def test_get_conflict_not_found(self, client_with_mocks):
        """GET /conflicts/{id} returns 404 for unknown conflict."""
        client, mocks = client_with_mocks
        mocks["neo4j"].set_query_response([])

        response = client.get("/conflicts/conflict_nonexistent")

        assert response.status_code == 404

    def test_resolve_conflict_auto_success(self, client_with_mocks):
        """POST /conflicts/{id}/resolve resolves using automatic strategy."""
        client, mocks = client_with_mocks

        # Mock conflict data query
        mocks["neo4j"].set_query_response([
            {
                "id": "claim_a",
                "object_value": "1200",
                "confidence": 0.85,
                "timestamp": "2023-01-01T00:00:00",
                "source_reliability": 0.8,
                "entity_id": "ent_test",
            },
            {
                "id": "claim_b",
                "object_value": "1500",
                "confidence": 0.95,
                "timestamp": "2024-01-01T00:00:00",
                "source_reliability": 0.9,
                "entity_id": "ent_test",
            },
        ])

        from services.graph_updater.conflict_resolver import (
            ResolutionResult, ResolutionOutcome, ResolutionStrategy, ResolutionAuditEntry
        )
        mocks["conflict_resolver"].resolve_conflict = AsyncMock(return_value=ResolutionResult(
            conflict_id="conflict_test",
            outcome=ResolutionOutcome.WINNER_SELECTED,
            winning_claim_id="claim_b",
            losing_claim_ids=["claim_a"],
            strategy_used=ResolutionStrategy.CONFIDENCE_BASED,
            reason="Higher confidence claim selected",
            confidence=0.9,
            audit_entry=ResolutionAuditEntry(
                id="audit_1",
                conflict_id="conflict_test",
                timestamp=datetime.utcnow(),
                strategy_used=ResolutionStrategy.CONFIDENCE_BASED,
                outcome=ResolutionOutcome.WINNER_SELECTED,
                winning_claim_id="claim_b",
                losing_claim_ids=["claim_a"],
                resolved_by="api_user",
                reason="Higher confidence claim selected",
                confidence=0.9,
                scores={"claim_a": 0.85, "claim_b": 0.95},
            ),
            actions_taken=["Marked claim_a as superseded", "Resolved conflict"],
        ))

        response = client.post(
            "/conflicts/conflict_test/resolve",
            json={
                "strategy": "confidence_based",
                "resolved_by": "test_user",
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["conflict_id"] == "conflict_test"
        assert data["outcome"] == "winner_selected"
        assert data["winning_claim_id"] == "claim_b"
        assert data["strategy_used"] == "confidence_based"

    def test_resolve_conflict_manual_success(self, client_with_mocks):
        """POST /conflicts/{id}/resolve resolves with manual selection."""
        client, mocks = client_with_mocks

        # Mock the conflict data query (required before resolution)
        mocks["neo4j"].set_query_response([
            {
                "id": "claim_a",
                "object_value": "1200",
                "confidence": 0.85,
                "timestamp": "2023-01-01T00:00:00",
                "source_reliability": 0.8,
                "entity_id": "ent_test",
            },
            {
                "id": "claim_b",
                "object_value": "1500",
                "confidence": 0.95,
                "timestamp": "2024-01-01T00:00:00",
                "source_reliability": 0.9,
                "entity_id": "ent_test",
            },
        ])

        from services.graph_updater.conflict_resolver import (
            ResolutionResult, ResolutionOutcome, ResolutionStrategy, ResolutionAuditEntry
        )
        mocks["conflict_resolver"].resolve_by_manual_selection = AsyncMock(return_value=ResolutionResult(
            conflict_id="conflict_test",
            outcome=ResolutionOutcome.WINNER_SELECTED,
            winning_claim_id="claim_a",
            losing_claim_ids=["claim_b"],
            strategy_used=ResolutionStrategy.MANUAL_REVIEW,
            reason="Domain expert confirmed older value is correct",
            confidence=1.0,
            audit_entry=ResolutionAuditEntry(
                id="audit_2",
                conflict_id="conflict_test",
                timestamp=datetime.utcnow(),
                strategy_used=ResolutionStrategy.MANUAL_REVIEW,
                outcome=ResolutionOutcome.WINNER_SELECTED,
                winning_claim_id="claim_a",
                losing_claim_ids=["claim_b"],
                resolved_by="domain_expert",
                reason="Domain expert confirmed older value is correct",
                confidence=1.0,
                scores={"claim_a": 1.0},
            ),
            actions_taken=["Manual resolution applied"],
        ))

        response = client.post(
            "/conflicts/conflict_test/resolve",
            json={
                "winning_claim_id": "claim_a",
                "reason": "Domain expert confirmed older value is correct",
                "resolved_by": "domain_expert",
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["winning_claim_id"] == "claim_a"
        assert data["strategy_used"] == "manual_review"

    def test_get_conflict_audit_success(self, client_with_mocks):
        """GET /conflicts/{id}/audit returns audit trail."""
        client, mocks = client_with_mocks

        from services.graph_updater.conflict_resolver import (
            ResolutionAuditEntry, ResolutionStrategy, ResolutionOutcome
        )
        mocks["conflict_resolver"].get_audit_trail = AsyncMock(return_value=[
            ResolutionAuditEntry(
                id="audit_1",
                conflict_id="conflict_test",
                timestamp=datetime(2024, 1, 15, 12, 0, 0),
                strategy_used=ResolutionStrategy.CONFIDENCE_BASED,
                outcome=ResolutionOutcome.WINNER_SELECTED,
                winning_claim_id="claim_b",
                losing_claim_ids=["claim_a"],
                resolved_by="system",
                reason="Auto-resolved: higher confidence",
                confidence=0.9,
                scores={"claim_a": 0.85, "claim_b": 0.95},
            ),
        ])

        response = client.get("/conflicts/conflict_test/audit")

        assert response.status_code == 200
        data = response.json()
        assert data["conflict_id"] == "conflict_test"
        assert len(data["entries"]) == 1
        assert data["total_entries"] == 1


# =============================================================================
# Detect/Scan Conflict Endpoint Tests
# =============================================================================

class TestDetectConflictEndpoints:
    """Tests for conflict detection endpoints."""

    def test_detect_conflicts_for_claim(self, client_with_mocks):
        """POST /detect-conflicts detects potential conflicts."""
        client, mocks = client_with_mocks

        from services.graph_updater.conflict_detector import (
            DetectedConflict, ConflictType, ConflictSeverity
        )
        mocks["conflict_detector"].detect_conflicts_for_claim = AsyncMock(return_value=[
            DetectedConflict(
                id="conflict_potential_1",
                conflict_type=ConflictType.VALUE_MISMATCH,
                claim_ids=["claim_existing"],
                subject_entity_id="ent_test",
                predicate="employee_count",
                values=["1200", "1500"],
                confidences=[0.85, 0.9],
                source_ids=["src_1"],
                severity=ConflictSeverity.MEDIUM,
                score=0.8,
            ),
        ])

        response = client.post(
            "/detect-conflicts",
            params={
                "subject_entity_id": "ent_test",
                "predicate": "employee_count",
                "object_value": "1500",
                "confidence": 0.9,
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["conflicts_detected"] == 1
        assert len(data["conflicts"]) == 1
        assert data["conflicts"][0]["conflict_type"] == "value_mismatch"

    def test_detect_conflicts_no_conflicts(self, client_with_mocks):
        """POST /detect-conflicts returns empty when no conflicts."""
        client, mocks = client_with_mocks
        mocks["conflict_detector"].detect_conflicts_for_claim = AsyncMock(return_value=[])

        response = client.post(
            "/detect-conflicts",
            params={
                "subject_entity_id": "ent_new",
                "predicate": "new_attribute",
                "object_value": "value",
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["conflicts_detected"] == 0
        assert data["conflicts"] == []

    def test_scan_all_conflicts(self, client_with_mocks):
        """GET /scan-conflicts scans graph for all conflicts."""
        client, mocks = client_with_mocks

        from services.graph_updater.conflict_detector import (
            DetectedConflict, ConflictType, ConflictSeverity
        )
        mocks["conflict_detector"].detect_all_conflicts = AsyncMock(return_value=[
            DetectedConflict(
                id="scan_conflict_1",
                conflict_type=ConflictType.VALUE_MISMATCH,
                claim_ids=["claim_1", "claim_2"],
                subject_entity_id="ent_a",
                predicate="revenue",
                values=["$10M", "$12M"],
                severity=ConflictSeverity.HIGH,
            ),
            DetectedConflict(
                id="scan_conflict_2",
                conflict_type=ConflictType.TEMPORAL,
                claim_ids=["claim_3", "claim_4"],
                subject_entity_id="ent_b",
                predicate="ceo",
                values=["Alice", "Bob"],
                severity=ConflictSeverity.HIGH,
            ),
        ])

        response = client.get("/scan-conflicts")

        assert response.status_code == 200
        data = response.json()
        assert data["conflicts_found"] == 2
        assert len(data["conflicts"]) == 2

    def test_scan_conflicts_with_filters(self, client_with_mocks):
        """GET /scan-conflicts supports entity and predicate filters."""
        client, mocks = client_with_mocks

        from services.graph_updater.conflict_detector import (
            DetectedConflict, ConflictType
        )
        mocks["conflict_detector"].detect_all_conflicts = AsyncMock(return_value=[
            DetectedConflict(
                id="filtered_conflict",
                conflict_type=ConflictType.VALUE_MISMATCH,
                claim_ids=["claim_x", "claim_y"],
                subject_entity_id="ent_filter",
                predicate="revenue",
                values=["$10M", "$12M"],
            ),
        ])

        response = client.get(
            "/scan-conflicts",
            params={
                "entity_id": "ent_filter",
                "predicate": "revenue",
                "limit": 50,
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["conflicts_found"] == 1


# =============================================================================
# Admin Endpoint Tests
# =============================================================================

class TestAdminEndpoints:
    """Tests for admin management endpoints."""

    def test_get_consumer_stats(self, client_with_mocks):
        """GET /admin/consumer/stats returns consumer statistics."""
        client, mocks = client_with_mocks

        response = client.get("/admin/consumer/stats")

        assert response.status_code == 200
        data = response.json()
        assert "claims_consumer" in data
        assert data["claims_consumer"]["running"] is True
        assert data["claims_consumer"]["processed_count"] == 100

    def test_restart_consumers(self, client_with_mocks):
        """POST /admin/consumer/restart restarts Kafka consumers."""
        client, mocks = client_with_mocks

        response = client.post("/admin/consumer/restart")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "restarted"
        # Verify stop and start were called
        mocks["claims_consumer"].stop.assert_called()
        mocks["claims_consumer"].start.assert_called()

    def test_refresh_entity_truth(self, client_with_mocks):
        """POST /admin/truth/refresh/{id} refreshes entity truth."""
        client, mocks = client_with_mocks

        from services.graph_updater.truth_layer import EntityTruth, TruthValue
        mocks["truth_layer"].refresh_truth = AsyncMock(return_value=EntityTruth(
            entity_id="ent_refresh",
            entity_name="Refresh Entity",
            entity_type="ORGANIZATION",
            truths={
                "ceo": TruthValue(
                    entity_id="ent_refresh",
                    predicate="ceo",
                    value="New CEO",
                    confidence=0.95,
                ),
            },
        ))

        response = client.post("/admin/truth/refresh/ent_refresh")

        assert response.status_code == 200
        data = response.json()
        assert data["entity_id"] == "ent_refresh"
        assert data["refreshed"] is True
        assert data["truths_count"] == 1

    def test_refresh_entity_truth_not_found(self, client_with_mocks):
        """POST /admin/truth/refresh/{id} returns 404 for unknown entity."""
        client, mocks = client_with_mocks
        mocks["truth_layer"].refresh_truth = AsyncMock(return_value=None)

        response = client.post("/admin/truth/refresh/ent_nonexistent")

        assert response.status_code == 404

    def test_materialize_truths(self, client_with_mocks):
        """POST /admin/truth/materialize starts background materialization."""
        client, mocks = client_with_mocks

        response = client.post(
            "/admin/truth/materialize",
            params={"limit": 500}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "started"
        assert "500" in data["message"]


# =============================================================================
# Error Handling Tests
# =============================================================================

class TestErrorHandling:
    """Tests for error handling scenarios."""

    def test_entity_endpoint_service_unavailable(self, client_with_mocks):
        """POST /entity returns 503 when graph_ops is None."""
        client, mocks = client_with_mocks

        from services.graph_updater import main as main_module
        original_graph_ops = main_module.graph_ops
        main_module.graph_ops = None

        response = client.post(
            "/entity",
            params={"name": "Test", "entity_type": "TEST"}
        )

        # Restore
        main_module.graph_ops = original_graph_ops

        assert response.status_code == 503
        assert "not available" in response.json()["detail"].lower()

    def test_truth_endpoint_service_unavailable(self, client_with_mocks):
        """GET /entity/{id}/truth returns 503 when truth_layer is None."""
        client, mocks = client_with_mocks

        from services.graph_updater import main as main_module
        original_truth = main_module.truth_layer
        main_module.truth_layer = None

        response = client.get("/entity/ent_test/truth")

        # Restore
        main_module.truth_layer = original_truth

        assert response.status_code == 503

    def test_claim_endpoint_service_unavailable(self, client_with_mocks):
        """GET /claim/{id} returns 503 when neo4j_client is None."""
        client, mocks = client_with_mocks

        from services.graph_updater import main as main_module
        original_neo4j = main_module.neo4j_client
        main_module.neo4j_client = None

        response = client.get("/claim/claim_test")

        # Restore
        main_module.neo4j_client = original_neo4j

        assert response.status_code == 503

    def test_conflict_resolver_service_unavailable(self, client_with_mocks):
        """POST /conflicts/{id}/resolve returns 503 when resolver is None."""
        client, mocks = client_with_mocks

        from services.graph_updater import main as main_module
        original_resolver = main_module.conflict_resolver
        main_module.conflict_resolver = None

        response = client.post(
            "/conflicts/conflict_test/resolve",
            json={"strategy": "confidence_based"}
        )

        # Restore
        main_module.conflict_resolver = original_resolver

        assert response.status_code == 503

    def test_conflict_detector_service_unavailable(self, client_with_mocks):
        """POST /detect-conflicts returns 503 when detector is None."""
        client, mocks = client_with_mocks

        from services.graph_updater import main as main_module
        original_detector = main_module.conflict_detector
        main_module.conflict_detector = None

        response = client.post(
            "/detect-conflicts",
            params={
                "subject_entity_id": "ent_test",
                "predicate": "test",
                "object_value": "value",
            }
        )

        # Restore
        main_module.conflict_detector = original_detector

        assert response.status_code == 503


# =============================================================================
# Request Validation Tests
# =============================================================================

class TestRequestValidation:
    """Tests for request validation."""

    def test_claim_request_confidence_bounds(self, client_with_mocks):
        """POST /claim validates confidence is between 0 and 1."""
        client, mocks = client_with_mocks

        # Confidence above 1 should fail validation
        response = client.post(
            "/claim",
            json={
                "subject_entity_id": "ent_test",
                "predicate": "test",
                "object_value": "value",
                "source_id": "src_test",
                "confidence": 1.5,  # Invalid
            }
        )

        assert response.status_code == 422  # Validation error

    def test_merge_entities_requires_fields(self, client_with_mocks):
        """POST /entity/merge validates required fields."""
        client, _ = client_with_mocks

        # Missing required fields
        response = client.post(
            "/entity/merge",
            json={}
        )

        assert response.status_code == 422

    def test_list_conflicts_validates_limit(self, client_with_mocks):
        """GET /conflicts validates limit parameter."""
        client, mocks = client_with_mocks
        mocks["graph_ops"].get_unresolved_conflicts = AsyncMock(return_value=[])

        # Limit exceeding max should fail
        response = client.get("/conflicts", params={"limit": 1000})

        assert response.status_code == 422


# =============================================================================
# Integration Scenario Tests
# =============================================================================

class TestIntegrationScenarios:
    """Tests for end-to-end integration scenarios."""

    def test_full_claim_lifecycle(self, client_with_mocks):
        """Test full claim lifecycle: create, detect conflict, resolve."""
        client, mocks = client_with_mocks

        # Step 1: Add initial claim
        mocks["graph_ops"].add_claim = AsyncMock(return_value="claim_initial")

        response = client.post(
            "/claim",
            json={
                "subject_entity_id": "ent_lifecycle",
                "predicate": "revenue",
                "object_value": "$10M",
                "source_id": "src_q1_report",
                "confidence": 0.85,
                "check_conflicts": False,
            }
        )
        assert response.status_code == 200
        assert response.json()["claim_id"] == "claim_initial"

        # Step 2: Add conflicting claim
        mocks["graph_ops"].add_claim_with_conflict_check = AsyncMock(return_value=(
            "claim_conflict",
            [{"id": "conflict_1", "existing_claim_id": "claim_initial"}]
        ))

        response = client.post(
            "/claim",
            json={
                "subject_entity_id": "ent_lifecycle",
                "predicate": "revenue",
                "object_value": "$12M",
                "source_id": "src_q2_report",
                "confidence": 0.95,
                "check_conflicts": True,
            }
        )
        assert response.status_code == 200
        assert response.json()["conflicts_detected"] == 1

        # Step 3: Resolve conflict
        from services.graph_updater.conflict_resolver import (
            ResolutionResult, ResolutionOutcome, ResolutionStrategy, ResolutionAuditEntry
        )
        mocks["neo4j"].set_query_response([
            {"id": "claim_initial", "object_value": "$10M", "confidence": 0.85, "entity_id": "ent_lifecycle"},
            {"id": "claim_conflict", "object_value": "$12M", "confidence": 0.95, "entity_id": "ent_lifecycle"},
        ])
        mocks["conflict_resolver"].resolve_conflict = AsyncMock(return_value=ResolutionResult(
            conflict_id="conflict_1",
            outcome=ResolutionOutcome.WINNER_SELECTED,
            winning_claim_id="claim_conflict",
            losing_claim_ids=["claim_initial"],
            strategy_used=ResolutionStrategy.CONFIDENCE_BASED,
            reason="Higher confidence",
            confidence=0.9,
            audit_entry=ResolutionAuditEntry(
                id="audit_1",
                conflict_id="conflict_1",
                timestamp=datetime.utcnow(),
                strategy_used=ResolutionStrategy.CONFIDENCE_BASED,
                outcome=ResolutionOutcome.WINNER_SELECTED,
                winning_claim_id="claim_conflict",
                losing_claim_ids=["claim_initial"],
                resolved_by="system",
                reason="Higher confidence",
                confidence=0.9,
                scores={},
            ),
            actions_taken=["Resolved"],
        ))

        response = client.post(
            "/conflicts/conflict_1/resolve",
            json={"strategy": "confidence_based"}
        )
        assert response.status_code == 200
        assert response.json()["winning_claim_id"] == "claim_conflict"

    def test_entity_merge_and_truth_refresh(self, client_with_mocks):
        """Test entity merge followed by truth refresh."""
        client, mocks = client_with_mocks

        # Step 1: Merge entities
        from services.graph_updater.operations import EntityMergeResult
        mocks["graph_ops"].merge_entities = AsyncMock(return_value=EntityMergeResult(
            target_entity_id="ent_primary",
            merged_entity_ids=["ent_duplicate"],
            aliases_added=["Duplicate Name"],
            claims_transferred=3,
            relationships_updated=1,
            success=True,
            message="Merged successfully",
        ))

        response = client.post(
            "/entity/merge",
            json={
                "target_entity_id": "ent_primary",
                "source_entity_ids": ["ent_duplicate"],
            }
        )
        assert response.status_code == 200
        assert response.json()["claims_transferred"] == 3

        # Step 2: Refresh truth for merged entity
        from services.graph_updater.truth_layer import EntityTruth, TruthValue
        mocks["truth_layer"].refresh_truth = AsyncMock(return_value=EntityTruth(
            entity_id="ent_primary",
            entity_name="Primary Entity",
            entity_type="ORGANIZATION",
            truths={
                "ceo": TruthValue(entity_id="ent_primary", predicate="ceo", value="CEO Name", confidence=0.9),
                "revenue": TruthValue(entity_id="ent_primary", predicate="revenue", value="$50M", confidence=0.85),
            },
        ))

        response = client.post("/admin/truth/refresh/ent_primary")
        assert response.status_code == 200
        assert response.json()["truths_count"] == 2
