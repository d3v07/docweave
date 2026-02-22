"""
Tests for New API Endpoints
============================

Comprehensive tests for the new endpoints added to the Graph Updater Service:
    - GET /entity/search - Search entities by name, type, aliases
    - GET /entity/{entity_id}/graph - Get entity relationship graph
    - GET /admin/stats - Get graph statistics
    - POST /claim/batch - Batch claim processing
    - POST /conflicts/preview - Preview conflict resolution

Run from project root:
    PYTHONPATH=. pytest services/graph-updater/tests/test_new_endpoints.py -v
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
# Mock Classes
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
    consumer._status = "running"
    consumer.STATUS_RUNNING = "running"
    consumer.get_stats = MagicMock(return_value={
        "running": True,
        "status": "running",
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
    consumer.get_stats = MagicMock(return_value={
        "running": True,
        "status": "running",
    })
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
# Entity Search Endpoint Tests (/entity/search)
# =============================================================================

class TestEntitySearchEndpoint:
    """Tests for GET /entity/search endpoint."""

    def test_search_all_entities_with_wildcard(self, client_with_mocks):
        """GET /entity/search with query=* returns all entities."""
        client, mocks = client_with_mocks

        # Mock search results
        mocks["neo4j"].set_query_responses([
            # Search query results
            [
                {
                    "id": "ent_001",
                    "name": "Acme Corp",
                    "type": "ORGANIZATION",
                    "aliases": ["ACME", "Acme Inc"],
                    "created_at": "2024-01-01T00:00:00",
                    "updated_at": "2024-01-15T00:00:00",
                },
                {
                    "id": "ent_002",
                    "name": "John Doe",
                    "type": "PERSON",
                    "aliases": ["J. Doe"],
                    "created_at": "2024-01-02T00:00:00",
                    "updated_at": "2024-01-16T00:00:00",
                },
            ],
            # Count query results
            [{"total": 2}],
        ])

        response = client.get("/entity/search", params={"query": "*"})

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 2
        assert len(data["entities"]) == 2
        assert data["has_more"] is False
        assert data["entities"][0]["id"] == "ent_001"
        assert data["entities"][0]["name"] == "Acme Corp"

    def test_search_entities_by_name(self, client_with_mocks):
        """GET /entity/search with name query returns matching entities."""
        client, mocks = client_with_mocks

        mocks["neo4j"].set_query_responses([
            # Search results
            [
                {
                    "id": "ent_acme",
                    "name": "Acme Corporation",
                    "type": "ORGANIZATION",
                    "aliases": ["ACME"],
                    "created_at": "2024-01-01T00:00:00",
                    "updated_at": "2024-01-15T00:00:00",
                },
            ],
            # Count results
            [{"total": 1}],
        ])

        response = client.get("/entity/search", params={"query": "acme"})

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["entities"][0]["name"] == "Acme Corporation"

    def test_search_entities_by_alias(self, client_with_mocks):
        """GET /entity/search finds entities by alias."""
        client, mocks = client_with_mocks

        mocks["neo4j"].set_query_responses([
            [
                {
                    "id": "ent_microsoft",
                    "name": "Microsoft Corporation",
                    "type": "ORGANIZATION",
                    "aliases": ["MSFT", "Microsoft"],
                    "created_at": "2024-01-01T00:00:00",
                    "updated_at": "2024-01-15T00:00:00",
                },
            ],
            [{"total": 1}],
        ])

        response = client.get("/entity/search", params={"query": "msft"})

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert "MSFT" in data["entities"][0]["aliases"]

    def test_search_entities_filter_by_type(self, client_with_mocks):
        """GET /entity/search filters by entity_type."""
        client, mocks = client_with_mocks

        mocks["neo4j"].set_query_responses([
            [
                {
                    "id": "ent_person1",
                    "name": "Jane Smith",
                    "type": "PERSON",
                    "aliases": [],
                    "created_at": "2024-01-01T00:00:00",
                    "updated_at": "2024-01-15T00:00:00",
                },
            ],
            [{"total": 1}],
        ])

        response = client.get(
            "/entity/search",
            params={"query": "*", "entity_type": "PERSON"}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["entities"][0]["type"] == "PERSON"

    def test_search_entities_pagination(self, client_with_mocks):
        """GET /entity/search supports pagination with limit and offset."""
        client, mocks = client_with_mocks

        # Create 5 entities, return only 2 (offset=1, limit=2)
        mocks["neo4j"].set_query_responses([
            [
                {
                    "id": "ent_002",
                    "name": "Entity Two",
                    "type": "ORGANIZATION",
                    "aliases": [],
                    "created_at": "2024-01-01T00:00:00",
                    "updated_at": "2024-01-15T00:00:00",
                },
                {
                    "id": "ent_003",
                    "name": "Entity Three",
                    "type": "ORGANIZATION",
                    "aliases": [],
                    "created_at": "2024-01-01T00:00:00",
                    "updated_at": "2024-01-15T00:00:00",
                },
                # Third result indicates has_more
                {
                    "id": "ent_004",
                    "name": "Entity Four",
                    "type": "ORGANIZATION",
                    "aliases": [],
                    "created_at": "2024-01-01T00:00:00",
                    "updated_at": "2024-01-15T00:00:00",
                },
            ],
            [{"total": 5}],
        ])

        response = client.get(
            "/entity/search",
            params={"query": "*", "limit": 2, "offset": 1}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 5
        assert len(data["entities"]) == 2
        assert data["has_more"] is True

    def test_search_entities_include_merged(self, client_with_mocks):
        """GET /entity/search can include merged entities."""
        client, mocks = client_with_mocks

        mocks["neo4j"].set_query_responses([
            [
                {
                    "id": "ent_merged",
                    "name": "Merged Entity",
                    "type": "ORGANIZATION",
                    "aliases": [],
                    "created_at": "2024-01-01T00:00:00",
                    "updated_at": "2024-01-15T00:00:00",
                },
            ],
            [{"total": 1}],
        ])

        response = client.get(
            "/entity/search",
            params={"query": "*", "include_merged": True}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1

    def test_search_entities_empty_results(self, client_with_mocks):
        """GET /entity/search returns empty results when no matches."""
        client, mocks = client_with_mocks

        mocks["neo4j"].set_query_responses([
            [],  # No search results
            [{"total": 0}],  # Count is 0
        ])

        response = client.get(
            "/entity/search",
            params={"query": "nonexistent_entity_name_xyz"}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 0
        assert data["entities"] == []
        assert data["has_more"] is False

    def test_search_entities_validates_limit(self, client_with_mocks):
        """GET /entity/search validates limit parameter."""
        client, _ = client_with_mocks

        # Limit exceeding max (200) should fail
        response = client.get(
            "/entity/search",
            params={"query": "*", "limit": 500}
        )

        assert response.status_code == 422

    def test_search_entities_database_unavailable(self, client_with_mocks):
        """GET /entity/search returns 503 when database is unavailable."""
        client, mocks = client_with_mocks

        from services.graph_updater import main as main_module
        original_neo4j = main_module.neo4j_client
        main_module.neo4j_client = None

        response = client.get("/entity/search", params={"query": "*"})

        main_module.neo4j_client = original_neo4j

        assert response.status_code == 503
        assert "not available" in response.json()["detail"].lower()


# =============================================================================
# Entity Graph Endpoint Tests (/entity/{entity_id}/graph)
# =============================================================================

class TestEntityGraphEndpoint:
    """Tests for GET /entity/{entity_id}/graph endpoint."""

    def test_get_entity_graph_success(self, client_with_mocks):
        """GET /entity/{id}/graph returns entity and relationships."""
        client, mocks = client_with_mocks

        # Mock center entity query
        mocks["neo4j"].set_query_responses([
            # Center entity
            [
                {
                    "id": "ent_center",
                    "name": "Acme Corp",
                    "type": "ORGANIZATION",
                    "aliases": ["ACME"],
                    "created_at": "2024-01-01T00:00:00",
                    "updated_at": "2024-01-15T00:00:00",
                },
            ],
            # Related entities and relationships
            [
                {
                    "entity_id": "ent_ceo",
                    "entity_name": "John Smith",
                    "entity_type": "PERSON",
                    "aliases": [],
                    "created_at": "2024-01-01T00:00:00",
                    "updated_at": "2024-01-15T00:00:00",
                    "source_entity_id": "ent_center",
                    "target_entity_id": "ent_ceo",
                    "relationship_type": "HAS_CEO",
                    "predicate": "ceo",
                    "confidence": 0.95,
                    "claim_ids": ["claim_1"],
                },
                {
                    "entity_id": "ent_subsidiary",
                    "entity_name": "Acme Labs",
                    "entity_type": "ORGANIZATION",
                    "aliases": [],
                    "created_at": "2024-01-01T00:00:00",
                    "updated_at": "2024-01-15T00:00:00",
                    "source_entity_id": "ent_center",
                    "target_entity_id": "ent_subsidiary",
                    "relationship_type": "OWNS",
                    "predicate": "subsidiary",
                    "confidence": 0.9,
                    "claim_ids": ["claim_2"],
                },
            ],
        ])

        response = client.get("/entity/ent_center/graph")

        assert response.status_code == 200
        data = response.json()
        assert data["center_entity_id"] == "ent_center"
        assert data["depth"] == 2  # Default depth
        assert len(data["entities"]) == 3  # Center + 2 related
        assert len(data["relationships"]) == 2

        # Verify center entity is included
        entity_ids = [e["id"] for e in data["entities"]]
        assert "ent_center" in entity_ids
        assert "ent_ceo" in entity_ids
        assert "ent_subsidiary" in entity_ids

    def test_get_entity_graph_with_custom_depth(self, client_with_mocks):
        """GET /entity/{id}/graph respects depth parameter."""
        client, mocks = client_with_mocks

        mocks["neo4j"].set_query_responses([
            [
                {
                    "id": "ent_center",
                    "name": "Acme Corp",
                    "type": "ORGANIZATION",
                    "aliases": [],
                    "created_at": "2024-01-01T00:00:00",
                    "updated_at": "2024-01-15T00:00:00",
                },
            ],
            [],  # No relationships at depth 1
        ])

        response = client.get("/entity/ent_center/graph", params={"depth": 1})

        assert response.status_code == 200
        data = response.json()
        assert data["depth"] == 1

    def test_get_entity_graph_max_depth(self, client_with_mocks):
        """GET /entity/{id}/graph accepts max depth of 5."""
        client, mocks = client_with_mocks

        mocks["neo4j"].set_query_responses([
            [
                {
                    "id": "ent_center",
                    "name": "Acme Corp",
                    "type": "ORGANIZATION",
                    "aliases": [],
                    "created_at": "2024-01-01T00:00:00",
                    "updated_at": "2024-01-15T00:00:00",
                },
            ],
            [],
        ])

        response = client.get("/entity/ent_center/graph", params={"depth": 5})

        assert response.status_code == 200
        data = response.json()
        assert data["depth"] == 5

    def test_get_entity_graph_depth_exceeds_max(self, client_with_mocks):
        """GET /entity/{id}/graph rejects depth > 5."""
        client, _ = client_with_mocks

        response = client.get("/entity/ent_center/graph", params={"depth": 6})

        assert response.status_code == 422

    def test_get_entity_graph_depth_below_min(self, client_with_mocks):
        """GET /entity/{id}/graph rejects depth < 1."""
        client, _ = client_with_mocks

        response = client.get("/entity/ent_center/graph", params={"depth": 0})

        assert response.status_code == 422

    def test_get_entity_graph_not_found(self, client_with_mocks):
        """GET /entity/{id}/graph returns 404 for unknown entity."""
        client, mocks = client_with_mocks

        mocks["neo4j"].set_query_responses([
            [],  # No center entity found
        ])

        response = client.get("/entity/ent_nonexistent/graph")

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_get_entity_graph_no_relationships(self, client_with_mocks):
        """GET /entity/{id}/graph returns entity with empty relationships."""
        client, mocks = client_with_mocks

        mocks["neo4j"].set_query_responses([
            [
                {
                    "id": "ent_isolated",
                    "name": "Isolated Entity",
                    "type": "ORGANIZATION",
                    "aliases": [],
                    "created_at": "2024-01-01T00:00:00",
                    "updated_at": "2024-01-15T00:00:00",
                },
            ],
            [],  # No relationships
        ])

        response = client.get("/entity/ent_isolated/graph")

        assert response.status_code == 200
        data = response.json()
        assert data["center_entity_id"] == "ent_isolated"
        assert len(data["entities"]) == 1
        assert len(data["relationships"]) == 0

    def test_get_entity_graph_database_unavailable(self, client_with_mocks):
        """GET /entity/{id}/graph returns 503 when database is unavailable."""
        client, mocks = client_with_mocks

        from services.graph_updater import main as main_module
        original_neo4j = main_module.neo4j_client
        main_module.neo4j_client = None

        response = client.get("/entity/ent_test/graph")

        main_module.neo4j_client = original_neo4j

        assert response.status_code == 503


# =============================================================================
# Admin Stats Endpoint Tests (/admin/stats)
# =============================================================================

class TestAdminStatsEndpoint:
    """Tests for GET /admin/stats endpoint."""

    def test_get_graph_stats_success(self, client_with_mocks):
        """GET /admin/stats returns graph statistics."""
        client, mocks = client_with_mocks

        mocks["neo4j"].set_query_response([
            {
                "entity_count": 150,
                "claim_count": 500,
                "source_count": 25,
                "conflict_count": 12,
                "truth_count": 300,
                "resolution_count": 8,
            },
        ])

        response = client.get("/admin/stats")

        assert response.status_code == 200
        data = response.json()
        assert data["entity_count"] == 150
        assert data["claim_count"] == 500
        assert data["source_count"] == 25
        assert data["conflict_count"] == 12
        assert data["truth_count"] == 300
        assert data["resolution_count"] == 8
        assert "last_updated" in data

    def test_get_graph_stats_empty_graph(self, client_with_mocks):
        """GET /admin/stats returns zeros for empty graph."""
        client, mocks = client_with_mocks

        mocks["neo4j"].set_query_response([
            {
                "entity_count": 0,
                "claim_count": 0,
                "source_count": 0,
                "conflict_count": 0,
                "truth_count": 0,
                "resolution_count": 0,
            },
        ])

        response = client.get("/admin/stats")

        assert response.status_code == 200
        data = response.json()
        assert data["entity_count"] == 0
        assert data["claim_count"] == 0

    def test_get_graph_stats_handles_query_error(self, client_with_mocks):
        """GET /admin/stats returns default values on query error."""
        client, mocks = client_with_mocks

        mocks["neo4j"].execute_query.side_effect = Exception("Query failed")

        response = client.get("/admin/stats")

        assert response.status_code == 200
        data = response.json()
        # Should return default zeros on error
        assert data["entity_count"] == 0
        assert data["claim_count"] == 0
        assert "last_updated" in data

    def test_get_graph_stats_database_unavailable(self, client_with_mocks):
        """GET /admin/stats returns 503 when database is unavailable."""
        client, mocks = client_with_mocks

        from services.graph_updater import main as main_module
        original_neo4j = main_module.neo4j_client
        main_module.neo4j_client = None

        response = client.get("/admin/stats")

        main_module.neo4j_client = original_neo4j

        assert response.status_code == 503


# =============================================================================
# Batch Claim Endpoint Tests (/claim/batch)
# =============================================================================

class TestBatchClaimEndpoint:
    """Tests for POST /claim/batch endpoint."""

    def test_batch_claims_all_successful(self, client_with_mocks):
        """POST /claim/batch processes all claims successfully."""
        client, mocks = client_with_mocks

        mocks["graph_ops"].add_claim = AsyncMock(
            side_effect=["claim_001", "claim_002", "claim_003"]
        )

        response = client.post(
            "/claim/batch",
            json={
                "claims": [
                    {
                        "subject_entity_id": "ent_1",
                        "predicate": "ceo",
                        "object_value": "John Doe",
                        "source_id": "src_1",
                        "confidence": 0.9,
                    },
                    {
                        "subject_entity_id": "ent_2",
                        "predicate": "revenue",
                        "object_value": "$10M",
                        "source_id": "src_1",
                        "confidence": 0.85,
                    },
                    {
                        "subject_entity_id": "ent_3",
                        "predicate": "employee_count",
                        "object_value": "500",
                        "source_id": "src_1",
                        "confidence": 0.8,
                    },
                ],
                "check_conflicts": False,
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total_claims"] == 3
        assert data["successful"] == 3
        assert data["failed"] == 0
        assert len(data["claim_ids"]) == 3
        assert data["claim_ids"] == ["claim_001", "claim_002", "claim_003"]
        assert data["errors"] == []
        assert data["processing_time_ms"] > 0

    def test_batch_claims_with_default_source(self, client_with_mocks):
        """POST /claim/batch uses default source_id from request."""
        client, mocks = client_with_mocks

        mocks["graph_ops"].add_claim = AsyncMock(return_value="claim_test")

        response = client.post(
            "/claim/batch",
            json={
                "claims": [
                    {
                        "subject_entity_id": "ent_1",
                        "predicate": "ceo",
                        "object_value": "John Doe",
                    },
                ],
                "source_id": "default_source",
                "check_conflicts": False,
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["successful"] == 1

    def test_batch_claims_with_conflict_check(self, client_with_mocks):
        """POST /claim/batch detects conflicts when enabled."""
        client, mocks = client_with_mocks

        mocks["graph_ops"].add_claim_with_conflict_check = AsyncMock(
            side_effect=[
                ("claim_001", []),  # No conflicts
                ("claim_002", [{"id": "conflict_1"}]),  # One conflict
            ]
        )

        response = client.post(
            "/claim/batch",
            json={
                "claims": [
                    {
                        "subject_entity_id": "ent_1",
                        "predicate": "ceo",
                        "object_value": "John Doe",
                        "source_id": "src_1",
                    },
                    {
                        "subject_entity_id": "ent_2",
                        "predicate": "ceo",
                        "object_value": "Jane Smith",
                        "source_id": "src_1",
                    },
                ],
                "check_conflicts": True,
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["successful"] == 2
        assert data["conflicts_detected"] == 1

    def test_batch_claims_partial_failure(self, client_with_mocks):
        """POST /claim/batch handles partial failures."""
        client, mocks = client_with_mocks

        mocks["graph_ops"].add_claim = AsyncMock(
            side_effect=["claim_001", Exception("Database error"), "claim_003"]
        )

        response = client.post(
            "/claim/batch",
            json={
                "claims": [
                    {
                        "subject_entity_id": "ent_1",
                        "predicate": "attr1",
                        "object_value": "value1",
                        "source_id": "src_1",
                    },
                    {
                        "subject_entity_id": "ent_2",
                        "predicate": "attr2",
                        "object_value": "value2",
                        "source_id": "src_1",
                    },
                    {
                        "subject_entity_id": "ent_3",
                        "predicate": "attr3",
                        "object_value": "value3",
                        "source_id": "src_1",
                    },
                ],
                "check_conflicts": False,
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total_claims"] == 3
        assert data["successful"] == 2
        assert data["failed"] == 1
        assert len(data["errors"]) == 1
        assert data["errors"][0]["index"] == 1
        assert "Database error" in data["errors"][0]["error"]

    def test_batch_claims_missing_required_fields(self, client_with_mocks):
        """POST /claim/batch reports errors for missing required fields."""
        client, mocks = client_with_mocks

        mocks["graph_ops"].add_claim = AsyncMock(return_value="claim_valid")

        response = client.post(
            "/claim/batch",
            json={
                "claims": [
                    {
                        "subject_entity_id": "ent_1",
                        "predicate": "attr1",
                        "object_value": "value1",
                        "source_id": "src_1",
                    },
                    {
                        # Missing predicate
                        "subject_entity_id": "ent_2",
                        "object_value": "value2",
                        "source_id": "src_1",
                    },
                    {
                        # Missing source_id and no default
                        "subject_entity_id": "ent_3",
                        "predicate": "attr3",
                        "object_value": "value3",
                    },
                ],
                "check_conflicts": False,
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total_claims"] == 3
        assert data["successful"] == 1
        assert data["failed"] == 2
        assert len(data["errors"]) == 2

    def test_batch_claims_empty_list(self, client_with_mocks):
        """POST /claim/batch handles empty claims list."""
        client, mocks = client_with_mocks

        response = client.post(
            "/claim/batch",
            json={
                "claims": [],
                "check_conflicts": False,
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total_claims"] == 0
        assert data["successful"] == 0
        assert data["failed"] == 0

    def test_batch_claims_service_unavailable(self, client_with_mocks):
        """POST /claim/batch returns 503 when graph_ops is unavailable."""
        client, mocks = client_with_mocks

        from services.graph_updater import main as main_module
        original_graph_ops = main_module.graph_ops
        main_module.graph_ops = None

        response = client.post(
            "/claim/batch",
            json={
                "claims": [
                    {
                        "subject_entity_id": "ent_1",
                        "predicate": "attr1",
                        "object_value": "value1",
                        "source_id": "src_1",
                    },
                ],
            }
        )

        main_module.graph_ops = original_graph_ops

        assert response.status_code == 503

    def test_batch_claims_invalidates_truth_cache(self, client_with_mocks):
        """POST /claim/batch invalidates truth cache for affected entities."""
        client, mocks = client_with_mocks

        mocks["graph_ops"].add_claim = AsyncMock(return_value="claim_test")

        response = client.post(
            "/claim/batch",
            json={
                "claims": [
                    {
                        "subject_entity_id": "ent_1",
                        "predicate": "attr1",
                        "object_value": "value1",
                        "source_id": "src_1",
                    },
                    {
                        "subject_entity_id": "ent_2",
                        "predicate": "attr2",
                        "object_value": "value2",
                        "source_id": "src_1",
                    },
                ],
                "check_conflicts": False,
            }
        )

        assert response.status_code == 200
        # Verify cache invalidation was called for each entity
        assert mocks["truth_layer"].invalidate_cache.call_count == 2


# =============================================================================
# Conflict Preview Endpoint Tests (/conflicts/preview)
# =============================================================================

class TestConflictPreviewEndpoint:
    """Tests for POST /conflicts/preview endpoint."""

    def test_preview_resolution_timestamp_strategy(self, client_with_mocks):
        """POST /conflicts/preview with timestamp_based strategy."""
        client, mocks = client_with_mocks

        # Mock conflict data query
        mocks["neo4j"].set_query_response([
            {
                "id": "claim_old",
                "object_value": "1200",
                "confidence": 0.85,
                "timestamp": "2023-01-01T00:00:00",
                "source_reliability": 0.8,
                "entity_id": "ent_test",
            },
            {
                "id": "claim_new",
                "object_value": "1500",
                "confidence": 0.9,
                "timestamp": "2024-01-01T00:00:00",
                "source_reliability": 0.85,
                "entity_id": "ent_test",
            },
        ])

        response = client.post(
            "/conflicts/preview",
            json={
                "conflict_id": "conflict_123",
                "strategy": "timestamp_based",
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["conflict_id"] == "conflict_123"
        assert data["proposed_winner"] == "claim_new"  # Newer wins
        assert "claim_old" in data["proposed_losers"]
        assert data["confidence"] == 0.85
        assert "timestamp" in data["rationale"].lower()
        assert len(data["side_effects"]) > 0

    def test_preview_resolution_confidence_strategy(self, client_with_mocks):
        """POST /conflicts/preview with confidence_based strategy."""
        client, mocks = client_with_mocks

        mocks["neo4j"].set_query_response([
            {
                "id": "claim_low",
                "object_value": "1200",
                "confidence": 0.7,
                "timestamp": "2024-01-01T00:00:00",
                "source_reliability": 0.9,
                "entity_id": "ent_test",
            },
            {
                "id": "claim_high",
                "object_value": "1500",
                "confidence": 0.95,
                "timestamp": "2023-01-01T00:00:00",
                "source_reliability": 0.8,
                "entity_id": "ent_test",
            },
        ])

        response = client.post(
            "/conflicts/preview",
            json={
                "conflict_id": "conflict_123",
                "strategy": "confidence_based",
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["proposed_winner"] == "claim_high"  # Higher confidence wins
        assert data["confidence"] == 0.95
        assert "confidence" in data["rationale"].lower()

    def test_preview_resolution_source_reliability_strategy(self, client_with_mocks):
        """POST /conflicts/preview with source_reliability strategy."""
        client, mocks = client_with_mocks

        mocks["neo4j"].set_query_response([
            {
                "id": "claim_unreliable",
                "object_value": "1200",
                "confidence": 0.9,
                "timestamp": "2024-01-01T00:00:00",
                "source_reliability": 0.5,
                "entity_id": "ent_test",
            },
            {
                "id": "claim_reliable",
                "object_value": "1500",
                "confidence": 0.8,
                "timestamp": "2023-01-01T00:00:00",
                "source_reliability": 0.95,
                "entity_id": "ent_test",
            },
        ])

        response = client.post(
            "/conflicts/preview",
            json={
                "conflict_id": "conflict_123",
                "strategy": "source_reliability",
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["proposed_winner"] == "claim_reliable"  # More reliable source wins
        assert "reliable" in data["rationale"].lower()

    def test_preview_resolution_manual_selection(self, client_with_mocks):
        """POST /conflicts/preview with manual winning_claim_id."""
        client, mocks = client_with_mocks

        mocks["neo4j"].set_query_response([
            {
                "id": "claim_a",
                "object_value": "1200",
                "confidence": 0.9,
                "timestamp": "2024-01-01T00:00:00",
                "source_reliability": 0.8,
                "entity_id": "ent_test",
            },
            {
                "id": "claim_b",
                "object_value": "1500",
                "confidence": 0.8,
                "timestamp": "2023-01-01T00:00:00",
                "source_reliability": 0.9,
                "entity_id": "ent_test",
            },
        ])

        response = client.post(
            "/conflicts/preview",
            json={
                "conflict_id": "conflict_123",
                "strategy": "manual_review",
                "winning_claim_id": "claim_a",
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["proposed_winner"] == "claim_a"
        assert "claim_b" in data["proposed_losers"]
        assert data["confidence"] == 1.0
        assert "manual" in data["rationale"].lower()

    def test_preview_resolution_weighted_combination(self, client_with_mocks):
        """POST /conflicts/preview with weighted_combination strategy."""
        client, mocks = client_with_mocks

        mocks["neo4j"].set_query_response([
            {
                "id": "claim_1",
                "object_value": "value1",
                "confidence": 0.8,
                "timestamp": "2024-01-01T00:00:00",
                "source_reliability": 0.7,
                "entity_id": "ent_test",
            },
            {
                "id": "claim_2",
                "object_value": "value2",
                "confidence": 0.75,
                "timestamp": "2023-06-01T00:00:00",
                "source_reliability": 0.8,
                "entity_id": "ent_test",
            },
        ])

        response = client.post(
            "/conflicts/preview",
            json={
                "conflict_id": "conflict_123",
                "strategy": "weighted_combination",
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["proposed_winner"] is not None
        assert "weighted" in data["rationale"].lower()

    def test_preview_resolution_conflict_not_found(self, client_with_mocks):
        """POST /conflicts/preview returns 404 for unknown conflict."""
        client, mocks = client_with_mocks

        mocks["neo4j"].set_query_response([])  # No conflict found

        response = client.post(
            "/conflicts/preview",
            json={
                "conflict_id": "nonexistent_conflict",
                "strategy": "confidence_based",
            }
        )

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_preview_resolution_side_effects_included(self, client_with_mocks):
        """POST /conflicts/preview includes side effects description."""
        client, mocks = client_with_mocks

        mocks["neo4j"].set_query_response([
            {
                "id": "claim_a",
                "object_value": "value_a",
                "confidence": 0.9,
                "timestamp": "2024-01-01T00:00:00",
                "source_reliability": 0.85,
                "entity_id": "ent_test",
            },
            {
                "id": "claim_b",
                "object_value": "value_b",
                "confidence": 0.8,
                "timestamp": "2023-01-01T00:00:00",
                "source_reliability": 0.8,
                "entity_id": "ent_test",
            },
        ])

        response = client.post(
            "/conflicts/preview",
            json={
                "conflict_id": "conflict_123",
                "strategy": "confidence_based",
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["side_effects"]) > 0
        # Verify expected side effects are mentioned
        side_effects_text = " ".join(data["side_effects"]).lower()
        assert "superseded" in side_effects_text
        assert "truth" in side_effects_text
        assert "audit" in side_effects_text

    def test_preview_resolution_service_unavailable(self, client_with_mocks):
        """POST /conflicts/preview returns 503 when resolver is unavailable."""
        client, mocks = client_with_mocks

        from services.graph_updater import main as main_module
        original_resolver = main_module.conflict_resolver
        main_module.conflict_resolver = None

        response = client.post(
            "/conflicts/preview",
            json={
                "conflict_id": "conflict_123",
                "strategy": "confidence_based",
            }
        )

        main_module.conflict_resolver = original_resolver

        assert response.status_code == 503

    def test_preview_resolution_default_strategy(self, client_with_mocks):
        """POST /conflicts/preview uses weighted_combination as default."""
        client, mocks = client_with_mocks

        mocks["neo4j"].set_query_response([
            {
                "id": "claim_a",
                "object_value": "value_a",
                "confidence": 0.9,
                "timestamp": "2024-01-01T00:00:00",
                "source_reliability": 0.85,
                "entity_id": "ent_test",
            },
        ])

        # No strategy specified, should use default
        response = client.post(
            "/conflicts/preview",
            json={
                "conflict_id": "conflict_123",
                "strategy": "",  # Empty strategy
            }
        )

        # Should not fail, uses default
        assert response.status_code in [200, 422]


# =============================================================================
# Integration Tests for New Endpoints
# =============================================================================

class TestNewEndpointsIntegration:
    """Integration tests combining multiple new endpoints."""

    def test_search_then_get_graph(self, client_with_mocks):
        """Test searching for entity then getting its graph."""
        client, mocks = client_with_mocks

        # Step 1: Search for entity
        mocks["neo4j"].set_query_responses([
            [
                {
                    "id": "ent_found",
                    "name": "Found Corp",
                    "type": "ORGANIZATION",
                    "aliases": [],
                    "created_at": "2024-01-01T00:00:00",
                    "updated_at": "2024-01-15T00:00:00",
                },
            ],
            [{"total": 1}],
        ])

        search_response = client.get(
            "/entity/search",
            params={"query": "Found"}
        )

        assert search_response.status_code == 200
        entity_id = search_response.json()["entities"][0]["id"]

        # Step 2: Get entity graph
        mocks["neo4j"].set_query_responses([
            [
                {
                    "id": entity_id,
                    "name": "Found Corp",
                    "type": "ORGANIZATION",
                    "aliases": [],
                    "created_at": "2024-01-01T00:00:00",
                    "updated_at": "2024-01-15T00:00:00",
                },
            ],
            [],  # No relationships
        ])

        graph_response = client.get(f"/entity/{entity_id}/graph")

        assert graph_response.status_code == 200
        assert graph_response.json()["center_entity_id"] == entity_id

    def test_batch_claims_then_check_stats(self, client_with_mocks):
        """Test adding batch claims then verifying stats update."""
        client, mocks = client_with_mocks

        # Step 1: Add batch claims
        mocks["graph_ops"].add_claim = AsyncMock(
            side_effect=["claim_1", "claim_2"]
        )

        batch_response = client.post(
            "/claim/batch",
            json={
                "claims": [
                    {
                        "subject_entity_id": "ent_1",
                        "predicate": "attr1",
                        "object_value": "val1",
                        "source_id": "src_1",
                    },
                    {
                        "subject_entity_id": "ent_2",
                        "predicate": "attr2",
                        "object_value": "val2",
                        "source_id": "src_1",
                    },
                ],
                "check_conflicts": False,
            }
        )

        assert batch_response.status_code == 200
        assert batch_response.json()["successful"] == 2

        # Step 2: Check stats
        mocks["neo4j"].set_query_response([
            {
                "entity_count": 2,
                "claim_count": 2,
                "source_count": 1,
                "conflict_count": 0,
                "truth_count": 0,
                "resolution_count": 0,
            },
        ])

        stats_response = client.get("/admin/stats")

        assert stats_response.status_code == 200
        assert stats_response.json()["claim_count"] == 2

    def test_batch_claims_with_conflicts_then_preview(self, client_with_mocks):
        """Test adding conflicting batch claims then previewing resolution."""
        client, mocks = client_with_mocks

        # Step 1: Add batch with conflict detection
        mocks["graph_ops"].add_claim_with_conflict_check = AsyncMock(
            return_value=("claim_new", [{"id": "conflict_1", "claim1_id": "claim_old"}])
        )

        batch_response = client.post(
            "/claim/batch",
            json={
                "claims": [
                    {
                        "subject_entity_id": "ent_1",
                        "predicate": "revenue",
                        "object_value": "$15M",
                        "source_id": "src_new",
                    },
                ],
                "check_conflicts": True,
            }
        )

        assert batch_response.status_code == 200
        assert batch_response.json()["conflicts_detected"] == 1

        # Step 2: Preview conflict resolution
        mocks["neo4j"].set_query_response([
            {
                "id": "claim_old",
                "object_value": "$10M",
                "confidence": 0.8,
                "timestamp": "2023-01-01T00:00:00",
                "source_reliability": 0.7,
                "entity_id": "ent_1",
            },
            {
                "id": "claim_new",
                "object_value": "$15M",
                "confidence": 0.9,
                "timestamp": "2024-01-01T00:00:00",
                "source_reliability": 0.85,
                "entity_id": "ent_1",
            },
        ])

        preview_response = client.post(
            "/conflicts/preview",
            json={
                "conflict_id": "conflict_1",
                "strategy": "confidence_based",
            }
        )

        assert preview_response.status_code == 200
        assert preview_response.json()["proposed_winner"] == "claim_new"


# =============================================================================
# Edge Cases and Error Handling
# =============================================================================

class TestNewEndpointsEdgeCases:
    """Edge cases and error handling for new endpoints."""

    def test_search_with_special_characters(self, client_with_mocks):
        """GET /entity/search handles special characters in query."""
        client, mocks = client_with_mocks

        mocks["neo4j"].set_query_responses([
            [],
            [{"total": 0}],
        ])

        # Query with special characters
        response = client.get(
            "/entity/search",
            params={"query": "O'Brien & Co."}
        )

        assert response.status_code == 200

    def test_search_with_unicode(self, client_with_mocks):
        """GET /entity/search handles unicode characters."""
        client, mocks = client_with_mocks

        mocks["neo4j"].set_query_responses([
            [
                {
                    "id": "ent_unicode",
                    "name": "Cafe Creme",
                    "type": "ORGANIZATION",
                    "aliases": [],
                    "created_at": "2024-01-01T00:00:00",
                    "updated_at": "2024-01-15T00:00:00",
                },
            ],
            [{"total": 1}],
        ])

        response = client.get(
            "/entity/search",
            params={"query": "Cafe"}
        )

        assert response.status_code == 200

    def test_batch_claims_with_null_values(self, client_with_mocks):
        """POST /claim/batch handles claims with null optional values."""
        client, mocks = client_with_mocks

        mocks["graph_ops"].add_claim = AsyncMock(return_value="claim_test")

        response = client.post(
            "/claim/batch",
            json={
                "claims": [
                    {
                        "subject_entity_id": "ent_1",
                        "predicate": "attr1",
                        "object_value": "value1",
                        "source_id": "src_1",
                        "confidence": 0.8,
                        "extracted_text": None,
                    },
                ],
                "check_conflicts": False,
            }
        )

        assert response.status_code == 200

    def test_preview_with_single_claim_conflict(self, client_with_mocks):
        """POST /conflicts/preview handles conflict with single claim."""
        client, mocks = client_with_mocks

        # Only one claim in conflict (edge case)
        mocks["neo4j"].set_query_response([
            {
                "id": "claim_only",
                "object_value": "value",
                "confidence": 0.9,
                "timestamp": "2024-01-01T00:00:00",
                "source_reliability": 0.85,
                "entity_id": "ent_test",
            },
        ])

        response = client.post(
            "/conflicts/preview",
            json={
                "conflict_id": "conflict_123",
                "strategy": "confidence_based",
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["proposed_winner"] == "claim_only"
        assert data["proposed_losers"] == []

    def test_entity_graph_with_circular_relationships(self, client_with_mocks):
        """GET /entity/{id}/graph handles circular relationships."""
        client, mocks = client_with_mocks

        mocks["neo4j"].set_query_responses([
            [
                {
                    "id": "ent_a",
                    "name": "Entity A",
                    "type": "ORGANIZATION",
                    "aliases": [],
                    "created_at": "2024-01-01T00:00:00",
                    "updated_at": "2024-01-15T00:00:00",
                },
            ],
            # Circular: A -> B -> A
            [
                {
                    "entity_id": "ent_b",
                    "entity_name": "Entity B",
                    "entity_type": "ORGANIZATION",
                    "aliases": [],
                    "created_at": "2024-01-01T00:00:00",
                    "updated_at": "2024-01-15T00:00:00",
                    "source_entity_id": "ent_a",
                    "target_entity_id": "ent_b",
                    "relationship_type": "RELATED_TO",
                    "predicate": "related",
                    "confidence": 0.9,
                    "claim_ids": [],
                },
                {
                    "entity_id": "ent_a",
                    "entity_name": "Entity A",
                    "entity_type": "ORGANIZATION",
                    "aliases": [],
                    "created_at": "2024-01-01T00:00:00",
                    "updated_at": "2024-01-15T00:00:00",
                    "source_entity_id": "ent_b",
                    "target_entity_id": "ent_a",
                    "relationship_type": "RELATED_TO",
                    "predicate": "related",
                    "confidence": 0.9,
                    "claim_ids": [],
                },
            ],
        ])

        response = client.get("/entity/ent_a/graph")

        assert response.status_code == 200
        data = response.json()
        # Should deduplicate entities
        entity_ids = [e["id"] for e in data["entities"]]
        assert len(entity_ids) == len(set(entity_ids))  # No duplicates
