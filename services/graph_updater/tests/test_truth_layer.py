"""
Tests for Truth Layer
=====================

Unit tests for the TruthLayer class covering:
- Current truth computation
- Historical truth queries
- Truth propagation after resolution
- Cache management
- Multi-value predicate handling
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

# Import from conftest's path setup and mocking
from truth_layer import (
    TruthLayer,
    TruthLayerConfig,
    TruthValue,
    EntityTruth,
    HistoricalTruth,
    TruthQueryRequest,
    TruthResponse,
    HistoryResponse,
)


class MockQueryResult:
    """Mock Neo4j query result."""

    def __init__(self, records=None):
        self.records = records or []


@pytest.fixture
def mock_neo4j_client():
    """Create a mock Neo4j client."""
    client = AsyncMock()
    client.execute_query = AsyncMock(return_value=MockQueryResult([]))
    return client


@pytest.fixture
def config():
    """Create truth layer configuration."""
    return TruthLayerConfig(
        use_weighted_confidence=True,
        min_truth_confidence=0.3,
        cache_ttl_seconds=300,
        enable_materialized_views=True,
        single_value_predicates=["ceo", "founded_date", "headquarters", "employee_count"],
    )


@pytest.fixture
def truth_layer(mock_neo4j_client, config):
    """Create TruthLayer with mock client."""
    return TruthLayer(mock_neo4j_client, config)


class TestGetEntityTruth:
    """Tests for getting entity truth."""

    @pytest.mark.asyncio
    async def test_get_entity_truth_success(self, truth_layer, mock_neo4j_client):
        """Test successful retrieval of entity truth."""
        mock_neo4j_client.execute_query.side_effect = [
            # Entity query
            MockQueryResult([{"id": "ent_123", "name": "Acme Corp", "type": "ORGANIZATION"}]),
            # Claims query
            MockQueryResult([
                {
                    "predicate": "ceo",
                    "claims": [
                        {"claim_id": "claim_1", "value": "John Smith", "confidence": 0.95, "source_id": "src_1"}
                    ]
                },
                {
                    "predicate": "employee_count",
                    "claims": [
                        {"claim_id": "claim_2", "value": "1500", "confidence": 0.9, "source_id": "src_2"}
                    ]
                },
            ]),
            # Conflicts query
            MockQueryResult([{"conflict_count": 0}]),
        ]

        entity_truth = await truth_layer.get_entity_truth("ent_123")

        assert entity_truth is not None
        assert entity_truth.entity_id == "ent_123"
        assert entity_truth.entity_name == "Acme Corp"
        assert "ceo" in entity_truth.truths
        assert "employee_count" in entity_truth.truths
        assert entity_truth.has_conflicts is False

    @pytest.mark.asyncio
    async def test_get_entity_truth_not_found(self, truth_layer, mock_neo4j_client):
        """Test entity not found returns None."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([])

        result = await truth_layer.get_entity_truth("nonexistent")

        assert result is None

    @pytest.mark.asyncio
    async def test_get_entity_truth_with_conflicts(self, truth_layer, mock_neo4j_client):
        """Test entity truth indicates conflicts exist."""
        mock_neo4j_client.execute_query.side_effect = [
            MockQueryResult([{"id": "ent_123", "name": "Acme Corp", "type": "ORGANIZATION"}]),
            MockQueryResult([
                {
                    "predicate": "ceo",
                    "claims": [
                        {"claim_id": "claim_1", "value": "John Smith", "confidence": 0.9, "source_id": "src_1"}
                    ]
                }
            ]),
            MockQueryResult([{"conflict_count": 2}]),  # Has conflicts
        ]

        entity_truth = await truth_layer.get_entity_truth("ent_123")

        assert entity_truth.has_conflicts is True
        assert entity_truth.conflict_count == 2

    @pytest.mark.asyncio
    async def test_get_entity_truth_uses_cache(self, truth_layer, mock_neo4j_client):
        """Test that cached values are returned."""
        mock_neo4j_client.execute_query.side_effect = [
            MockQueryResult([{"id": "ent_123", "name": "Acme Corp", "type": "ORGANIZATION"}]),
            MockQueryResult([]),
            MockQueryResult([{"conflict_count": 0}]),
        ]

        # First call
        await truth_layer.get_entity_truth("ent_123")

        # Second call should use cache
        await truth_layer.get_entity_truth("ent_123", use_cache=True)

        # Only first call should hit database (entity query + claims + conflicts)
        assert mock_neo4j_client.execute_query.call_count == 3

    @pytest.mark.asyncio
    async def test_get_entity_truth_bypass_cache(self, truth_layer, mock_neo4j_client):
        """Test bypassing cache."""
        mock_neo4j_client.execute_query.side_effect = [
            MockQueryResult([{"id": "ent_123", "name": "Acme Corp", "type": "ORGANIZATION"}]),
            MockQueryResult([]),
            MockQueryResult([{"conflict_count": 0}]),
            MockQueryResult([{"id": "ent_123", "name": "Acme Corp", "type": "ORGANIZATION"}]),
            MockQueryResult([]),
            MockQueryResult([{"conflict_count": 0}]),
        ]

        # First call
        await truth_layer.get_entity_truth("ent_123")

        # Second call bypassing cache
        await truth_layer.get_entity_truth("ent_123", use_cache=False)

        # Both calls should hit database
        assert mock_neo4j_client.execute_query.call_count == 6


class TestGetTruthValue:
    """Tests for getting specific truth values."""

    @pytest.mark.asyncio
    async def test_get_truth_value_exists(self, truth_layer, mock_neo4j_client):
        """Test getting a specific truth value that exists."""
        mock_neo4j_client.execute_query.side_effect = [
            MockQueryResult([{"id": "ent_123", "name": "Acme Corp", "type": "ORGANIZATION"}]),
            MockQueryResult([
                {
                    "predicate": "ceo",
                    "claims": [
                        {"claim_id": "claim_1", "value": "John Smith", "confidence": 0.95, "source_id": "src_1"}
                    ]
                }
            ]),
            MockQueryResult([{"conflict_count": 0}]),
        ]

        truth_value = await truth_layer.get_truth_value("ent_123", "ceo")

        assert truth_value is not None
        assert truth_value.value == "John Smith"
        assert truth_value.confidence == 0.95

    @pytest.mark.asyncio
    async def test_get_truth_value_not_exists(self, truth_layer, mock_neo4j_client):
        """Test getting a truth value that doesn't exist."""
        mock_neo4j_client.execute_query.side_effect = [
            MockQueryResult([{"id": "ent_123", "name": "Acme Corp", "type": "ORGANIZATION"}]),
            MockQueryResult([]),  # No claims
            MockQueryResult([{"conflict_count": 0}]),
        ]

        truth_value = await truth_layer.get_truth_value("ent_123", "nonexistent_predicate")

        assert truth_value is None


class TestHistoricalTruth:
    """Tests for historical truth queries."""

    @pytest.mark.asyncio
    async def test_get_truth_at_time(self, truth_layer, mock_neo4j_client):
        """Test getting truth at a specific point in time."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([
            {
                "claim_id": "claim_historical",
                "value": "Jane Doe",
                "confidence": 0.85,
                "source_id": "src_old",
            }
        ])

        as_of = datetime(2023, 6, 15)
        historical = await truth_layer.get_truth_at_time("ent_123", "ceo", as_of)

        assert historical is not None
        assert historical.value == "Jane Doe"
        assert historical.as_of == as_of

    @pytest.mark.asyncio
    async def test_get_truth_at_time_not_found(self, truth_layer, mock_neo4j_client):
        """Test historical truth not found."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([])

        result = await truth_layer.get_truth_at_time("ent_123", "ceo", datetime(2020, 1, 1))

        assert result is None

    @pytest.mark.asyncio
    async def test_get_truth_history(self, truth_layer, mock_neo4j_client):
        """Test getting complete truth history."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([
            {
                "claim_id": "claim_3",
                "value": "1500",
                "status": "current",
                "confidence": 0.95,
                "created_at": "2024-01-01",
                "valid_from": "2024-01-01",
                "valid_until": None,
                "source_uri": "report_2024.pdf",
                "superseded_by": None,
            },
            {
                "claim_id": "claim_2",
                "value": "1200",
                "status": "superseded",
                "confidence": 0.9,
                "created_at": "2023-06-01",
                "valid_from": "2023-06-01",
                "valid_until": "2024-01-01",
                "source_uri": "report_2023.pdf",
                "superseded_by": "claim_3",
            },
        ])

        history = await truth_layer.get_truth_history("ent_123", "employee_count", limit=50)

        assert len(history) == 2
        assert history[0]["status"] == "current"
        assert history[1]["status"] == "superseded"


class TestTruthPropagation:
    """Tests for truth propagation after resolution."""

    @pytest.mark.asyncio
    async def test_propagate_resolution(self, truth_layer, mock_neo4j_client):
        """Test propagating a conflict resolution."""
        mock_neo4j_client.execute_query.side_effect = [
            # Get winning claim
            MockQueryResult([{
                "value": "John Smith",
                "confidence": 0.95,
                "source_id": "src_1",
            }]),
            # Update materialized truth
            MockQueryResult([{"version": 2}]),
        ]

        truth_value = await truth_layer.propagate_resolution(
            entity_id="ent_123",
            predicate="ceo",
            winning_claim_id="claim_winner",
        )

        assert truth_value.value == "John Smith"
        assert truth_value.confidence == 0.95
        assert "claim_winner" in truth_value.supporting_claim_ids

    @pytest.mark.asyncio
    async def test_propagate_resolution_claim_not_found(self, truth_layer, mock_neo4j_client):
        """Test propagation with invalid claim ID."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([])

        with pytest.raises(ValueError, match="not found"):
            await truth_layer.propagate_resolution(
                entity_id="ent_123",
                predicate="ceo",
                winning_claim_id="nonexistent",
            )


class TestCacheManagement:
    """Tests for cache management."""

    def test_invalidate_cache_specific_entity(self, truth_layer):
        """Test invalidating cache for specific entity."""
        # Add something to cache
        truth_layer._cache["ent_123"] = (MagicMock(), datetime.utcnow())
        truth_layer._cache["ent_456"] = (MagicMock(), datetime.utcnow())

        truth_layer.invalidate_cache("ent_123")

        assert "ent_123" not in truth_layer._cache
        assert "ent_456" in truth_layer._cache

    def test_invalidate_cache_all(self, truth_layer):
        """Test invalidating entire cache."""
        truth_layer._cache["ent_123"] = (MagicMock(), datetime.utcnow())
        truth_layer._cache["ent_456"] = (MagicMock(), datetime.utcnow())

        truth_layer.invalidate_cache()  # No entity_id = clear all

        assert len(truth_layer._cache) == 0

    @pytest.mark.asyncio
    async def test_refresh_truth(self, truth_layer, mock_neo4j_client):
        """Test refreshing truth (invalidate + recompute)."""
        mock_neo4j_client.execute_query.side_effect = [
            MockQueryResult([{"id": "ent_123", "name": "Acme Corp", "type": "ORGANIZATION"}]),
            MockQueryResult([]),
            MockQueryResult([{"conflict_count": 0}]),
        ]

        # Add old cache entry
        old_truth = MagicMock()
        truth_layer._cache["ent_123"] = (old_truth, datetime.utcnow() - timedelta(hours=1))

        # Refresh
        new_truth = await truth_layer.refresh_truth("ent_123")

        # Should have fetched fresh data
        assert new_truth is not old_truth


class TestTruthComputation:
    """Tests for truth computation logic."""

    def test_compute_truth_value_single_value_predicate(self, truth_layer):
        """Test computing truth for single-value predicate."""
        claims = [
            {"claim_id": "claim_1", "value": "John Smith", "confidence": 0.95, "source_id": "src_1", "created_at": "2024-01-01"},
            {"claim_id": "claim_2", "value": "Jane Doe", "confidence": 0.8, "source_id": "src_2", "created_at": "2023-06-01"},
        ]

        truth_value = truth_layer._compute_truth_value("ent_123", "ceo", claims)

        # Should pick highest confidence
        assert truth_value.value == "John Smith"
        assert truth_value.confidence == 0.95

    def test_compute_truth_value_multi_value_predicate(self, truth_layer):
        """Test computing truth for multi-value predicate (not in single_value list)."""
        claims = [
            {"claim_id": "claim_1", "value": "Product A", "confidence": 0.9, "source_id": "src_1"},
            {"claim_id": "claim_2", "value": "Product B", "confidence": 0.8, "source_id": "src_2"},
        ]

        truth_value = truth_layer._compute_truth_value("ent_123", "products", claims)

        # Should aggregate all values
        assert isinstance(truth_value.value, list)
        assert "Product A" in truth_value.value
        assert "Product B" in truth_value.value
        assert len(truth_value.supporting_claim_ids) == 2

    def test_compute_truth_value_empty_claims(self, truth_layer):
        """Test computing truth with no claims."""
        result = truth_layer._compute_truth_value("ent_123", "ceo", [])
        assert result is None


class TestTruthValue:
    """Tests for TruthValue dataclass."""

    def test_to_dict(self):
        """Test serialization to dictionary."""
        truth_value = TruthValue(
            entity_id="ent_123",
            predicate="ceo",
            value="John Smith",
            confidence=0.95,
            supporting_claim_ids=["claim_1", "claim_2"],
            source_count=2,
            version=3,
        )

        data = truth_value.to_dict()

        assert data["entity_id"] == "ent_123"
        assert data["predicate"] == "ceo"
        assert data["value"] == "John Smith"
        assert data["confidence"] == 0.95
        assert len(data["supporting_claim_ids"]) == 2

    def test_from_dict(self):
        """Test deserialization from dictionary."""
        data = {
            "entity_id": "ent_456",
            "predicate": "headquarters",
            "value": "San Francisco",
            "confidence": 0.9,
            "supporting_claim_ids": ["claim_hq"],
            "source_count": 1,
            "last_updated": "2024-01-15T12:00:00",
            "version": 1,
            "metadata": {},
        }

        truth_value = TruthValue.from_dict(data)

        assert truth_value.entity_id == "ent_456"
        assert truth_value.value == "San Francisco"


class TestEntityTruth:
    """Tests for EntityTruth dataclass."""

    def test_to_dict(self):
        """Test serialization to dictionary."""
        ceo_truth = TruthValue(
            entity_id="ent_123",
            predicate="ceo",
            value="John Smith",
            confidence=0.95,
        )

        entity_truth = EntityTruth(
            entity_id="ent_123",
            entity_name="Acme Corp",
            entity_type="ORGANIZATION",
            truths={"ceo": ceo_truth},
            has_conflicts=False,
            conflict_count=0,
        )

        data = entity_truth.to_dict()

        assert data["entity_id"] == "ent_123"
        assert "ceo" in data["truths"]
        assert data["has_conflicts"] is False

    def test_get_value(self):
        """Test getting value for predicate."""
        ceo_truth = TruthValue(
            entity_id="ent_123",
            predicate="ceo",
            value="John Smith",
            confidence=0.95,
        )

        entity_truth = EntityTruth(
            entity_id="ent_123",
            entity_name="Acme Corp",
            entity_type="ORGANIZATION",
            truths={"ceo": ceo_truth},
        )

        assert entity_truth.get_value("ceo") == "John Smith"
        assert entity_truth.get_value("nonexistent") is None

    def test_get_confidence(self):
        """Test getting confidence for predicate."""
        ceo_truth = TruthValue(
            entity_id="ent_123",
            predicate="ceo",
            value="John Smith",
            confidence=0.95,
        )

        entity_truth = EntityTruth(
            entity_id="ent_123",
            entity_name="Acme Corp",
            entity_type="ORGANIZATION",
            truths={"ceo": ceo_truth},
        )

        assert entity_truth.get_confidence("ceo") == 0.95
        assert entity_truth.get_confidence("nonexistent") == 0.0


class TestHistoricalTruthDataclass:
    """Tests for HistoricalTruth dataclass."""

    def test_to_dict(self):
        """Test serialization to dictionary."""
        historical = HistoricalTruth(
            entity_id="ent_123",
            predicate="ceo",
            value="Jane Doe",
            as_of=datetime(2023, 6, 15),
            claim_id="claim_old",
            source_id="src_old",
            confidence=0.85,
        )

        data = historical.to_dict()

        assert data["entity_id"] == "ent_123"
        assert data["value"] == "Jane Doe"
        assert data["claim_id"] == "claim_old"


class TestPydanticModels:
    """Tests for Pydantic request/response models."""

    def test_truth_query_request(self):
        """Test TruthQueryRequest model."""
        request = TruthQueryRequest(
            entity_id="ent_123",
            predicate="ceo",
        )
        assert request.entity_id == "ent_123"
        assert request.predicate == "ceo"

    def test_truth_response(self):
        """Test TruthResponse model."""
        response = TruthResponse(
            entity_id="ent_123",
            entity_name="Acme Corp",
            truths={"ceo": {"value": "John Smith", "confidence": 0.95}},
            has_conflicts=False,
            conflict_count=0,
        )
        assert response.entity_id == "ent_123"
        assert "ceo" in response.truths

    def test_history_response(self):
        """Test HistoryResponse model."""
        response = HistoryResponse(
            entity_id="ent_123",
            predicate="employee_count",
            history=[
                {"claim_id": "claim_1", "value": "1500", "status": "current"},
            ],
            total_entries=1,
        )
        assert response.predicate == "employee_count"
        assert response.total_entries == 1


class TestMaterializeAllTruths:
    """Tests for truth materialization."""

    @pytest.mark.asyncio
    async def test_materialize_all_truths(self, truth_layer, mock_neo4j_client):
        """Test materializing truths for all entities."""
        mock_neo4j_client.execute_query.side_effect = [
            # Get all entities
            MockQueryResult([
                {"entity_id": "ent_1"},
                {"entity_id": "ent_2"},
            ]),
            # Entity 1 queries
            MockQueryResult([{"id": "ent_1", "name": "Acme", "type": "ORGANIZATION"}]),
            MockQueryResult([{"predicate": "ceo", "claims": [{"claim_id": "c1", "value": "John", "confidence": 0.9, "source_id": "s1"}]}]),
            MockQueryResult([{"conflict_count": 0}]),
            MockQueryResult([{"version": 1}]),  # Materialize
            # Entity 2 queries
            MockQueryResult([{"id": "ent_2", "name": "Beta", "type": "ORGANIZATION"}]),
            MockQueryResult([{"predicate": "ceo", "claims": [{"claim_id": "c2", "value": "Jane", "confidence": 0.85, "source_id": "s2"}]}]),
            MockQueryResult([{"conflict_count": 0}]),
            MockQueryResult([{"version": 1}]),  # Materialize
        ]

        count = await truth_layer.materialize_all_truths(limit=10)

        assert count == 2  # Two truth values materialized
