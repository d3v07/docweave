"""
Tests for Graph Operations
===========================

Unit tests for the GraphOperations class covering:
- Entity management
- Claim operations with versioning
- Conflict detection and creation
- Truth queries
"""

import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

# Import from conftest's path setup and mocking
from operations import (
    GraphOperations,
    EntityMergeRequest,
    EntityMergeResult,
    ClaimVersionInfo,
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
def graph_ops(mock_neo4j_client):
    """Create GraphOperations with mock client."""
    return GraphOperations(mock_neo4j_client)


class TestEntityOperations:
    """Tests for entity operations."""

    @pytest.mark.asyncio
    async def test_merge_entity_creates_new(self, graph_ops, mock_neo4j_client):
        """Test creating a new entity via merge."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([
            {"id": "ent_abc123"}
        ])

        entity_id = await graph_ops.merge_entity(
            name="Acme Corp",
            entity_type="ORGANIZATION",
            aliases=["ACME", "Acme Inc"],
        )

        assert entity_id.startswith("ent_")
        assert mock_neo4j_client.execute_query.called

    @pytest.mark.asyncio
    async def test_merge_entity_with_properties(self, graph_ops, mock_neo4j_client):
        """Test merging entity with additional properties."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([
            {"id": "ent_xyz789"}
        ])

        entity_id = await graph_ops.merge_entity(
            name="John Doe",
            entity_type="PERSON",
            properties={"title": "CEO", "department": "Executive"},
        )

        assert entity_id.startswith("ent_")

    @pytest.mark.asyncio
    async def test_merge_entities_success(self, graph_ops, mock_neo4j_client):
        """Test successful entity merge."""
        # Mock the various queries in merge_entities
        mock_neo4j_client.execute_query.side_effect = [
            # Alias collection
            MockQueryResult([
                {"name": "Acme Typo", "aliases": ["acme-typo"]},
                {"name": "Acme Old", "aliases": ["old-acme"]},
            ]),
            # Add aliases to target
            MockQueryResult([]),
            # Transfer claims
            MockQueryResult([{"count": 5}]),
            # Update relationships
            MockQueryResult([{"count": 2}]),
            # Mark source as merged
            MockQueryResult([]),
            # Update merged_from
            MockQueryResult([]),
        ]

        result = await graph_ops.merge_entities(
            target_entity_id="ent_target",
            source_entity_ids=["ent_source1", "ent_source2"],
            merge_aliases=True,
            merge_claims=True,
            resolved_by="test_user",
        )

        assert result.success
        assert result.target_entity_id == "ent_target"
        assert result.claims_transferred == 5
        assert result.relationships_updated == 2

    @pytest.mark.asyncio
    async def test_find_entity_by_alias(self, graph_ops, mock_neo4j_client):
        """Test finding entity by alias."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([
            {"id": "ent_found", "name": "Found Entity", "type": "ORGANIZATION", "aliases": ["alias1"]}
        ])

        result = await graph_ops.find_entity_by_alias("alias1")

        assert result is not None
        assert result["id"] == "ent_found"

    @pytest.mark.asyncio
    async def test_find_entity_by_alias_not_found(self, graph_ops, mock_neo4j_client):
        """Test finding entity by alias when not found."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([])

        result = await graph_ops.find_entity_by_alias("nonexistent")

        assert result is None


class TestClaimOperations:
    """Tests for claim operations."""

    @pytest.mark.asyncio
    async def test_add_claim_basic(self, graph_ops, mock_neo4j_client):
        """Test adding a basic claim."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([])

        claim_id = await graph_ops.add_claim(
            subject_entity_id="ent_123",
            predicate="employee_count",
            object_value="1000",
            source_id="src_456",
            confidence=0.9,
            extracted_text="The company has 1000 employees.",
        )

        assert claim_id.startswith("claim_")

    @pytest.mark.asyncio
    async def test_add_claim_with_validity(self, graph_ops, mock_neo4j_client):
        """Test adding a claim with validity period."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([])

        claim_id = await graph_ops.add_claim(
            subject_entity_id="ent_123",
            predicate="ceo",
            object_value="John Smith",
            source_id="src_456",
            confidence=0.85,
            valid_from=datetime(2023, 1, 1),
            valid_until=datetime(2024, 12, 31),
        )

        assert claim_id.startswith("claim_")

    @pytest.mark.asyncio
    async def test_add_claim_versioned_supersedes_existing(self, graph_ops, mock_neo4j_client):
        """Test adding versioned claim that supersedes existing."""
        # Mock finding existing claims
        mock_neo4j_client.execute_query.side_effect = [
            # Find existing claims
            MockQueryResult([{"id": "claim_old"}]),
            # Add new claim
            MockQueryResult([]),
            # Supersede old claims
            MockQueryResult([]),
            # Create supersedes relationship
            MockQueryResult([]),
        ]

        claim_id, superseded = await graph_ops.add_claim_versioned(
            subject_entity_id="ent_123",
            predicate="employee_count",
            object_value="1500",
            source_id="src_789",
            confidence=0.95,
            supersede_existing=True,
        )

        assert claim_id.startswith("claim_")
        assert "claim_old" in superseded

    @pytest.mark.asyncio
    async def test_add_claim_with_conflict_check(self, graph_ops, mock_neo4j_client):
        """Test adding claim with conflict detection."""
        # Mock finding conflicting claims
        mock_neo4j_client.execute_query.side_effect = [
            # Find conflicting claims
            MockQueryResult([
                {"id": "claim_conflict", "predicate": "ceo", "object_value": "Jane Doe",
                 "confidence": 0.8, "source_id": "src_old", "source_reliability": 0.7}
            ]),
            # Add new claim
            MockQueryResult([]),
            # Create conflict relationship
            MockQueryResult([]),
            # Update claim status
            MockQueryResult([]),
        ]

        claim_id, conflicts = await graph_ops.add_claim_with_conflict_check(
            subject_entity_id="ent_123",
            predicate="ceo",
            object_value="John Smith",
            source_id="src_new",
            confidence=0.9,
        )

        assert claim_id.startswith("claim_")
        assert len(conflicts) == 1
        assert conflicts[0]["id"] == "claim_conflict"

    @pytest.mark.asyncio
    async def test_get_claim_versions(self, graph_ops, mock_neo4j_client):
        """Test getting claim version history."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([
            {"id": "claim_v3", "status": "current", "value": "1500", "confidence": 0.95,
             "created_at": "2024-01-01T00:00:00", "superseded_by": None, "version": 3},
            {"id": "claim_v2", "status": "superseded", "value": "1200", "confidence": 0.9,
             "created_at": "2023-06-01T00:00:00", "superseded_by": "claim_v3", "version": 2},
            {"id": "claim_v1", "status": "superseded", "value": "1000", "confidence": 0.85,
             "created_at": "2023-01-01T00:00:00", "superseded_by": "claim_v2", "version": 1},
        ])

        versions = await graph_ops.get_claim_versions("ent_123", "employee_count")

        assert len(versions) == 3
        assert versions[0].claim_id == "claim_v3"
        assert versions[0].status == "current"


class TestConflictOperations:
    """Tests for conflict operations."""

    @pytest.mark.asyncio
    async def test_find_conflicting_claims(self, graph_ops, mock_neo4j_client):
        """Test finding conflicting claims."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([
            {"id": "claim_1", "predicate": "revenue", "object_value": "10B",
             "confidence": 0.8, "source_id": "src_1", "source_reliability": 0.9},
            {"id": "claim_2", "predicate": "revenue", "object_value": "12B",
             "confidence": 0.85, "source_id": "src_2", "source_reliability": 0.85},
        ])

        conflicts = await graph_ops.find_conflicting_claims(
            subject_entity_id="ent_company",
            predicate="revenue",
            object_value="11B",
        )

        assert len(conflicts) == 2

    @pytest.mark.asyncio
    async def test_get_unresolved_conflicts(self, graph_ops, mock_neo4j_client):
        """Test getting unresolved conflicts."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([
            {
                "conflict_id": "conflict_1", "conflict_type": "VALUE_MISMATCH",
                "claim1_id": "claim_a", "predicate": "ceo",
                "value1": "John", "value2": "Jane",
                "confidence1": 0.8, "confidence2": 0.9,
                "reliability1": 0.7, "reliability2": 0.85,
                "detected_at": "2024-01-15T00:00:00",
            },
        ])

        conflicts = await graph_ops.get_unresolved_conflicts(limit=10)

        assert len(conflicts) == 1
        assert conflicts[0]["conflict_id"] == "conflict_1"

    @pytest.mark.asyncio
    async def test_resolve_conflict(self, graph_ops, mock_neo4j_client):
        """Test resolving a conflict."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([])

        await graph_ops.resolve_conflict(
            conflict_id="conflict_123",
            winning_claim_id="claim_winner",
            resolution_reason="Higher confidence and more recent source",
            resolved_by="admin",
        )

        assert mock_neo4j_client.execute_query.called


class TestTruthOperations:
    """Tests for truth/current state operations."""

    @pytest.mark.asyncio
    async def test_get_current_truth(self, graph_ops, mock_neo4j_client):
        """Test getting current truth for an entity."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([
            {"predicate": "ceo", "value": "John Smith", "confidence": 0.95,
             "claim_id": "claim_1", "source_uri": "https://example.com/doc1",
             "created_at": "2024-01-01T00:00:00"},
            {"predicate": "employee_count", "value": "1500", "confidence": 0.9,
             "claim_id": "claim_2", "source_uri": "https://example.com/doc2",
             "created_at": "2024-01-15T00:00:00"},
        ])

        truths = await graph_ops.get_current_truth("ent_company")

        assert len(truths) == 2

    @pytest.mark.asyncio
    async def test_get_current_truth_for_predicate(self, graph_ops, mock_neo4j_client):
        """Test getting current truth for specific predicate."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([
            {"predicate": "ceo", "value": "John Smith", "confidence": 0.95,
             "claim_id": "claim_1", "source_uri": "https://example.com/doc1",
             "created_at": "2024-01-01T00:00:00"},
        ])

        truths = await graph_ops.get_current_truth("ent_company", predicate="ceo")

        assert len(truths) == 1
        assert truths[0]["predicate"] == "ceo"

    @pytest.mark.asyncio
    async def test_get_claim_history(self, graph_ops, mock_neo4j_client):
        """Test getting claim history."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([
            {"claim_id": "claim_3", "value": "1500", "status": "current",
             "confidence": 0.95, "created_at": "2024-01-01",
             "valid_from": "2024-01-01", "valid_until": None,
             "source_uri": "doc3.pdf", "source_reliability": 0.9,
             "supersedes_claim_id": "claim_2"},
            {"claim_id": "claim_2", "value": "1200", "status": "superseded",
             "confidence": 0.9, "created_at": "2023-06-01",
             "valid_from": "2023-06-01", "valid_until": "2024-01-01",
             "source_uri": "doc2.pdf", "source_reliability": 0.85,
             "supersedes_claim_id": "claim_1"},
        ])

        history = await graph_ops.get_claim_history(
            "ent_company", "employee_count", include_superseded=True
        )

        assert len(history) == 2
        assert history[0]["status"] == "current"
        assert history[1]["status"] == "superseded"


class TestSourceOperations:
    """Tests for source operations."""

    @pytest.mark.asyncio
    async def test_get_claims_by_source(self, graph_ops, mock_neo4j_client):
        """Test getting claims from a specific source."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([
            {"claim_id": "claim_1", "predicate": "ceo", "value": "John",
             "confidence": 0.9, "status": "current",
             "entity_id": "ent_1", "entity_name": "Acme Corp"},
            {"claim_id": "claim_2", "predicate": "founded", "value": "2010",
             "confidence": 0.95, "status": "current",
             "entity_id": "ent_1", "entity_name": "Acme Corp"},
        ])

        claims = await graph_ops.get_claims_by_source("src_annual_report")

        assert len(claims) == 2

    @pytest.mark.asyncio
    async def test_update_source_reliability(self, graph_ops, mock_neo4j_client):
        """Test updating source reliability score."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([])

        await graph_ops.update_source_reliability("src_123", 0.95)

        assert mock_neo4j_client.execute_query.called
