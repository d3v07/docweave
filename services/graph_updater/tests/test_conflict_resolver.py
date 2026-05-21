"""
Tests for Conflict Resolver
============================

Unit tests for the ConflictResolver class covering:
- Resolution strategies (timestamp, confidence, reliability, voting, weighted)
- Manual resolution
- Audit trail management
- Score calculation
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

# Import from conftest's path setup and mocking
from conflict_resolver import (
    ConflictResolver,
    ConflictResolverConfig,
    ResolutionStrategy,
    ResolutionOutcome,
    ResolutionResult,
    ResolutionAuditEntry,
    ManualResolutionRequest,
    AutoResolutionRequest,
    ResolutionResponse,
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
    """Create resolver configuration."""
    return ConflictResolverConfig(
        default_strategy=ResolutionStrategy.WEIGHTED_COMBINATION,
        timestamp_weight=0.2,
        confidence_weight=0.4,
        source_reliability_weight=0.3,
        source_count_weight=0.1,
        auto_resolve_confidence_threshold=0.7,
        manual_review_threshold=0.5,
        score_difference_threshold=0.1,
    )


@pytest.fixture
def resolver(mock_neo4j_client, config):
    """Create ConflictResolver with mock client."""
    return ConflictResolver(mock_neo4j_client, config)


class TestResolutionStrategies:
    """Tests for different resolution strategies."""

    @pytest.mark.asyncio
    async def test_resolve_by_confidence(self, resolver, mock_neo4j_client):
        """Test resolution using confidence-based strategy."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([])

        claim_data = [
            {
                "id": "claim_1",
                "confidence": 0.6,
                "source_reliability": 0.7,
                "timestamp": datetime.utcnow().isoformat(),
                "object_value": "100",
            },
            {
                "id": "claim_2",
                "confidence": 0.95,
                "source_reliability": 0.8,
                "timestamp": datetime.utcnow().isoformat(),
                "object_value": "200",
            },
        ]

        result = await resolver.resolve_conflict(
            conflict_id="conflict_123",
            claim_data=claim_data,
            strategy=ResolutionStrategy.CONFIDENCE_BASED,
            force_resolution=True,
        )

        assert result.winning_claim_id == "claim_2"
        assert result.strategy_used == ResolutionStrategy.CONFIDENCE_BASED

    @pytest.mark.asyncio
    async def test_resolve_by_timestamp(self, resolver, mock_neo4j_client):
        """Test resolution using timestamp-based strategy."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([])

        old_time = (datetime.utcnow() - timedelta(days=365)).isoformat()
        new_time = datetime.utcnow().isoformat()

        claim_data = [
            {
                "id": "claim_old",
                "confidence": 0.9,
                "timestamp": old_time,
                "object_value": "old_value",
            },
            {
                "id": "claim_new",
                "confidence": 0.8,
                "timestamp": new_time,
                "object_value": "new_value",
            },
        ]

        result = await resolver.resolve_conflict(
            conflict_id="conflict_123",
            claim_data=claim_data,
            strategy=ResolutionStrategy.TIMESTAMP_BASED,
            force_resolution=True,
        )

        assert result.winning_claim_id == "claim_new"
        assert result.strategy_used == ResolutionStrategy.TIMESTAMP_BASED

    @pytest.mark.asyncio
    async def test_resolve_by_source_reliability(self, resolver, mock_neo4j_client):
        """Test resolution using source reliability strategy."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([])

        claim_data = [
            {
                "id": "claim_1",
                "confidence": 0.9,
                "source_reliability": 0.5,
                "timestamp": datetime.utcnow().isoformat(),
                "object_value": "value1",
            },
            {
                "id": "claim_2",
                "confidence": 0.8,
                "source_reliability": 0.95,
                "timestamp": datetime.utcnow().isoformat(),
                "object_value": "value2",
            },
        ]

        result = await resolver.resolve_conflict(
            conflict_id="conflict_123",
            claim_data=claim_data,
            strategy=ResolutionStrategy.SOURCE_RELIABILITY,
            force_resolution=True,
        )

        assert result.winning_claim_id == "claim_2"
        assert result.strategy_used == ResolutionStrategy.SOURCE_RELIABILITY

    @pytest.mark.asyncio
    async def test_resolve_by_voting(self, resolver, mock_neo4j_client):
        """Test resolution using multi-source voting strategy."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([])

        claim_data = [
            {"id": "claim_1", "confidence": 0.8, "object_value": "A", "timestamp": datetime.utcnow().isoformat()},
            {"id": "claim_2", "confidence": 0.8, "object_value": "A", "timestamp": datetime.utcnow().isoformat()},
            {"id": "claim_3", "confidence": 0.8, "object_value": "A", "timestamp": datetime.utcnow().isoformat()},
            {"id": "claim_4", "confidence": 0.9, "object_value": "B", "timestamp": datetime.utcnow().isoformat()},
        ]

        result = await resolver.resolve_conflict(
            conflict_id="conflict_123",
            claim_data=claim_data,
            strategy=ResolutionStrategy.MULTI_SOURCE_VOTING,
            force_resolution=True,
        )

        # Claims 1-3 all say "A", so one of them should win
        assert result.winning_claim_id in ["claim_1", "claim_2", "claim_3"]

    @pytest.mark.asyncio
    async def test_resolve_weighted_combination(self, resolver, mock_neo4j_client):
        """Test resolution using weighted combination strategy."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([])

        claim_data = [
            {
                "id": "claim_1",
                "confidence": 0.9,
                "source_reliability": 0.9,
                "timestamp": datetime.utcnow().isoformat(),
                "object_value": "value1",
            },
            {
                "id": "claim_2",
                "confidence": 0.5,
                "source_reliability": 0.5,
                "timestamp": (datetime.utcnow() - timedelta(days=300)).isoformat(),
                "object_value": "value2",
            },
        ]

        result = await resolver.resolve_conflict(
            conflict_id="conflict_123",
            claim_data=claim_data,
            strategy=ResolutionStrategy.WEIGHTED_COMBINATION,
            force_resolution=True,
        )

        assert result.winning_claim_id == "claim_1"
        assert result.outcome == ResolutionOutcome.WINNER_SELECTED

    @pytest.mark.asyncio
    async def test_manual_review_strategy(self, resolver, mock_neo4j_client):
        """Test that manual review strategy flags for review."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([])

        claim_data = [
            {"id": "claim_1", "confidence": 0.9, "object_value": "A", "timestamp": datetime.utcnow().isoformat()},
            {"id": "claim_2", "confidence": 0.85, "object_value": "B", "timestamp": datetime.utcnow().isoformat()},
        ]

        result = await resolver.resolve_conflict(
            conflict_id="conflict_123",
            claim_data=claim_data,
            strategy=ResolutionStrategy.MANUAL_REVIEW,
        )

        assert result.outcome == ResolutionOutcome.FLAGGED_FOR_REVIEW
        assert result.winning_claim_id is None


class TestManualResolution:
    """Tests for manual resolution."""

    @pytest.mark.asyncio
    async def test_manual_resolution_success(self, resolver, mock_neo4j_client):
        """Test successful manual resolution."""
        # Mock getting conflict details
        mock_neo4j_client.execute_query.side_effect = [
            MockQueryResult([{
                "conflict_id": "conflict_123",
                "claim1_id": "claim_a",
                "claim2_id": "claim_b",
                "status": "UNRESOLVED",
            }]),
            MockQueryResult([]),  # Store audit entry
            MockQueryResult([]),  # Mark losing claim
            MockQueryResult([]),  # Update conflict status
            MockQueryResult([]),  # Confirm winner
        ]

        result = await resolver.resolve_by_manual_selection(
            conflict_id="conflict_123",
            winning_claim_id="claim_a",
            reason="Manual review determined claim_a has more accurate data",
            resolved_by="admin_user",
        )

        assert result.outcome == ResolutionOutcome.WINNER_SELECTED
        assert result.winning_claim_id == "claim_a"
        assert "claim_b" in result.losing_claim_ids
        assert result.confidence == 1.0  # Manual decisions have full confidence

    @pytest.mark.asyncio
    async def test_manual_resolution_conflict_not_found(self, resolver, mock_neo4j_client):
        """Test manual resolution with invalid conflict ID."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([])

        with pytest.raises(ValueError, match="not found"):
            await resolver.resolve_by_manual_selection(
                conflict_id="nonexistent",
                winning_claim_id="claim_a",
                reason="Test reason for resolution",
                resolved_by="admin",
            )

    @pytest.mark.asyncio
    async def test_manual_resolution_invalid_winner(self, resolver, mock_neo4j_client):
        """Test manual resolution with claim not in conflict."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([{
            "conflict_id": "conflict_123",
            "claim1_id": "claim_a",
            "claim2_id": "claim_b",
        }])

        with pytest.raises(ValueError, match="not part of conflict"):
            await resolver.resolve_by_manual_selection(
                conflict_id="conflict_123",
                winning_claim_id="claim_c",  # Not in conflict
                reason="Test reason for resolution",
                resolved_by="admin",
            )


class TestAuditTrail:
    """Tests for audit trail functionality."""

    @pytest.mark.asyncio
    async def test_audit_entry_created_on_resolution(self, resolver, mock_neo4j_client):
        """Test that audit entries are created during resolution."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([])

        claim_data = [
            {"id": "claim_1", "confidence": 0.95, "object_value": "A", "timestamp": datetime.utcnow().isoformat()},
            {"id": "claim_2", "confidence": 0.5, "object_value": "B", "timestamp": datetime.utcnow().isoformat()},
        ]

        result = await resolver.resolve_conflict(
            conflict_id="conflict_123",
            claim_data=claim_data,
            strategy=ResolutionStrategy.CONFIDENCE_BASED,
            force_resolution=True,
        )

        assert result.audit_entry is not None
        assert result.audit_entry.conflict_id == "conflict_123"
        assert result.audit_entry.strategy_used == ResolutionStrategy.CONFIDENCE_BASED
        assert result.audit_entry.id.startswith("audit_")

    @pytest.mark.asyncio
    async def test_get_audit_trail(self, resolver, mock_neo4j_client):
        """Test retrieving audit trail."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([
            {
                "a": {
                    "id": "audit_1",
                    "conflict_id": "conflict_123",
                    "timestamp": datetime.utcnow().isoformat(),
                    "strategy_used": "confidence_based",
                    "outcome": "winner_selected",
                    "winning_claim_id": "claim_a",
                    "losing_claim_ids": ["claim_b"],
                    "resolved_by": "system",
                    "reason": "Test reason",
                    "confidence": 0.95,
                    "scores": {"claim_a": 0.95, "claim_b": 0.5},
                }
            }
        ])

        entries = await resolver.get_audit_trail(conflict_id="conflict_123")

        assert len(entries) == 1
        assert entries[0].conflict_id == "conflict_123"


class TestResolutionAuditEntry:
    """Tests for ResolutionAuditEntry dataclass."""

    def test_to_dict(self):
        """Test serialization to dictionary."""
        entry = ResolutionAuditEntry(
            id="audit_123",
            conflict_id="conflict_456",
            timestamp=datetime(2024, 1, 15, 12, 0, 0),
            strategy_used=ResolutionStrategy.CONFIDENCE_BASED,
            outcome=ResolutionOutcome.WINNER_SELECTED,
            winning_claim_id="claim_winner",
            losing_claim_ids=["claim_loser1", "claim_loser2"],
            resolved_by="system",
            reason="Higher confidence selected",
            confidence=0.95,
            scores={"claim_winner": 0.95, "claim_loser1": 0.5, "claim_loser2": 0.4},
        )

        data = entry.to_dict()

        assert data["id"] == "audit_123"
        assert data["strategy_used"] == "confidence_based"
        assert data["outcome"] == "winner_selected"
        assert data["winning_claim_id"] == "claim_winner"
        assert len(data["losing_claim_ids"]) == 2

    def test_from_dict(self):
        """Test deserialization from dictionary."""
        data = {
            "id": "audit_789",
            "conflict_id": "conflict_xyz",
            "timestamp": "2024-01-15T12:00:00",
            "strategy_used": "source_reliability",
            "outcome": "flagged_for_review",
            "winning_claim_id": None,
            "losing_claim_ids": [],
            "resolved_by": "admin",
            "reason": "Needs human review",
            "confidence": 0.3,
            "scores": {},
        }

        entry = ResolutionAuditEntry.from_dict(data)

        assert entry.id == "audit_789"
        assert entry.strategy_used == ResolutionStrategy.SOURCE_RELIABILITY
        assert entry.outcome == ResolutionOutcome.FLAGGED_FOR_REVIEW


class TestResolutionResult:
    """Tests for ResolutionResult dataclass."""

    def test_to_dict(self):
        """Test serialization to dictionary."""
        audit_entry = ResolutionAuditEntry(
            id="audit_123",
            conflict_id="conflict_456",
            timestamp=datetime.utcnow(),
            strategy_used=ResolutionStrategy.WEIGHTED_COMBINATION,
            outcome=ResolutionOutcome.WINNER_SELECTED,
            winning_claim_id="claim_winner",
            losing_claim_ids=["claim_loser"],
            resolved_by="system",
            reason="Weighted combination",
            confidence=0.85,
            scores={},
        )

        result = ResolutionResult(
            conflict_id="conflict_456",
            outcome=ResolutionOutcome.WINNER_SELECTED,
            winning_claim_id="claim_winner",
            losing_claim_ids=["claim_loser"],
            strategy_used=ResolutionStrategy.WEIGHTED_COMBINATION,
            reason="Weighted combination",
            confidence=0.85,
            audit_entry=audit_entry,
            actions_taken=["Marked claim_loser as superseded"],
        )

        data = result.to_dict()

        assert data["conflict_id"] == "conflict_456"
        assert data["outcome"] == "winner_selected"
        assert len(data["actions_taken"]) == 1


class TestScoreCalculation:
    """Tests for score calculation methods."""

    def test_score_by_timestamp_recent(self, resolver):
        """Test timestamp scoring for recent claim."""
        claim = {
            "timestamp": datetime.utcnow().isoformat(),
        }
        score = resolver._score_by_timestamp(claim)
        assert score > 0.9  # Recent should score high

    def test_score_by_timestamp_old(self, resolver):
        """Test timestamp scoring for old claim."""
        old_time = (datetime.utcnow() - timedelta(days=365)).isoformat()
        claim = {"timestamp": old_time}
        score = resolver._score_by_timestamp(claim)
        assert score < 0.1  # Old should score low

    def test_score_by_confidence(self, resolver):
        """Test confidence scoring."""
        claim = {"confidence": 0.85}
        score = resolver._score_by_confidence(claim)
        assert score == 0.85

    def test_score_by_source_reliability(self, resolver):
        """Test source reliability scoring."""
        claim = {"source_reliability": 0.9}
        score = resolver._score_by_source_reliability(claim)
        assert score == 0.9


class TestLowConfidenceResolution:
    """Tests for handling low confidence scenarios."""

    @pytest.mark.asyncio
    async def test_deferred_resolution_low_confidence(self, resolver, mock_neo4j_client):
        """Test that low confidence results in deferred resolution."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([])

        claim_data = [
            {"id": "claim_1", "confidence": 0.5, "object_value": "A", "timestamp": datetime.utcnow().isoformat()},
            {"id": "claim_2", "confidence": 0.49, "object_value": "B", "timestamp": datetime.utcnow().isoformat()},
        ]

        result = await resolver.resolve_conflict(
            conflict_id="conflict_123",
            claim_data=claim_data,
            strategy=ResolutionStrategy.CONFIDENCE_BASED,
            force_resolution=False,  # Don't force
        )

        # Score difference is too small for auto-resolve
        assert result.outcome in [ResolutionOutcome.DEFERRED, ResolutionOutcome.FLAGGED_FOR_REVIEW]

    @pytest.mark.asyncio
    async def test_empty_claims_no_resolution(self, resolver, mock_neo4j_client):
        """Test handling of empty claims list."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([])

        result = await resolver.resolve_conflict(
            conflict_id="conflict_123",
            claim_data=[],
            strategy=ResolutionStrategy.CONFIDENCE_BASED,
        )

        assert result.outcome == ResolutionOutcome.NO_RESOLUTION


class TestPydanticModels:
    """Tests for Pydantic request/response models."""

    def test_manual_resolution_request_validation(self):
        """Test ManualResolutionRequest validation."""
        request = ManualResolutionRequest(
            winning_claim_id="claim_123",
            reason="This is a valid reason with more than 10 characters",
            resolved_by="admin_user",
        )
        assert request.winning_claim_id == "claim_123"

    def test_auto_resolution_request_defaults(self):
        """Test AutoResolutionRequest default values."""
        request = AutoResolutionRequest()
        assert request.strategy is None
        assert request.force_resolution is False

    def test_resolution_response(self):
        """Test ResolutionResponse model."""
        response = ResolutionResponse(
            conflict_id="conflict_123",
            outcome="winner_selected",
            winning_claim_id="claim_a",
            losing_claim_ids=["claim_b"],
            strategy_used="confidence_based",
            reason="Higher confidence",
            confidence=0.95,
            actions_taken=["Superseded claim_b"],
        )
        assert response.conflict_id == "conflict_123"
        assert response.outcome == "winner_selected"
