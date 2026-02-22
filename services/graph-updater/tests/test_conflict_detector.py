"""
Tests for Conflict Detector
============================

Unit tests for the ConflictDetector class covering:
- Value mismatch detection
- Temporal conflict detection
- Semantic conflict detection
- Conflict severity assessment
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

# Import from conftest's path setup and mocking
from conflict_detector import (
    ConflictDetector,
    ConflictDetectorConfig,
    ConflictType,
    ConflictSeverity,
    DetectedConflict,
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
    """Create detector configuration."""
    return ConflictDetectorConfig(
        enable_value_mismatch=True,
        enable_temporal=True,
        enable_semantic=True,
        value_similarity_threshold=0.85,
        min_confidence_threshold=0.3,
    )


@pytest.fixture
def detector(mock_neo4j_client, config):
    """Create ConflictDetector with mock client."""
    return ConflictDetector(mock_neo4j_client, config)


class TestValueMismatchDetection:
    """Tests for value mismatch conflict detection."""

    @pytest.mark.asyncio
    async def test_detect_value_mismatch_conflict(self, detector, mock_neo4j_client):
        """Test detecting value mismatch between claims."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([
            {"id": "claim_existing", "object_value": "1000", "confidence": 0.8,
             "valid_from": None, "valid_until": None, "source_id": "src_1",
             "source_reliability": 0.7}
        ])

        claim_data = {
            "id": "claim_new",
            "subject_entity_id": "ent_company",
            "predicate": "employee_count",
            "object_value": "1500",
            "confidence": 0.9,
            "source_id": "src_2",
        }

        conflicts = await detector.detect_conflicts_for_claim(claim_data)

        assert len(conflicts) == 1
        assert conflicts[0].conflict_type == ConflictType.VALUE_MISMATCH

    @pytest.mark.asyncio
    async def test_no_conflict_for_same_value(self, detector, mock_neo4j_client):
        """Test no conflict when values are the same."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([
            {"id": "claim_existing", "object_value": "1000", "confidence": 0.8,
             "valid_from": None, "valid_until": None, "source_id": "src_1",
             "source_reliability": 0.7}
        ])

        claim_data = {
            "id": "claim_new",
            "subject_entity_id": "ent_company",
            "predicate": "employee_count",
            "object_value": "1000",  # Same value
            "confidence": 0.9,
            "source_id": "src_2",
        }

        conflicts = await detector.detect_conflicts_for_claim(claim_data)

        # No value mismatch conflicts (values are same)
        value_mismatches = [c for c in conflicts if c.conflict_type == ConflictType.VALUE_MISMATCH]
        assert len(value_mismatches) == 0

    @pytest.mark.asyncio
    async def test_no_conflict_for_similar_values(self, detector, mock_neo4j_client):
        """Test no conflict when values are similar enough."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([
            {"id": "claim_existing", "object_value": "john smith", "confidence": 0.8,
             "valid_from": None, "valid_until": None, "source_id": "src_1",
             "source_reliability": 0.7}
        ])

        claim_data = {
            "id": "claim_new",
            "subject_entity_id": "ent_person",
            "predicate": "name",
            "object_value": "John Smith",  # Similar (case difference)
            "confidence": 0.9,
            "source_id": "src_2",
        }

        conflicts = await detector.detect_conflicts_for_claim(claim_data)

        # No value mismatch conflicts (values are similar)
        value_mismatches = [c for c in conflicts if c.conflict_type == ConflictType.VALUE_MISMATCH]
        assert len(value_mismatches) == 0

    @pytest.mark.asyncio
    async def test_skip_low_confidence_claims(self, detector, mock_neo4j_client):
        """Test that low confidence claims are skipped."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([
            {"id": "claim_existing", "object_value": "1000", "confidence": 0.2,  # Low
             "valid_from": None, "valid_until": None, "source_id": "src_1",
             "source_reliability": 0.7}
        ])

        claim_data = {
            "id": "claim_new",
            "subject_entity_id": "ent_company",
            "predicate": "employee_count",
            "object_value": "1500",
            "confidence": 0.2,  # Both low confidence
            "source_id": "src_2",
        }

        conflicts = await detector.detect_conflicts_for_claim(claim_data)

        value_mismatches = [c for c in conflicts if c.conflict_type == ConflictType.VALUE_MISMATCH]
        assert len(value_mismatches) == 0


class TestTemporalConflictDetection:
    """Tests for temporal conflict detection."""

    @pytest.mark.asyncio
    async def test_detect_temporal_overlap_conflict(self, detector, mock_neo4j_client):
        """Test detecting conflict with overlapping validity periods."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([
            {"id": "claim_existing", "object_value": "John Smith",
             "confidence": 0.8, "source_id": "src_1", "source_reliability": 0.7,
             "valid_from": "2023-01-01T00:00:00", "valid_until": "2024-12-31T00:00:00"}
        ])

        claim_data = {
            "id": "claim_new",
            "subject_entity_id": "ent_company",
            "predicate": "ceo",
            "object_value": "Jane Doe",  # Different value
            "confidence": 0.9,
            "source_id": "src_2",
            "valid_from": "2024-01-01T00:00:00",  # Overlaps
            "valid_until": "2025-12-31T00:00:00",
        }

        conflicts = await detector.detect_conflicts_for_claim(claim_data)

        # Should have both value mismatch and temporal conflict
        temporal = [c for c in conflicts if c.conflict_type == ConflictType.TEMPORAL]
        assert len(temporal) >= 1

    @pytest.mark.asyncio
    async def test_no_temporal_conflict_non_overlapping(self, detector, mock_neo4j_client):
        """Test no conflict when validity periods don't overlap."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([
            {"id": "claim_existing", "object_value": "John Smith",
             "confidence": 0.8, "source_id": "src_1", "source_reliability": 0.7,
             "valid_from": "2022-01-01T00:00:00", "valid_until": "2022-12-31T00:00:00"}
        ])

        claim_data = {
            "id": "claim_new",
            "subject_entity_id": "ent_company",
            "predicate": "ceo",
            "object_value": "Jane Doe",
            "confidence": 0.9,
            "source_id": "src_2",
            "valid_from": "2024-01-01T00:00:00",  # After existing
            "valid_until": "2025-12-31T00:00:00",
        }

        conflicts = await detector.detect_conflicts_for_claim(claim_data)

        temporal = [c for c in conflicts if c.conflict_type == ConflictType.TEMPORAL]
        assert len(temporal) == 0


class TestSemanticConflictDetection:
    """Tests for semantic conflict detection."""

    @pytest.mark.asyncio
    async def test_detect_semantic_contradiction(self, detector, mock_neo4j_client):
        """Test detecting semantic contradiction via predicate pairs."""
        # Mock for existing claims query and semantic check
        mock_neo4j_client.execute_query.side_effect = [
            # First call: get existing claims for same predicate
            MockQueryResult([]),
            # Second call: get claims with contradicting predicates
            MockQueryResult([
                {"id": "claim_existing", "predicate": "deceased",
                 "object_value": "true", "confidence": 0.95, "source_id": "src_1"}
            ])
        ]

        claim_data = {
            "id": "claim_new",
            "subject_entity_id": "ent_person",
            "predicate": "alive",  # Contradicts "deceased"
            "object_value": "true",
            "confidence": 0.9,
            "source_id": "src_2",
        }

        conflicts = await detector.detect_conflicts_for_claim(claim_data)

        semantic = [c for c in conflicts if c.conflict_type == ConflictType.SEMANTIC]
        assert len(semantic) >= 1


class TestDetectedConflict:
    """Tests for DetectedConflict dataclass."""

    def test_to_dict(self):
        """Test serialization to dictionary."""
        conflict = DetectedConflict(
            id="conflict_123",
            conflict_type=ConflictType.VALUE_MISMATCH,
            claim_ids=["claim_1", "claim_2"],
            subject_entity_id="ent_company",
            predicate="employee_count",
            values=["1000", "1500"],
            confidences=[0.8, 0.9],
            source_ids=["src_1", "src_2"],
            severity=ConflictSeverity.HIGH,
            score=0.95,
        )

        data = conflict.to_dict()

        assert data["id"] == "conflict_123"
        assert data["conflict_type"] == "value_mismatch"
        assert data["severity"] == "high"
        assert len(data["claim_ids"]) == 2

    def test_from_dict(self):
        """Test deserialization from dictionary."""
        data = {
            "id": "conflict_456",
            "conflict_type": "temporal",
            "claim_ids": ["claim_a", "claim_b"],
            "subject_entity_id": "ent_person",
            "predicate": "employment_status",
            "values": ["employed", "unemployed"],
            "confidences": [0.85, 0.9],
            "source_ids": ["src_a"],
            "severity": "medium",
            "score": 0.7,
            "detected_at": "2024-01-15T12:00:00",
        }

        conflict = DetectedConflict.from_dict(data)

        assert conflict.id == "conflict_456"
        assert conflict.conflict_type == ConflictType.TEMPORAL
        assert conflict.severity == ConflictSeverity.MEDIUM


class TestDetectAllConflicts:
    """Tests for scanning all conflicts in the graph."""

    @pytest.mark.asyncio
    async def test_detect_all_conflicts(self, detector, mock_neo4j_client):
        """Test scanning for all conflicts."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([
            {
                "entity_id": "ent_1",
                "claim1_id": "claim_a", "value1": "100", "conf1": 0.8, "source1_id": "src_1",
                "claim2_id": "claim_b", "value2": "200", "conf2": 0.9, "source2_id": "src_2",
                "predicate": "revenue",
            },
            {
                "entity_id": "ent_2",
                "claim1_id": "claim_c", "value1": "John", "conf1": 0.7, "source1_id": "src_3",
                "claim2_id": "claim_d", "value2": "Jane", "conf2": 0.85, "source2_id": "src_4",
                "predicate": "ceo",
            },
        ])

        conflicts = await detector.detect_all_conflicts(limit=10)

        assert len(conflicts) == 2
        assert all(c.conflict_type == ConflictType.VALUE_MISMATCH for c in conflicts)

    @pytest.mark.asyncio
    async def test_detect_conflicts_filtered_by_entity(self, detector, mock_neo4j_client):
        """Test filtering conflicts by entity."""
        mock_neo4j_client.execute_query.return_value = MockQueryResult([
            {
                "entity_id": "ent_specific",
                "claim1_id": "claim_x", "value1": "A", "conf1": 0.9, "source1_id": "src_x",
                "claim2_id": "claim_y", "value2": "B", "conf2": 0.85, "source2_id": "src_y",
                "predicate": "attribute",
            },
        ])

        conflicts = await detector.detect_all_conflicts(entity_id="ent_specific")

        assert len(conflicts) == 1
        assert conflicts[0].subject_entity_id == "ent_specific"


class TestValueSimilarity:
    """Tests for value similarity calculation."""

    def test_identical_values(self, detector):
        """Test similarity of identical values."""
        similarity = detector._calculate_value_similarity("test", "test")
        assert similarity == 1.0

    def test_completely_different_values(self, detector):
        """Test similarity of completely different values."""
        similarity = detector._calculate_value_similarity("abc", "xyz")
        assert similarity < 0.5

    def test_numeric_similarity(self, detector):
        """Test similarity of numeric values."""
        # 1000 vs 1100 = 10% difference
        similarity = detector._calculate_value_similarity("1000", "1100")
        assert similarity > 0.8

    def test_empty_value(self, detector):
        """Test similarity with empty value."""
        similarity = detector._calculate_value_similarity("test", "")
        assert similarity == 0.0


class TestSeverityAssessment:
    """Tests for conflict severity assessment."""

    def test_high_impact_predicate_high_confidence(self, detector):
        """Test severity for high impact predicate with high confidence."""
        severity = detector._assess_severity("ceo", [0.9, 0.85])
        assert severity == ConflictSeverity.CRITICAL

    def test_high_impact_predicate_low_confidence(self, detector):
        """Test severity for high impact predicate with lower confidence."""
        severity = detector._assess_severity("revenue", [0.6, 0.7])
        assert severity == ConflictSeverity.HIGH

    def test_normal_predicate(self, detector):
        """Test severity for normal predicate."""
        severity = detector._assess_severity("description", [0.8, 0.75])
        assert severity == ConflictSeverity.MEDIUM

    def test_low_confidence_predicate(self, detector):
        """Test severity for low confidence claims."""
        severity = detector._assess_severity("description", [0.4, 0.5])
        assert severity == ConflictSeverity.LOW
