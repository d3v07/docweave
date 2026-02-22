"""
Conflict Detection Service
==========================

This module provides conflict detection capabilities for the DocWeave knowledge graph.
It identifies VALUE_MISMATCH, TEMPORAL, and SEMANTIC conflicts between claims.

Conflict Types:
    - VALUE_MISMATCH: Same subject+predicate with different object values
    - TEMPORAL: Overlapping validity periods with conflicting claims
    - SEMANTIC: Claims that are semantically contradictory
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class ConflictType(str, Enum):
    """Types of conflicts that can be detected between claims."""
    VALUE_MISMATCH = "value_mismatch"
    TEMPORAL = "temporal"
    SEMANTIC = "semantic"
    CARDINALITY = "cardinality"  # When predicate should have single value but has multiple


class ConflictSeverity(str, Enum):
    """Severity levels for detected conflicts."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class DetectedConflict:
    """
    Represents a detected conflict between two or more claims.

    Attributes:
        id: Unique identifier for this conflict
        conflict_type: Type of the detected conflict
        claim_ids: IDs of claims involved in the conflict
        subject_entity_id: ID of the entity the claims are about
        predicate: The predicate where the conflict exists
        values: The conflicting values
        confidences: Confidence scores of conflicting claims
        source_ids: Source IDs for each claim
        severity: Assessed severity of the conflict
        score: Conflict score (0-1) indicating certainty of conflict
        detected_at: When the conflict was detected
        metadata: Additional conflict metadata
    """
    id: str
    conflict_type: ConflictType
    claim_ids: list[str]
    subject_entity_id: str
    predicate: str
    values: list[Any]
    confidences: list[float] = field(default_factory=list)
    source_ids: list[str] = field(default_factory=list)
    severity: ConflictSeverity = ConflictSeverity.MEDIUM
    score: float = 1.0
    detected_at: datetime = field(default_factory=datetime.utcnow)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "id": self.id,
            "conflict_type": self.conflict_type.value,
            "claim_ids": self.claim_ids,
            "subject_entity_id": self.subject_entity_id,
            "predicate": self.predicate,
            "values": self.values,
            "confidences": self.confidences,
            "source_ids": self.source_ids,
            "severity": self.severity.value,
            "score": self.score,
            "detected_at": self.detected_at.isoformat(),
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DetectedConflict":
        """Create from dictionary."""
        return cls(
            id=data["id"],
            conflict_type=ConflictType(data["conflict_type"]),
            claim_ids=data["claim_ids"],
            subject_entity_id=data["subject_entity_id"],
            predicate=data["predicate"],
            values=data["values"],
            confidences=data.get("confidences", []),
            source_ids=data.get("source_ids", []),
            severity=ConflictSeverity(data.get("severity", "medium")),
            score=data.get("score", 1.0),
            detected_at=datetime.fromisoformat(data["detected_at"]) if isinstance(data.get("detected_at"), str) else datetime.utcnow(),
            metadata=data.get("metadata", {}),
        )


class ConflictDetectorConfig(BaseModel):
    """Configuration for the conflict detector."""
    # Value mismatch detection
    enable_value_mismatch: bool = Field(default=True)
    value_similarity_threshold: float = Field(default=0.85, ge=0.0, le=1.0)

    # Temporal conflict detection
    enable_temporal: bool = Field(default=True)
    temporal_overlap_threshold_days: int = Field(default=0, ge=0)

    # Semantic conflict detection
    enable_semantic: bool = Field(default=True)
    semantic_contradiction_threshold: float = Field(default=0.7, ge=0.0, le=1.0)

    # General settings
    min_confidence_threshold: float = Field(default=0.3, ge=0.0, le=1.0)
    confidence_difference_threshold: float = Field(default=0.1, ge=0.0, le=1.0)

    # Predicates that should have single values (cardinality = 1)
    single_value_predicates: list[str] = Field(default_factory=lambda: [
        "ceo", "founded_date", "headquarters", "employee_count",
        "annual_revenue", "stock_ticker", "date_of_birth"
    ])


class ConflictDetector:
    """
    Detects conflicts between claims in the knowledge graph.

    This service analyzes claims to identify:
    - Value mismatches (same subject+predicate, different values)
    - Temporal conflicts (overlapping validity periods)
    - Semantic contradictions (using NLI or semantic similarity)

    Example:
        detector = ConflictDetector(neo4j_client)
        conflicts = await detector.detect_conflicts_for_claim(new_claim)

        for conflict in conflicts:
            print(f"Conflict detected: {conflict.conflict_type}")
    """

    def __init__(
        self,
        neo4j_client,
        config: Optional[ConflictDetectorConfig] = None,
    ):
        """
        Initialize the conflict detector.

        Args:
            neo4j_client: Neo4j client for database queries
            config: Configuration for conflict detection
        """
        self.client = neo4j_client
        self.config = config or ConflictDetectorConfig()
        self._contradiction_pairs = self._load_contradiction_pairs()

    def _load_contradiction_pairs(self) -> dict[str, list[str]]:
        """Load predicate pairs that are known contradictions."""
        return {
            "alive": ["deceased", "dead"],
            "active": ["inactive", "retired", "defunct"],
            "operating": ["closed", "shutdown", "bankrupt"],
            "employed_at": ["former_employee_of"],
            "married_to": ["divorced_from"],
            "owns": ["sold", "divested"],
        }

    async def detect_conflicts_for_claim(
        self,
        claim_data: dict[str, Any],
    ) -> list[DetectedConflict]:
        """
        Detect all conflicts for a new claim against existing claims.

        Args:
            claim_data: The new claim to check for conflicts

        Returns:
            List of detected conflicts
        """
        conflicts = []

        subject_entity_id = claim_data.get("subject_entity_id")
        predicate = claim_data.get("predicate")
        object_value = claim_data.get("object_value")
        claim_id = claim_data.get("id")
        source_id = claim_data.get("source_id")
        confidence = claim_data.get("confidence", 0.8)
        valid_from = claim_data.get("valid_from")
        valid_until = claim_data.get("valid_until")

        if not subject_entity_id or not predicate:
            logger.warning("Missing subject_entity_id or predicate in claim")
            return conflicts

        # Get existing claims for this entity and predicate
        existing_claims = await self._get_existing_claims(
            subject_entity_id, predicate
        )

        for existing in existing_claims:
            if existing.get("id") == claim_id:
                continue  # Skip self

            # Value mismatch detection
            if self.config.enable_value_mismatch:
                value_conflict = self._detect_value_mismatch(
                    claim_data, existing, predicate
                )
                if value_conflict:
                    conflicts.append(value_conflict)

            # Temporal conflict detection
            if self.config.enable_temporal:
                temporal_conflict = self._detect_temporal_conflict(
                    claim_data, existing, predicate
                )
                if temporal_conflict:
                    conflicts.append(temporal_conflict)

        # Semantic conflict detection across all predicates
        if self.config.enable_semantic:
            semantic_conflicts = await self._detect_semantic_conflicts(
                claim_data, subject_entity_id
            )
            conflicts.extend(semantic_conflicts)

        return conflicts

    async def detect_all_conflicts(
        self,
        entity_id: Optional[str] = None,
        predicate: Optional[str] = None,
        limit: int = 100,
    ) -> list[DetectedConflict]:
        """
        Scan for all conflicts in the graph.

        Args:
            entity_id: Optional filter by entity
            predicate: Optional filter by predicate
            limit: Maximum conflicts to return

        Returns:
            List of detected conflicts
        """
        conflicts = []

        # Query for potential conflicts (same entity+predicate, different values)
        query = """
        MATCH (e:Entity)-[:HAS_CLAIM]->(c1:Claim)
        MATCH (e)-[:HAS_CLAIM]->(c2:Claim)
        WHERE c1.predicate = c2.predicate
          AND c1.object_value <> c2.object_value
          AND id(c1) < id(c2)
          AND c1.status IN ['current', 'CURRENT']
          AND c2.status IN ['current', 'CURRENT']
        """

        params = {"limit": limit}

        if entity_id:
            query += " AND e.id = $entity_id"
            params["entity_id"] = entity_id

        if predicate:
            query += " AND c1.predicate = $predicate"
            params["predicate"] = predicate

        query += """
        OPTIONAL MATCH (c1)-[:SOURCED_FROM]->(s1:Source)
        OPTIONAL MATCH (c2)-[:SOURCED_FROM]->(s2:Source)
        RETURN e.id as entity_id,
               c1.id as claim1_id, c1.object_value as value1,
               c1.confidence as conf1, s1.id as source1_id,
               c2.id as claim2_id, c2.object_value as value2,
               c2.confidence as conf2, s2.id as source2_id,
               c1.predicate as predicate
        LIMIT $limit
        """

        result = await self.client.execute_query(query, params)

        for record in result.records:
            conflict = DetectedConflict(
                id=f"conflict_{uuid4().hex[:12]}",
                conflict_type=ConflictType.VALUE_MISMATCH,
                claim_ids=[record["claim1_id"], record["claim2_id"]],
                subject_entity_id=record["entity_id"],
                predicate=record["predicate"],
                values=[record["value1"], record["value2"]],
                confidences=[record.get("conf1", 0.8), record.get("conf2", 0.8)],
                source_ids=[s for s in [record.get("source1_id"), record.get("source2_id")] if s],
                severity=self._assess_severity(
                    record["predicate"],
                    [record.get("conf1", 0.8), record.get("conf2", 0.8)]
                ),
            )
            conflicts.append(conflict)

        return conflicts

    async def _get_existing_claims(
        self,
        entity_id: str,
        predicate: str,
    ) -> list[dict[str, Any]]:
        """Get existing current claims for an entity and predicate."""
        query = """
        MATCH (e:Entity {id: $entity_id})-[:HAS_CLAIM]->(c:Claim)
        WHERE c.predicate = $predicate
          AND c.status IN ['current', 'CURRENT']
        OPTIONAL MATCH (c)-[:SOURCED_FROM]->(s:Source)
        RETURN c.id as id, c.object_value as object_value,
               c.confidence as confidence, c.valid_from as valid_from,
               c.valid_until as valid_until, s.id as source_id,
               s.reliability_score as source_reliability
        """

        result = await self.client.execute_query(query, {
            "entity_id": entity_id,
            "predicate": predicate,
        })

        return result.records

    def _detect_value_mismatch(
        self,
        new_claim: dict[str, Any],
        existing_claim: dict[str, Any],
        predicate: str,
    ) -> Optional[DetectedConflict]:
        """
        Detect value mismatch conflict between two claims.

        Args:
            new_claim: The new claim being added
            existing_claim: An existing claim in the graph
            predicate: The predicate being compared

        Returns:
            DetectedConflict if mismatch found, None otherwise
        """
        new_value = str(new_claim.get("object_value", "")).strip().lower()
        existing_value = str(existing_claim.get("object_value", "")).strip().lower()

        # Check if values are different
        if new_value == existing_value:
            return None

        # Check if values are similar (fuzzy match)
        similarity = self._calculate_value_similarity(new_value, existing_value)
        if similarity >= self.config.value_similarity_threshold:
            return None  # Values are similar enough, not a conflict

        # Check if this predicate should have a single value
        is_single_value = predicate in self.config.single_value_predicates

        new_confidence = new_claim.get("confidence", 0.8)
        existing_confidence = existing_claim.get("confidence", 0.8)

        # Skip if both confidences are too low
        if new_confidence < self.config.min_confidence_threshold and \
           existing_confidence < self.config.min_confidence_threshold:
            return None

        return DetectedConflict(
            id=f"conflict_{uuid4().hex[:12]}",
            conflict_type=ConflictType.VALUE_MISMATCH,
            claim_ids=[new_claim.get("id", "unknown"), existing_claim.get("id", "unknown")],
            subject_entity_id=new_claim.get("subject_entity_id", "unknown"),
            predicate=predicate,
            values=[new_claim.get("object_value"), existing_claim.get("object_value")],
            confidences=[new_confidence, existing_confidence],
            source_ids=[
                s for s in [new_claim.get("source_id"), existing_claim.get("source_id")] if s
            ],
            severity=self._assess_severity(predicate, [new_confidence, existing_confidence]),
            score=1.0 - similarity,  # Higher score = more certain of conflict
            metadata={
                "value_similarity": similarity,
                "is_single_value_predicate": is_single_value,
            },
        )

    def _detect_temporal_conflict(
        self,
        new_claim: dict[str, Any],
        existing_claim: dict[str, Any],
        predicate: str,
    ) -> Optional[DetectedConflict]:
        """
        Detect temporal conflict (overlapping validity periods).

        Args:
            new_claim: The new claim being added
            existing_claim: An existing claim in the graph
            predicate: The predicate being compared

        Returns:
            DetectedConflict if temporal overlap found, None otherwise
        """
        new_valid_from = new_claim.get("valid_from")
        new_valid_until = new_claim.get("valid_until")
        existing_valid_from = existing_claim.get("valid_from")
        existing_valid_until = existing_claim.get("valid_until")

        # Parse dates if strings
        if isinstance(new_valid_from, str):
            try:
                new_valid_from = datetime.fromisoformat(new_valid_from.replace('Z', '+00:00'))
            except ValueError:
                new_valid_from = None

        if isinstance(existing_valid_from, str):
            try:
                existing_valid_from = datetime.fromisoformat(existing_valid_from.replace('Z', '+00:00'))
            except ValueError:
                existing_valid_from = None

        # If no temporal data, cannot detect temporal conflicts
        if not new_valid_from and not existing_valid_from:
            return None

        # Check for overlap
        overlap = self._check_temporal_overlap(
            new_valid_from, new_valid_until,
            existing_valid_from, existing_valid_until
        )

        if not overlap:
            return None

        # Values must be different for this to be a temporal conflict
        new_value = str(new_claim.get("object_value", "")).strip().lower()
        existing_value = str(existing_claim.get("object_value", "")).strip().lower()

        if new_value == existing_value:
            return None

        return DetectedConflict(
            id=f"conflict_{uuid4().hex[:12]}",
            conflict_type=ConflictType.TEMPORAL,
            claim_ids=[new_claim.get("id", "unknown"), existing_claim.get("id", "unknown")],
            subject_entity_id=new_claim.get("subject_entity_id", "unknown"),
            predicate=predicate,
            values=[new_claim.get("object_value"), existing_claim.get("object_value")],
            confidences=[
                new_claim.get("confidence", 0.8),
                existing_claim.get("confidence", 0.8)
            ],
            source_ids=[
                s for s in [new_claim.get("source_id"), existing_claim.get("source_id")] if s
            ],
            severity=ConflictSeverity.HIGH,
            metadata={
                "new_valid_from": new_valid_from.isoformat() if new_valid_from else None,
                "new_valid_until": new_valid_until.isoformat() if isinstance(new_valid_until, datetime) else new_valid_until,
                "existing_valid_from": existing_valid_from.isoformat() if existing_valid_from else None,
                "existing_valid_until": existing_valid_until.isoformat() if isinstance(existing_valid_until, datetime) else existing_valid_until,
            },
        )

    async def _detect_semantic_conflicts(
        self,
        new_claim: dict[str, Any],
        entity_id: str,
    ) -> list[DetectedConflict]:
        """
        Detect semantic contradictions using predicate analysis.

        Args:
            new_claim: The new claim being added
            entity_id: The entity ID

        Returns:
            List of semantic conflicts detected
        """
        conflicts = []
        new_predicate = new_claim.get("predicate", "")

        # Check if the new predicate contradicts any existing predicates
        contradiction_predicates = self._contradiction_pairs.get(new_predicate, [])

        if not contradiction_predicates:
            return conflicts

        # Query for claims with contradicting predicates
        query = """
        MATCH (e:Entity {id: $entity_id})-[:HAS_CLAIM]->(c:Claim)
        WHERE c.predicate IN $predicates
          AND c.status IN ['current', 'CURRENT']
        OPTIONAL MATCH (c)-[:SOURCED_FROM]->(s:Source)
        RETURN c.id as id, c.predicate as predicate, c.object_value as object_value,
               c.confidence as confidence, s.id as source_id
        """

        result = await self.client.execute_query(query, {
            "entity_id": entity_id,
            "predicates": contradiction_predicates,
        })

        for record in result.records:
            conflict = DetectedConflict(
                id=f"conflict_{uuid4().hex[:12]}",
                conflict_type=ConflictType.SEMANTIC,
                claim_ids=[new_claim.get("id", "unknown"), record["id"]],
                subject_entity_id=entity_id,
                predicate=f"{new_predicate} vs {record['predicate']}",
                values=[new_claim.get("object_value"), record["object_value"]],
                confidences=[
                    new_claim.get("confidence", 0.8),
                    record.get("confidence", 0.8)
                ],
                source_ids=[
                    s for s in [new_claim.get("source_id"), record.get("source_id")] if s
                ],
                severity=ConflictSeverity.HIGH,
                metadata={
                    "new_predicate": new_predicate,
                    "existing_predicate": record["predicate"],
                    "contradiction_type": "predicate_pair",
                },
            )
            conflicts.append(conflict)

        return conflicts

    def _calculate_value_similarity(self, value1: str, value2: str) -> float:
        """
        Calculate similarity between two values.

        Uses a simple character-based similarity for now.
        Can be enhanced with embeddings for semantic similarity.

        Args:
            value1: First value
            value2: Second value

        Returns:
            Similarity score between 0 and 1
        """
        if value1 == value2:
            return 1.0

        if not value1 or not value2:
            return 0.0

        # Normalize values
        v1 = value1.lower().strip()
        v2 = value2.lower().strip()

        # Check for numeric similarity
        try:
            n1 = float(v1.replace(",", "").replace("$", "").replace("%", ""))
            n2 = float(v2.replace(",", "").replace("$", "").replace("%", ""))
            # Calculate relative difference
            if max(abs(n1), abs(n2)) > 0:
                return 1.0 - abs(n1 - n2) / max(abs(n1), abs(n2))
        except ValueError:
            pass

        # Simple Jaccard similarity on words
        words1 = set(v1.split())
        words2 = set(v2.split())

        if not words1 or not words2:
            return 0.0

        intersection = len(words1 & words2)
        union = len(words1 | words2)

        return intersection / union if union > 0 else 0.0

    def _check_temporal_overlap(
        self,
        start1: Optional[datetime],
        end1: Optional[datetime],
        start2: Optional[datetime],
        end2: Optional[datetime],
    ) -> bool:
        """
        Check if two time periods overlap.

        Args:
            start1, end1: First time period
            start2, end2: Second time period

        Returns:
            True if periods overlap
        """
        # Use infinity for missing endpoints
        if start1 is None:
            start1 = datetime.min
        if end1 is None:
            end1 = datetime.max
        if start2 is None:
            start2 = datetime.min
        if end2 is None:
            end2 = datetime.max

        # Ensure datetime objects
        if isinstance(end1, str):
            try:
                end1 = datetime.fromisoformat(end1.replace('Z', '+00:00'))
            except ValueError:
                end1 = datetime.max

        if isinstance(end2, str):
            try:
                end2 = datetime.fromisoformat(end2.replace('Z', '+00:00'))
            except ValueError:
                end2 = datetime.max

        # Standard overlap check
        return start1 <= end2 and start2 <= end1

    def _assess_severity(
        self,
        predicate: str,
        confidences: list[float],
    ) -> ConflictSeverity:
        """
        Assess the severity of a conflict.

        Args:
            predicate: The predicate involved
            confidences: Confidence scores of conflicting claims

        Returns:
            Severity level
        """
        avg_confidence = sum(confidences) / len(confidences) if confidences else 0.5

        # High-impact predicates
        high_impact = {"ceo", "revenue", "employee_count", "founded_date", "status", "price"}

        if predicate in high_impact and avg_confidence > 0.8:
            return ConflictSeverity.CRITICAL
        elif predicate in high_impact:
            return ConflictSeverity.HIGH
        elif avg_confidence > 0.7:
            return ConflictSeverity.MEDIUM
        else:
            return ConflictSeverity.LOW


# Pydantic models for API request/response

class ConflictDetectionRequest(BaseModel):
    """Request to detect conflicts for a claim."""
    subject_entity_id: str
    predicate: str
    object_value: Any
    claim_id: Optional[str] = None
    source_id: Optional[str] = None
    confidence: float = Field(default=0.8, ge=0.0, le=1.0)
    valid_from: Optional[datetime] = None
    valid_until: Optional[datetime] = None


class ConflictDetectionResponse(BaseModel):
    """Response containing detected conflicts."""
    conflicts: list[dict[str, Any]]
    total_conflicts: int
    has_conflicts: bool
