"""
Conflict Resolution Service
===========================

This module provides conflict resolution strategies for the DocWeave knowledge graph.
It supports multiple resolution strategies and maintains an audit trail of all
resolution decisions.

Resolution Strategies:
    - TIMESTAMP_BASED: Newer claim wins
    - SOURCE_RELIABILITY: Higher reliability source wins
    - CONFIDENCE_BASED: Higher confidence claim wins
    - MULTI_SOURCE_VOTING: Majority of sources wins
    - MANUAL_REVIEW: Flag for human review
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional
from uuid import uuid4

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class ResolutionStrategy(str, Enum):
    """Available strategies for resolving conflicts."""
    TIMESTAMP_BASED = "timestamp_based"
    SOURCE_RELIABILITY = "source_reliability"
    CONFIDENCE_BASED = "confidence_based"
    MULTI_SOURCE_VOTING = "multi_source_voting"
    WEIGHTED_COMBINATION = "weighted_combination"
    MANUAL_REVIEW = "manual_review"


class ResolutionOutcome(str, Enum):
    """Possible outcomes of conflict resolution."""
    WINNER_SELECTED = "winner_selected"
    FLAGGED_FOR_REVIEW = "flagged_for_review"
    MERGED = "merged"  # For compatible values
    DEFERRED = "deferred"
    NO_RESOLUTION = "no_resolution"


@dataclass
class ResolutionAuditEntry:
    """
    Represents an entry in the resolution audit trail.

    Tracks every resolution decision for accountability and
    potential rollback.
    """
    id: str
    conflict_id: str
    timestamp: datetime
    strategy_used: ResolutionStrategy
    outcome: ResolutionOutcome
    winning_claim_id: Optional[str]
    losing_claim_ids: list[str]
    resolved_by: str  # "system" or user ID
    reason: str
    confidence: float
    scores: dict[str, float]  # Claim ID -> score
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "id": self.id,
            "conflict_id": self.conflict_id,
            "timestamp": self.timestamp.isoformat(),
            "strategy_used": self.strategy_used.value,
            "outcome": self.outcome.value,
            "winning_claim_id": self.winning_claim_id,
            "losing_claim_ids": self.losing_claim_ids,
            "resolved_by": self.resolved_by,
            "reason": self.reason,
            "confidence": self.confidence,
            "scores": self.scores,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ResolutionAuditEntry":
        """Create from dictionary."""
        return cls(
            id=data["id"],
            conflict_id=data["conflict_id"],
            timestamp=datetime.fromisoformat(data["timestamp"]) if isinstance(data["timestamp"], str) else data["timestamp"],
            strategy_used=ResolutionStrategy(data["strategy_used"]),
            outcome=ResolutionOutcome(data["outcome"]),
            winning_claim_id=data.get("winning_claim_id"),
            losing_claim_ids=data.get("losing_claim_ids", []),
            resolved_by=data["resolved_by"],
            reason=data["reason"],
            confidence=data.get("confidence", 0.0),
            scores=data.get("scores", {}),
            metadata=data.get("metadata", {}),
        )


@dataclass
class ResolutionResult:
    """
    Result of a conflict resolution attempt.

    Contains the resolution decision, winning claim,
    and audit information.
    """
    conflict_id: str
    outcome: ResolutionOutcome
    winning_claim_id: Optional[str]
    losing_claim_ids: list[str]
    strategy_used: ResolutionStrategy
    reason: str
    confidence: float
    audit_entry: ResolutionAuditEntry
    actions_taken: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "conflict_id": self.conflict_id,
            "outcome": self.outcome.value,
            "winning_claim_id": self.winning_claim_id,
            "losing_claim_ids": self.losing_claim_ids,
            "strategy_used": self.strategy_used.value,
            "reason": self.reason,
            "confidence": self.confidence,
            "audit_entry": self.audit_entry.to_dict(),
            "actions_taken": self.actions_taken,
        }


class ConflictResolverConfig(BaseModel):
    """Configuration for the conflict resolver."""
    # Default strategy
    default_strategy: ResolutionStrategy = Field(
        default=ResolutionStrategy.WEIGHTED_COMBINATION
    )

    # Strategy weights for weighted combination
    timestamp_weight: float = Field(default=0.2, ge=0.0, le=1.0)
    confidence_weight: float = Field(default=0.4, ge=0.0, le=1.0)
    source_reliability_weight: float = Field(default=0.3, ge=0.0, le=1.0)
    source_count_weight: float = Field(default=0.1, ge=0.0, le=1.0)

    # Thresholds
    auto_resolve_confidence_threshold: float = Field(default=0.7, ge=0.0, le=1.0)
    manual_review_threshold: float = Field(default=0.5, ge=0.0, le=1.0)
    score_difference_threshold: float = Field(default=0.1, ge=0.0, le=1.0)

    # Multi-source voting settings
    min_sources_for_voting: int = Field(default=2, ge=2)
    voting_majority_threshold: float = Field(default=0.5, ge=0.0, le=1.0)

    # Audit settings
    keep_audit_history_days: int = Field(default=365, ge=30)


class ConflictResolver:
    """
    Resolves conflicts between claims using configurable strategies.

    This service applies resolution strategies to conflicts and
    maintains a complete audit trail of all decisions.

    Example:
        resolver = ConflictResolver(neo4j_client)
        result = await resolver.resolve_conflict(
            conflict_id="conflict_123",
            claim_data=[claim1, claim2],
            strategy=ResolutionStrategy.CONFIDENCE_BASED
        )

        if result.outcome == ResolutionOutcome.WINNER_SELECTED:
            print(f"Winner: {result.winning_claim_id}")
    """

    def __init__(
        self,
        neo4j_client,
        config: Optional[ConflictResolverConfig] = None,
    ):
        """
        Initialize the conflict resolver.

        Args:
            neo4j_client: Neo4j client for database operations
            config: Configuration for resolution
        """
        self.client = neo4j_client
        self.config = config or ConflictResolverConfig()

    async def resolve_conflict(
        self,
        conflict_id: str,
        claim_data: list[dict[str, Any]],
        strategy: Optional[ResolutionStrategy] = None,
        resolved_by: str = "system",
        force_resolution: bool = False,
    ) -> ResolutionResult:
        """
        Resolve a conflict between claims.

        Args:
            conflict_id: ID of the conflict to resolve
            claim_data: List of claim dictionaries with their metadata
            strategy: Resolution strategy to use (defaults to config)
            resolved_by: Who is resolving (system or user ID)
            force_resolution: Force resolution even if below threshold

        Returns:
            ResolutionResult with the decision and audit trail
        """
        strategy = strategy or self.config.default_strategy

        logger.info(f"Resolving conflict {conflict_id} using strategy {strategy.value}")

        # Calculate scores for each claim
        scores = await self._calculate_scores(claim_data, strategy)

        # Determine winner based on scores
        winning_claim_id = None
        losing_claim_ids = []
        outcome = ResolutionOutcome.NO_RESOLUTION
        confidence = 0.0
        reason = ""

        if not scores:
            reason = "No claims to evaluate"
        else:
            # Sort by score
            sorted_claims = sorted(scores.items(), key=lambda x: x[1], reverse=True)
            best_claim_id, best_score = sorted_claims[0]
            second_best_score = sorted_claims[1][1] if len(sorted_claims) > 1 else 0.0

            # Calculate confidence based on score difference
            score_diff = best_score - second_best_score
            confidence = min(1.0, score_diff / self.config.score_difference_threshold)

            if strategy == ResolutionStrategy.MANUAL_REVIEW:
                outcome = ResolutionOutcome.FLAGGED_FOR_REVIEW
                reason = "Manual review requested"
            elif confidence >= self.config.auto_resolve_confidence_threshold or force_resolution:
                winning_claim_id = best_claim_id
                losing_claim_ids = [cid for cid, _ in sorted_claims[1:]]
                outcome = ResolutionOutcome.WINNER_SELECTED
                reason = self._generate_reason(strategy, best_score, score_diff)
            elif confidence >= self.config.manual_review_threshold:
                outcome = ResolutionOutcome.FLAGGED_FOR_REVIEW
                reason = f"Score difference ({score_diff:.2f}) below auto-resolve threshold"
            else:
                outcome = ResolutionOutcome.DEFERRED
                reason = f"Low confidence ({confidence:.2f}) for resolution"

        # Create audit entry
        audit_entry = ResolutionAuditEntry(
            id=f"audit_{uuid4().hex[:12]}",
            conflict_id=conflict_id,
            timestamp=datetime.utcnow(),
            strategy_used=strategy,
            outcome=outcome,
            winning_claim_id=winning_claim_id,
            losing_claim_ids=losing_claim_ids,
            resolved_by=resolved_by,
            reason=reason,
            confidence=confidence,
            scores=scores,
            metadata={
                "claim_count": len(claim_data),
                "force_resolution": force_resolution,
            },
        )

        # Store audit entry
        await self._store_audit_entry(audit_entry)

        # Apply resolution if winner selected
        actions_taken = []
        if outcome == ResolutionOutcome.WINNER_SELECTED and winning_claim_id:
            actions_taken = await self._apply_resolution(
                conflict_id, winning_claim_id, losing_claim_ids
            )

        return ResolutionResult(
            conflict_id=conflict_id,
            outcome=outcome,
            winning_claim_id=winning_claim_id,
            losing_claim_ids=losing_claim_ids,
            strategy_used=strategy,
            reason=reason,
            confidence=confidence,
            audit_entry=audit_entry,
            actions_taken=actions_taken,
        )

    async def resolve_by_manual_selection(
        self,
        conflict_id: str,
        winning_claim_id: str,
        reason: str,
        resolved_by: str,
    ) -> ResolutionResult:
        """
        Manually resolve a conflict by selecting the winning claim.

        Args:
            conflict_id: ID of the conflict
            winning_claim_id: ID of the claim to keep
            reason: Human-provided reason for the decision
            resolved_by: User ID of the resolver

        Returns:
            ResolutionResult with the manual decision
        """
        # Get conflict details
        conflict_data = await self._get_conflict_details(conflict_id)
        if not conflict_data:
            raise ValueError(f"Conflict {conflict_id} not found")

        all_claim_ids = [conflict_data.get("claim1_id"), conflict_data.get("claim2_id")]
        all_claim_ids = [c for c in all_claim_ids if c]

        if winning_claim_id not in all_claim_ids:
            raise ValueError(f"Winning claim {winning_claim_id} not part of conflict {conflict_id}")

        losing_claim_ids = [c for c in all_claim_ids if c != winning_claim_id]

        # Create audit entry
        audit_entry = ResolutionAuditEntry(
            id=f"audit_{uuid4().hex[:12]}",
            conflict_id=conflict_id,
            timestamp=datetime.utcnow(),
            strategy_used=ResolutionStrategy.MANUAL_REVIEW,
            outcome=ResolutionOutcome.WINNER_SELECTED,
            winning_claim_id=winning_claim_id,
            losing_claim_ids=losing_claim_ids,
            resolved_by=resolved_by,
            reason=reason,
            confidence=1.0,  # Manual decisions have full confidence
            scores={winning_claim_id: 1.0},
        )

        await self._store_audit_entry(audit_entry)

        # Apply resolution
        actions_taken = await self._apply_resolution(
            conflict_id, winning_claim_id, losing_claim_ids
        )

        return ResolutionResult(
            conflict_id=conflict_id,
            outcome=ResolutionOutcome.WINNER_SELECTED,
            winning_claim_id=winning_claim_id,
            losing_claim_ids=losing_claim_ids,
            strategy_used=ResolutionStrategy.MANUAL_REVIEW,
            reason=reason,
            confidence=1.0,
            audit_entry=audit_entry,
            actions_taken=actions_taken,
        )

    async def get_audit_trail(
        self,
        conflict_id: Optional[str] = None,
        claim_id: Optional[str] = None,
        limit: int = 100,
    ) -> list[ResolutionAuditEntry]:
        """
        Get audit trail entries.

        Args:
            conflict_id: Filter by conflict ID
            claim_id: Filter by claim ID
            limit: Maximum entries to return

        Returns:
            List of audit entries
        """
        query = """
        MATCH (a:ResolutionAudit)
        """

        params = {"limit": limit}
        conditions = []

        if conflict_id:
            conditions.append("a.conflict_id = $conflict_id")
            params["conflict_id"] = conflict_id

        if claim_id:
            conditions.append("($claim_id IN a.losing_claim_ids OR a.winning_claim_id = $claim_id)")
            params["claim_id"] = claim_id

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += """
        RETURN a
        ORDER BY a.timestamp DESC
        LIMIT $limit
        """

        result = await self.client.execute_query(query, params)

        entries = []
        for record in result.records:
            if "a" in record:
                entry_data = dict(record["a"])
                entries.append(ResolutionAuditEntry.from_dict(entry_data))

        return entries

    async def _calculate_scores(
        self,
        claim_data: list[dict[str, Any]],
        strategy: ResolutionStrategy,
    ) -> dict[str, float]:
        """
        Calculate resolution scores for each claim.

        Args:
            claim_data: List of claim dictionaries
            strategy: Strategy to use for scoring

        Returns:
            Dictionary mapping claim IDs to scores
        """
        scores = {}

        for claim in claim_data:
            claim_id = claim.get("id")
            if not claim_id:
                continue

            if strategy == ResolutionStrategy.TIMESTAMP_BASED:
                score = self._score_by_timestamp(claim)
            elif strategy == ResolutionStrategy.SOURCE_RELIABILITY:
                score = self._score_by_source_reliability(claim)
            elif strategy == ResolutionStrategy.CONFIDENCE_BASED:
                score = self._score_by_confidence(claim)
            elif strategy == ResolutionStrategy.MULTI_SOURCE_VOTING:
                score = await self._score_by_voting(claim, claim_data)
            elif strategy == ResolutionStrategy.WEIGHTED_COMBINATION:
                score = await self._score_weighted_combination(claim, claim_data)
            else:
                score = claim.get("confidence", 0.5)

            scores[claim_id] = score

        return scores

    def _score_by_timestamp(self, claim: dict[str, Any]) -> float:
        """Score claim by recency (newer = higher score)."""
        timestamp = claim.get("timestamp") or claim.get("created_at")

        if isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except ValueError:
                return 0.5

        if isinstance(timestamp, datetime):
            # Normalize to 0-1 based on recency (within last year)
            age_seconds = (datetime.utcnow() - timestamp.replace(tzinfo=None)).total_seconds()
            year_seconds = 365 * 24 * 60 * 60
            return max(0.0, 1.0 - (age_seconds / year_seconds))

        return 0.5

    def _score_by_source_reliability(self, claim: dict[str, Any]) -> float:
        """Score claim by source reliability."""
        return claim.get("source_reliability", 0.5)

    def _score_by_confidence(self, claim: dict[str, Any]) -> float:
        """Score claim by extraction confidence."""
        return claim.get("confidence", 0.5)

    async def _score_by_voting(
        self,
        claim: dict[str, Any],
        all_claims: list[dict[str, Any]],
    ) -> float:
        """
        Score claim by counting supporting sources.

        A claim gets higher score if multiple sources report
        the same value.
        """
        target_value = str(claim.get("object_value", "")).strip().lower()
        supporting_sources = 0
        total_sources = 0

        for c in all_claims:
            total_sources += 1
            c_value = str(c.get("object_value", "")).strip().lower()
            if c_value == target_value:
                supporting_sources += 1

        if total_sources == 0:
            return 0.5

        return supporting_sources / total_sources

    async def _score_weighted_combination(
        self,
        claim: dict[str, Any],
        all_claims: list[dict[str, Any]],
    ) -> float:
        """
        Calculate weighted combination of all scoring factors.

        Uses configured weights to combine:
        - Timestamp score
        - Confidence score
        - Source reliability score
        - Multi-source voting score
        """
        timestamp_score = self._score_by_timestamp(claim)
        confidence_score = self._score_by_confidence(claim)
        reliability_score = self._score_by_source_reliability(claim)
        voting_score = await self._score_by_voting(claim, all_claims)

        weighted_score = (
            self.config.timestamp_weight * timestamp_score +
            self.config.confidence_weight * confidence_score +
            self.config.source_reliability_weight * reliability_score +
            self.config.source_count_weight * voting_score
        )

        # Normalize by sum of weights
        total_weight = (
            self.config.timestamp_weight +
            self.config.confidence_weight +
            self.config.source_reliability_weight +
            self.config.source_count_weight
        )

        return weighted_score / total_weight if total_weight > 0 else 0.5

    def _generate_reason(
        self,
        strategy: ResolutionStrategy,
        best_score: float,
        score_diff: float,
    ) -> str:
        """Generate human-readable reason for resolution."""
        strategy_descriptions = {
            ResolutionStrategy.TIMESTAMP_BASED: "newer claim selected",
            ResolutionStrategy.SOURCE_RELIABILITY: "higher reliability source selected",
            ResolutionStrategy.CONFIDENCE_BASED: "higher confidence claim selected",
            ResolutionStrategy.MULTI_SOURCE_VOTING: "majority of sources agree",
            ResolutionStrategy.WEIGHTED_COMBINATION: "weighted combination of factors",
        }

        base_reason = strategy_descriptions.get(strategy, "automated resolution")
        return f"Auto-resolved: {base_reason} (score: {best_score:.2f}, diff: {score_diff:.2f})"

    async def _store_audit_entry(self, entry: ResolutionAuditEntry) -> None:
        """Store audit entry in the graph."""
        query = """
        CREATE (a:ResolutionAudit $props)
        """

        await self.client.execute_query(query, {"props": entry.to_dict()})
        logger.info(f"Stored audit entry {entry.id} for conflict {entry.conflict_id}")

    async def _get_conflict_details(self, conflict_id: str) -> Optional[dict[str, Any]]:
        """Get details of a conflict from the graph."""
        query = """
        MATCH (c1:Claim)-[r:CONFLICTS_WITH {id: $conflict_id}]->(c2:Claim)
        OPTIONAL MATCH (c1)-[:SOURCED_FROM]->(s1:Source)
        OPTIONAL MATCH (c2)-[:SOURCED_FROM]->(s2:Source)
        RETURN r.id as conflict_id, r.status as status,
               c1.id as claim1_id, c1.object_value as value1,
               c1.confidence as conf1, c1.created_at as timestamp1,
               s1.reliability_score as reliability1,
               c2.id as claim2_id, c2.object_value as value2,
               c2.confidence as conf2, c2.created_at as timestamp2,
               s2.reliability_score as reliability2
        """

        result = await self.client.execute_query(query, {"conflict_id": conflict_id})
        return result.records[0] if result.records else None

    async def _apply_resolution(
        self,
        conflict_id: str,
        winning_claim_id: str,
        losing_claim_ids: list[str],
    ) -> list[str]:
        """
        Apply the resolution decision to the graph.

        Updates claim statuses and conflict relationship.

        Args:
            conflict_id: ID of the conflict
            winning_claim_id: ID of the winning claim
            losing_claim_ids: IDs of losing claims

        Returns:
            List of actions taken
        """
        actions = []

        # Mark losing claims as superseded
        for claim_id in losing_claim_ids:
            query = """
            MATCH (c:Claim {id: $claim_id})
            SET c.status = 'superseded',
                c.superseded_by = $winning_claim_id,
                c.superseded_at = datetime(),
                c.valid_until = datetime()
            RETURN c.id as id
            """
            await self.client.execute_query(query, {
                "claim_id": claim_id,
                "winning_claim_id": winning_claim_id,
            })
            actions.append(f"Marked claim {claim_id} as superseded")

        # Update conflict relationship
        query = """
        MATCH ()-[r:CONFLICTS_WITH {id: $conflict_id}]->()
        SET r.status = 'RESOLVED',
            r.resolved_at = datetime(),
            r.winning_claim_id = $winning_claim_id
        RETURN r.id as id
        """
        await self.client.execute_query(query, {
            "conflict_id": conflict_id,
            "winning_claim_id": winning_claim_id,
        })
        actions.append(f"Resolved conflict {conflict_id}")

        # Ensure winning claim is current
        query = """
        MATCH (c:Claim {id: $claim_id})
        SET c.status = 'current'
        RETURN c.id as id
        """
        await self.client.execute_query(query, {"claim_id": winning_claim_id})
        actions.append(f"Confirmed claim {winning_claim_id} as current")

        logger.info(f"Applied resolution for conflict {conflict_id}: winner={winning_claim_id}")
        return actions


# Pydantic models for API request/response

class ManualResolutionRequest(BaseModel):
    """Request for manual conflict resolution."""
    winning_claim_id: str
    reason: str = Field(..., min_length=10, max_length=1000)
    resolved_by: str = Field(default="manual_user")


class AutoResolutionRequest(BaseModel):
    """Request for automatic conflict resolution."""
    strategy: Optional[ResolutionStrategy] = None
    force_resolution: bool = Field(default=False)


class ResolutionResponse(BaseModel):
    """Response from conflict resolution."""
    conflict_id: str
    outcome: str
    winning_claim_id: Optional[str]
    losing_claim_ids: list[str]
    strategy_used: str
    reason: str
    confidence: float
    actions_taken: list[str]
