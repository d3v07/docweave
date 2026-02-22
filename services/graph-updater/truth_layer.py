"""
Governed Truth Layer
====================

This module provides the truth layer for the DocWeave knowledge graph.
It computes and maintains the current truth for entities, supports
historical truth queries, and propagates truth updates.

The truth layer acts as a materialized view over the claims, providing:
    - Current truth: The best-known value for each entity+predicate
    - Historical truth: The truth at any point in time
    - Truth confidence: How certain we are about each truth
    - Truth lineage: Which claims support each truth
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional
from uuid import uuid4

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


@dataclass
class TruthValue:
    """
    Represents the current truth for an entity attribute.

    A TruthValue aggregates all current claims about a specific
    entity+predicate combination and presents the best-known value.

    Attributes:
        entity_id: ID of the entity
        predicate: The attribute/relationship
        value: The current truth value
        confidence: Aggregated confidence score
        supporting_claim_ids: Claims that support this truth
        source_count: Number of unique sources
        last_updated: When this truth was last computed
        version: Version number for tracking changes
        metadata: Additional truth metadata
    """
    entity_id: str
    predicate: str
    value: Any
    confidence: float
    supporting_claim_ids: list[str] = field(default_factory=list)
    source_count: int = 0
    last_updated: datetime = field(default_factory=datetime.utcnow)
    version: int = 1
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "entity_id": self.entity_id,
            "predicate": self.predicate,
            "value": self.value,
            "confidence": self.confidence,
            "supporting_claim_ids": self.supporting_claim_ids,
            "source_count": self.source_count,
            "last_updated": self.last_updated.isoformat(),
            "version": self.version,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TruthValue":
        """Create from dictionary."""
        return cls(
            entity_id=data["entity_id"],
            predicate=data["predicate"],
            value=data["value"],
            confidence=data.get("confidence", 0.0),
            supporting_claim_ids=data.get("supporting_claim_ids", []),
            source_count=data.get("source_count", 0),
            last_updated=datetime.fromisoformat(data["last_updated"]) if isinstance(data.get("last_updated"), str) else datetime.utcnow(),
            version=data.get("version", 1),
            metadata=data.get("metadata", {}),
        )


@dataclass
class EntityTruth:
    """
    Complete truth view for an entity.

    Aggregates all truth values for a single entity into
    a coherent view.

    Attributes:
        entity_id: ID of the entity
        entity_name: Name of the entity
        entity_type: Type of the entity
        truths: Dictionary of predicate -> TruthValue
        has_conflicts: Whether any unresolved conflicts exist
        conflict_count: Number of unresolved conflicts
        computed_at: When this truth view was computed
    """
    entity_id: str
    entity_name: str
    entity_type: str
    truths: dict[str, TruthValue] = field(default_factory=dict)
    has_conflicts: bool = False
    conflict_count: int = 0
    computed_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "entity_id": self.entity_id,
            "entity_name": self.entity_name,
            "entity_type": self.entity_type,
            "truths": {k: v.to_dict() for k, v in self.truths.items()},
            "has_conflicts": self.has_conflicts,
            "conflict_count": self.conflict_count,
            "computed_at": self.computed_at.isoformat(),
        }

    def get_value(self, predicate: str) -> Optional[Any]:
        """Get the truth value for a predicate."""
        truth = self.truths.get(predicate)
        return truth.value if truth else None

    def get_confidence(self, predicate: str) -> float:
        """Get the confidence for a predicate."""
        truth = self.truths.get(predicate)
        return truth.confidence if truth else 0.0


@dataclass
class HistoricalTruth:
    """
    Truth value at a specific point in time.

    Supports point-in-time queries for auditing and
    historical analysis.
    """
    entity_id: str
    predicate: str
    value: Any
    as_of: datetime
    claim_id: str
    source_id: Optional[str]
    confidence: float

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "entity_id": self.entity_id,
            "predicate": self.predicate,
            "value": self.value,
            "as_of": self.as_of.isoformat(),
            "claim_id": self.claim_id,
            "source_id": self.source_id,
            "confidence": self.confidence,
        }


class TruthLayerConfig(BaseModel):
    """Configuration for the truth layer."""
    # Confidence aggregation
    use_weighted_confidence: bool = Field(default=True)
    min_truth_confidence: float = Field(default=0.3, ge=0.0, le=1.0)

    # Caching
    cache_ttl_seconds: int = Field(default=300, ge=60)
    enable_materialized_views: bool = Field(default=True)

    # Multi-value handling
    single_value_predicates: list[str] = Field(default_factory=lambda: [
        "ceo", "founded_date", "headquarters", "stock_ticker",
        "employee_count", "annual_revenue", "date_of_birth"
    ])

    # Truth propagation
    propagate_on_resolution: bool = Field(default=True)


class TruthLayer:
    """
    Provides truth computation and queries for the knowledge graph.

    The truth layer sits above claims and provides a clean,
    conflict-resolved view of entity attributes.

    Example:
        truth_layer = TruthLayer(neo4j_client)

        # Get current truth
        entity_truth = await truth_layer.get_entity_truth("ent_acme_corp")
        print(entity_truth.get_value("employee_count"))

        # Get historical truth
        historical = await truth_layer.get_truth_at_time(
            "ent_acme_corp", "employee_count", datetime(2023, 1, 1)
        )
    """

    def __init__(
        self,
        neo4j_client,
        config: Optional[TruthLayerConfig] = None,
    ):
        """
        Initialize the truth layer.

        Args:
            neo4j_client: Neo4j client for database operations
            config: Configuration for truth computation
        """
        self.client = neo4j_client
        self.config = config or TruthLayerConfig()
        self._cache: dict[str, tuple[EntityTruth, datetime]] = {}

    async def get_entity_truth(
        self,
        entity_id: str,
        use_cache: bool = True,
    ) -> Optional[EntityTruth]:
        """
        Get the complete current truth for an entity.

        Args:
            entity_id: ID of the entity
            use_cache: Whether to use cached values

        Returns:
            EntityTruth containing all truth values, or None if entity not found
        """
        # Check cache
        if use_cache and entity_id in self._cache:
            cached_truth, cached_at = self._cache[entity_id]
            age = (datetime.utcnow() - cached_at).total_seconds()
            if age < self.config.cache_ttl_seconds:
                return cached_truth

        # Query entity and its claims
        entity_query = """
        MATCH (e:Entity {id: $entity_id})
        RETURN e.id as id, e.name as name, e.type as type
        """

        entity_result = await self.client.execute_query(entity_query, {"entity_id": entity_id})
        if not entity_result.records:
            return None

        entity_record = entity_result.records[0]

        # Query all current claims grouped by predicate
        claims_query = """
        MATCH (e:Entity {id: $entity_id})-[:HAS_CLAIM]->(c:Claim)
        WHERE c.status IN ['current', 'CURRENT']
        OPTIONAL MATCH (c)-[:SOURCED_FROM]->(s:Source)
        WITH c.predicate as predicate,
             collect({
                 claim_id: c.id,
                 value: c.object_value,
                 confidence: c.confidence,
                 source_id: s.id,
                 source_reliability: s.reliability_score,
                 created_at: c.created_at
             }) as claims
        RETURN predicate, claims
        """

        claims_result = await self.client.execute_query(claims_query, {"entity_id": entity_id})

        # Build truth values
        truths = {}
        for record in claims_result.records:
            predicate = record["predicate"]
            claims = record["claims"]

            truth_value = self._compute_truth_value(entity_id, predicate, claims)
            if truth_value:
                truths[predicate] = truth_value

        # Check for unresolved conflicts
        conflicts_query = """
        MATCH (e:Entity {id: $entity_id})-[:HAS_CLAIM]->(c1:Claim)
        MATCH (c1)-[r:CONFLICTS_WITH {status: 'UNRESOLVED'}]->(c2:Claim)
        RETURN count(r) as conflict_count
        """

        conflicts_result = await self.client.execute_query(conflicts_query, {"entity_id": entity_id})
        conflict_count = conflicts_result.records[0]["conflict_count"] if conflicts_result.records else 0

        entity_truth = EntityTruth(
            entity_id=entity_id,
            entity_name=entity_record.get("name", ""),
            entity_type=entity_record.get("type", ""),
            truths=truths,
            has_conflicts=conflict_count > 0,
            conflict_count=conflict_count,
            computed_at=datetime.utcnow(),
        )

        # Cache result
        self._cache[entity_id] = (entity_truth, datetime.utcnow())

        return entity_truth

    async def get_truth_value(
        self,
        entity_id: str,
        predicate: str,
    ) -> Optional[TruthValue]:
        """
        Get the truth value for a specific entity+predicate.

        Args:
            entity_id: ID of the entity
            predicate: The attribute to query

        Returns:
            TruthValue or None if not found
        """
        entity_truth = await self.get_entity_truth(entity_id)
        if entity_truth:
            return entity_truth.truths.get(predicate)
        return None

    async def get_truth_at_time(
        self,
        entity_id: str,
        predicate: str,
        as_of: datetime,
    ) -> Optional[HistoricalTruth]:
        """
        Get the truth value at a specific point in time.

        Args:
            entity_id: ID of the entity
            predicate: The attribute to query
            as_of: The point in time to query

        Returns:
            HistoricalTruth or None if not found
        """
        query = """
        MATCH (e:Entity {id: $entity_id})-[:HAS_CLAIM]->(c:Claim)
        WHERE c.predicate = $predicate
          AND c.valid_from <= datetime($as_of)
          AND (c.valid_until IS NULL OR c.valid_until >= datetime($as_of))
        OPTIONAL MATCH (c)-[:SOURCED_FROM]->(s:Source)
        RETURN c.id as claim_id, c.object_value as value,
               c.confidence as confidence, s.id as source_id
        ORDER BY c.confidence DESC, c.created_at DESC
        LIMIT 1
        """

        result = await self.client.execute_query(query, {
            "entity_id": entity_id,
            "predicate": predicate,
            "as_of": as_of.isoformat(),
        })

        if not result.records:
            return None

        record = result.records[0]
        return HistoricalTruth(
            entity_id=entity_id,
            predicate=predicate,
            value=record["value"],
            as_of=as_of,
            claim_id=record["claim_id"],
            source_id=record.get("source_id"),
            confidence=record.get("confidence", 0.0),
        )

    async def get_truth_history(
        self,
        entity_id: str,
        predicate: str,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """
        Get the history of truth values for an entity+predicate.

        Shows how the truth has changed over time.

        Args:
            entity_id: ID of the entity
            predicate: The attribute to query
            limit: Maximum history entries

        Returns:
            List of historical truth entries
        """
        query = """
        MATCH (e:Entity {id: $entity_id})-[:HAS_CLAIM]->(c:Claim)
        WHERE c.predicate = $predicate
        OPTIONAL MATCH (c)-[:SOURCED_FROM]->(s:Source)
        RETURN c.id as claim_id, c.object_value as value,
               c.status as status, c.confidence as confidence,
               c.created_at as created_at, c.valid_from as valid_from,
               c.valid_until as valid_until, s.uri as source_uri,
               c.superseded_by as superseded_by
        ORDER BY c.created_at DESC
        LIMIT $limit
        """

        result = await self.client.execute_query(query, {
            "entity_id": entity_id,
            "predicate": predicate,
            "limit": limit,
        })

        return result.records

    async def refresh_truth(self, entity_id: str) -> EntityTruth:
        """
        Force refresh of truth for an entity.

        Invalidates cache and recomputes truth values.

        Args:
            entity_id: ID of the entity

        Returns:
            Fresh EntityTruth
        """
        # Invalidate cache
        if entity_id in self._cache:
            del self._cache[entity_id]

        return await self.get_entity_truth(entity_id, use_cache=False)

    async def propagate_resolution(
        self,
        entity_id: str,
        predicate: str,
        winning_claim_id: str,
    ) -> TruthValue:
        """
        Propagate a conflict resolution to update truth.

        Called after a conflict is resolved to update the
        truth layer accordingly.

        Args:
            entity_id: ID of the entity
            predicate: The attribute that was resolved
            winning_claim_id: The claim that won the resolution

        Returns:
            Updated TruthValue
        """
        # Invalidate cache for this entity
        if entity_id in self._cache:
            del self._cache[entity_id]

        # Get the winning claim details
        query = """
        MATCH (c:Claim {id: $claim_id})
        OPTIONAL MATCH (c)-[:SOURCED_FROM]->(s:Source)
        RETURN c.object_value as value, c.confidence as confidence,
               s.id as source_id
        """

        result = await self.client.execute_query(query, {"claim_id": winning_claim_id})

        if not result.records:
            raise ValueError(f"Winning claim {winning_claim_id} not found")

        record = result.records[0]

        # Create updated truth value
        truth_value = TruthValue(
            entity_id=entity_id,
            predicate=predicate,
            value=record["value"],
            confidence=record.get("confidence", 0.8),
            supporting_claim_ids=[winning_claim_id],
            source_count=1 if record.get("source_id") else 0,
            last_updated=datetime.utcnow(),
            version=1,  # Will be incremented if materialized
        )

        # Update materialized truth if enabled
        if self.config.enable_materialized_views:
            await self._update_materialized_truth(truth_value)

        logger.info(f"Propagated resolution for {entity_id}.{predicate}")
        return truth_value

    async def materialize_all_truths(self, limit: int = 1000) -> int:
        """
        Materialize truth values for all entities.

        Creates or updates Truth nodes in the graph for fast queries.

        Args:
            limit: Maximum entities to process

        Returns:
            Number of truth values materialized
        """
        # Get all entities
        query = """
        MATCH (e:Entity)
        RETURN e.id as entity_id
        LIMIT $limit
        """

        result = await self.client.execute_query(query, {"limit": limit})

        count = 0
        for record in result.records:
            entity_id = record["entity_id"]
            entity_truth = await self.get_entity_truth(entity_id, use_cache=False)

            if entity_truth:
                for predicate, truth_value in entity_truth.truths.items():
                    await self._update_materialized_truth(truth_value)
                    count += 1

        logger.info(f"Materialized {count} truth values")
        return count

    def _compute_truth_value(
        self,
        entity_id: str,
        predicate: str,
        claims: list[dict[str, Any]],
    ) -> Optional[TruthValue]:
        """
        Compute the truth value from a list of claims.

        For single-value predicates, selects the best claim.
        For multi-value predicates, aggregates all current values.

        Args:
            entity_id: ID of the entity
            predicate: The predicate
            claims: List of claim dictionaries

        Returns:
            Computed TruthValue or None
        """
        if not claims:
            return None

        is_single_value = predicate in self.config.single_value_predicates

        if is_single_value:
            # Select best claim by confidence and recency
            best_claim = max(
                claims,
                key=lambda c: (c.get("confidence", 0), c.get("created_at", ""))
            )

            source_ids = set()
            for c in claims:
                if c.get("source_id"):
                    source_ids.add(c["source_id"])

            return TruthValue(
                entity_id=entity_id,
                predicate=predicate,
                value=best_claim["value"],
                confidence=best_claim.get("confidence", 0.0),
                supporting_claim_ids=[best_claim["claim_id"]],
                source_count=len(source_ids),
                last_updated=datetime.utcnow(),
            )
        else:
            # Multi-value: aggregate all values
            values = []
            claim_ids = []
            source_ids = set()
            total_confidence = 0

            for claim in claims:
                values.append(claim["value"])
                claim_ids.append(claim["claim_id"])
                if claim.get("source_id"):
                    source_ids.add(claim["source_id"])
                total_confidence += claim.get("confidence", 0)

            avg_confidence = total_confidence / len(claims) if claims else 0

            return TruthValue(
                entity_id=entity_id,
                predicate=predicate,
                value=values if len(values) > 1 else values[0],
                confidence=avg_confidence,
                supporting_claim_ids=claim_ids,
                source_count=len(source_ids),
                last_updated=datetime.utcnow(),
            )

    async def _update_materialized_truth(self, truth_value: TruthValue) -> None:
        """
        Update or create a materialized Truth node.

        Args:
            truth_value: The truth value to materialize
        """
        query = """
        MERGE (t:Truth {entity_id: $entity_id, predicate: $predicate})
        ON CREATE SET
            t.id = $truth_id,
            t.value = $value,
            t.confidence = $confidence,
            t.supporting_claim_ids = $claim_ids,
            t.source_count = $source_count,
            t.created_at = datetime(),
            t.updated_at = datetime(),
            t.version = 1
        ON MATCH SET
            t.value = $value,
            t.confidence = $confidence,
            t.supporting_claim_ids = $claim_ids,
            t.source_count = $source_count,
            t.updated_at = datetime(),
            t.version = t.version + 1
        RETURN t.version as version
        """

        await self.client.execute_query(query, {
            "entity_id": truth_value.entity_id,
            "predicate": truth_value.predicate,
            "truth_id": f"truth_{uuid4().hex[:12]}",
            "value": str(truth_value.value),
            "confidence": truth_value.confidence,
            "claim_ids": truth_value.supporting_claim_ids,
            "source_count": truth_value.source_count,
        })

    def invalidate_cache(self, entity_id: Optional[str] = None) -> None:
        """
        Invalidate the truth cache.

        Args:
            entity_id: Specific entity to invalidate, or None for all
        """
        if entity_id:
            if entity_id in self._cache:
                del self._cache[entity_id]
        else:
            self._cache.clear()


# Pydantic models for API request/response

class TruthQueryRequest(BaseModel):
    """Request to query truth values."""
    entity_id: str
    predicate: Optional[str] = None
    as_of: Optional[datetime] = None


class TruthResponse(BaseModel):
    """Response containing truth value(s)."""
    entity_id: str
    entity_name: Optional[str] = None
    truths: dict[str, Any]
    has_conflicts: bool = False
    conflict_count: int = 0
    computed_at: datetime = Field(default_factory=datetime.utcnow)


class HistoryResponse(BaseModel):
    """Response containing truth history."""
    entity_id: str
    predicate: str
    history: list[dict[str, Any]]
    total_entries: int
