"""
Graph Relationship Models
=========================

This module defines the data models for representing relationships
and structures within the Neo4j knowledge graph, including nodes,
relationships, paths, query results, and conflict resolution.

The graph structure follows the pattern:
    (Entity)-[:HAS_CLAIM]->(Claim)-[:FROM_SOURCE]->(Source)
    (Entity)-[RELATES_TO {predicate}]->(Entity)
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional, Union
from uuid import UUID

from pydantic import BaseModel, Field


class RelationshipType(str, Enum):
    """
    Types of relationships in the knowledge graph.

    Attributes:
        HAS_CLAIM: Entity has an associated claim
        FROM_SOURCE: Claim originated from a source
        RELATES_TO: General relationship between entities
        MENTIONS: Source mentions an entity
        SUPERSEDES: Claim supersedes another claim
        CONFLICTS_WITH: Claim conflicts with another claim
        MERGED_INTO: Entity was merged into another
        DERIVED_FROM: Entity/Claim derived from another
        CONTAINS: Document contains parsed elements
        REFERENCES: Entity references another entity
    """
    HAS_CLAIM = "HAS_CLAIM"
    FROM_SOURCE = "FROM_SOURCE"
    RELATES_TO = "RELATES_TO"
    MENTIONS = "MENTIONS"
    SUPERSEDES = "SUPERSEDES"
    CONFLICTS_WITH = "CONFLICTS_WITH"
    MERGED_INTO = "MERGED_INTO"
    DERIVED_FROM = "DERIVED_FROM"
    CONTAINS = "CONTAINS"
    REFERENCES = "REFERENCES"


class ConflictStatus(str, Enum):
    """
    Status of a conflict between claims.

    Attributes:
        DETECTED: Conflict has been detected but not resolved
        UNDER_REVIEW: Conflict is being reviewed
        RESOLVED: Conflict has been resolved
        DEFERRED: Conflict resolution has been deferred
        IGNORED: Conflict has been marked as ignorable
    """
    DETECTED = "detected"
    UNDER_REVIEW = "under_review"
    RESOLVED = "resolved"
    DEFERRED = "deferred"
    IGNORED = "ignored"


@dataclass
class GraphNode:
    """
    Represents a node in the knowledge graph.

    This is a generic representation that can hold any type of node
    (Entity, Claim, Source, Document) for graph operations.

    Attributes:
        id: Unique identifier for the node
        labels: Neo4j labels (e.g., ["Entity", "Person"])
        properties: Node properties
        element_id: Neo4j internal element ID (for updates)
    """
    id: UUID
    labels: list[str]
    properties: dict[str, Any]
    element_id: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert node to dictionary for serialization."""
        return {
            "id": str(self.id),
            "labels": self.labels,
            "properties": self.properties,
            "element_id": self.element_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "GraphNode":
        """Create node from dictionary."""
        return cls(
            id=UUID(data["id"]) if isinstance(data["id"], str) else data["id"],
            labels=data.get("labels", []),
            properties=data.get("properties", {}),
            element_id=data.get("element_id"),
        )

    @classmethod
    def from_neo4j_node(cls, node: Any) -> "GraphNode":
        """Create GraphNode from a Neo4j node object."""
        properties = dict(node.items())
        node_id = properties.pop("id", None)
        if node_id is None:
            raise ValueError("Node must have an 'id' property")
        return cls(
            id=UUID(node_id) if isinstance(node_id, str) else node_id,
            labels=list(node.labels),
            properties=properties,
            element_id=node.element_id if hasattr(node, "element_id") else None,
        )


@dataclass
class GraphRelationship:
    """
    Represents a relationship in the knowledge graph.

    Attributes:
        id: Unique identifier for the relationship
        relationship_type: Type of the relationship
        source_id: ID of the source node
        target_id: ID of the target node
        properties: Relationship properties
        element_id: Neo4j internal element ID (for updates)
        created_at: Timestamp of relationship creation
    """
    id: UUID
    relationship_type: Union[RelationshipType, str]
    source_id: UUID
    target_id: UUID
    properties: dict[str, Any] = field(default_factory=dict)
    element_id: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict[str, Any]:
        """Convert relationship to dictionary for serialization."""
        rel_type = self.relationship_type.value if isinstance(self.relationship_type, RelationshipType) else self.relationship_type
        return {
            "id": str(self.id),
            "relationship_type": rel_type,
            "source_id": str(self.source_id),
            "target_id": str(self.target_id),
            "properties": self.properties,
            "element_id": self.element_id,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "GraphRelationship":
        """Create relationship from dictionary."""
        rel_type = data["relationship_type"]
        try:
            rel_type = RelationshipType(rel_type)
        except ValueError:
            pass  # Keep as string for custom relationship types

        return cls(
            id=UUID(data["id"]) if isinstance(data["id"], str) else data["id"],
            relationship_type=rel_type,
            source_id=UUID(data["source_id"]) if isinstance(data["source_id"], str) else data["source_id"],
            target_id=UUID(data["target_id"]) if isinstance(data["target_id"], str) else data["target_id"],
            properties=data.get("properties", {}),
            element_id=data.get("element_id"),
            created_at=datetime.fromisoformat(data["created_at"]) if isinstance(data.get("created_at"), str) else data.get("created_at", datetime.utcnow()),
        )


@dataclass
class GraphPath:
    """
    Represents a path through the knowledge graph.

    A path consists of alternating nodes and relationships,
    starting and ending with nodes.

    Attributes:
        nodes: List of nodes in the path
        relationships: List of relationships connecting the nodes
        start_node: First node in the path
        end_node: Last node in the path
        length: Number of relationships in the path
    """
    nodes: list[GraphNode]
    relationships: list[GraphRelationship]

    @property
    def start_node(self) -> Optional[GraphNode]:
        """Get the first node in the path."""
        return self.nodes[0] if self.nodes else None

    @property
    def end_node(self) -> Optional[GraphNode]:
        """Get the last node in the path."""
        return self.nodes[-1] if self.nodes else None

    @property
    def length(self) -> int:
        """Get the number of relationships in the path."""
        return len(self.relationships)

    def to_dict(self) -> dict[str, Any]:
        """Convert path to dictionary for serialization."""
        return {
            "nodes": [node.to_dict() for node in self.nodes],
            "relationships": [rel.to_dict() for rel in self.relationships],
            "length": self.length,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "GraphPath":
        """Create path from dictionary."""
        return cls(
            nodes=[GraphNode.from_dict(node) for node in data.get("nodes", [])],
            relationships=[GraphRelationship.from_dict(rel) for rel in data.get("relationships", [])],
        )


@dataclass
class GraphQueryResult:
    """
    Represents the result of a graph query.

    Provides a standardized structure for returning query results
    that may include nodes, relationships, paths, and aggregations.

    Attributes:
        nodes: List of nodes returned by the query
        relationships: List of relationships returned by the query
        paths: List of paths returned by the query
        records: Raw records for complex queries
        total_count: Total count of results (for pagination)
        execution_time_ms: Query execution time in milliseconds
        query: The query that was executed (for debugging)
    """
    nodes: list[GraphNode] = field(default_factory=list)
    relationships: list[GraphRelationship] = field(default_factory=list)
    paths: list[GraphPath] = field(default_factory=list)
    records: list[dict[str, Any]] = field(default_factory=list)
    total_count: Optional[int] = None
    execution_time_ms: Optional[float] = None
    query: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary for serialization."""
        return {
            "nodes": [node.to_dict() for node in self.nodes],
            "relationships": [rel.to_dict() for rel in self.relationships],
            "paths": [path.to_dict() for path in self.paths],
            "records": self.records,
            "total_count": self.total_count,
            "execution_time_ms": self.execution_time_ms,
            "query": self.query,
        }

    @property
    def is_empty(self) -> bool:
        """Check if the result is empty."""
        return not self.nodes and not self.relationships and not self.paths and not self.records


@dataclass
class ConflictResolution:
    """
    Represents a conflict resolution decision between claims.

    When two claims conflict, a resolution record tracks the
    decision-making process and outcome.

    Attributes:
        id: Unique identifier for the resolution
        claim_ids: IDs of the conflicting claims
        winning_claim_id: ID of the claim that was chosen
        losing_claim_ids: IDs of claims that were rejected
        resolution_method: How the conflict was resolved
        resolution_reason: Explanation of the resolution
        confidence: Confidence in the resolution
        resolved_by: User or system that resolved the conflict
        status: Current status of the resolution
        created_at: Timestamp of resolution creation
        resolved_at: Timestamp of when resolution was finalized
        metadata: Additional resolution metadata
    """
    id: UUID
    claim_ids: list[UUID]
    winning_claim_id: Optional[UUID] = None
    losing_claim_ids: list[UUID] = field(default_factory=list)
    resolution_method: str = "manual"
    resolution_reason: Optional[str] = None
    confidence: float = 0.0
    resolved_by: Optional[str] = None
    status: ConflictStatus = ConflictStatus.DETECTED
    created_at: datetime = field(default_factory=datetime.utcnow)
    resolved_at: Optional[datetime] = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert resolution to dictionary for serialization."""
        return {
            "id": str(self.id),
            "claim_ids": [str(uid) for uid in self.claim_ids],
            "winning_claim_id": str(self.winning_claim_id) if self.winning_claim_id else None,
            "losing_claim_ids": [str(uid) for uid in self.losing_claim_ids],
            "resolution_method": self.resolution_method,
            "resolution_reason": self.resolution_reason,
            "confidence": self.confidence,
            "resolved_by": self.resolved_by,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ConflictResolution":
        """Create resolution from dictionary."""
        return cls(
            id=UUID(data["id"]) if isinstance(data["id"], str) else data["id"],
            claim_ids=[UUID(uid) if isinstance(uid, str) else uid for uid in data.get("claim_ids", [])],
            winning_claim_id=UUID(data["winning_claim_id"]) if data.get("winning_claim_id") else None,
            losing_claim_ids=[UUID(uid) if isinstance(uid, str) else uid for uid in data.get("losing_claim_ids", [])],
            resolution_method=data.get("resolution_method", "manual"),
            resolution_reason=data.get("resolution_reason"),
            confidence=data.get("confidence", 0.0),
            resolved_by=data.get("resolved_by"),
            status=ConflictStatus(data.get("status", "detected")),
            created_at=datetime.fromisoformat(data["created_at"]) if isinstance(data.get("created_at"), str) else data.get("created_at", datetime.utcnow()),
            resolved_at=datetime.fromisoformat(data["resolved_at"]) if data.get("resolved_at") else None,
            metadata=data.get("metadata", {}),
        )

    def resolve(
        self,
        winning_claim_id: UUID,
        resolution_reason: str,
        resolved_by: str,
        confidence: float = 1.0,
    ) -> None:
        """
        Mark the conflict as resolved.

        Args:
            winning_claim_id: ID of the claim that was chosen
            resolution_reason: Explanation of why this claim was chosen
            resolved_by: User or system that made the decision
            confidence: Confidence in the resolution (0.0 to 1.0)
        """
        self.winning_claim_id = winning_claim_id
        self.losing_claim_ids = [cid for cid in self.claim_ids if cid != winning_claim_id]
        self.resolution_reason = resolution_reason
        self.resolved_by = resolved_by
        self.confidence = confidence
        self.status = ConflictStatus.RESOLVED
        self.resolved_at = datetime.utcnow()


# Pydantic models for API request/response validation

class GraphQueryRequest(BaseModel):
    """Request model for graph queries."""
    query: str = Field(..., min_length=1, max_length=10000)
    parameters: dict[str, Any] = Field(default_factory=dict)
    limit: int = Field(100, ge=1, le=1000)
    skip: int = Field(0, ge=0)


class ConflictResolutionRequest(BaseModel):
    """Request model for resolving conflicts."""
    winning_claim_id: UUID
    resolution_reason: str = Field(..., min_length=1, max_length=5000)
    confidence: float = Field(1.0, ge=0.0, le=1.0)
