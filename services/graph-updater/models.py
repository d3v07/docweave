"""
Graph Updater Service Models
============================

This module provides additional Pydantic models and data structures
specific to the graph-updater service that are not covered by shared models.

These models extend the shared models with service-specific validation
and serialization logic for API requests and responses.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class BatchClaimRequest(BaseModel):
    """Request to add multiple claims in a batch."""
    claims: list[dict[str, Any]] = Field(..., min_length=1, max_length=1000)
    check_conflicts: bool = Field(default=True)
    auto_resolve: bool = Field(default=False)
    resolution_strategy: Optional[str] = Field(default=None)
    source_id: Optional[str] = Field(default=None)


class BatchClaimResponse(BaseModel):
    """Response from batch claim processing."""
    total_claims: int
    successful: int
    failed: int
    conflicts_detected: int
    claim_ids: list[str]
    errors: list[dict[str, Any]]
    processing_time_ms: float


class EntitySearchRequest(BaseModel):
    """Request to search for entities."""
    query: str = Field(..., min_length=1, max_length=500)
    entity_type: Optional[str] = Field(default=None)
    include_merged: bool = Field(default=False)
    limit: int = Field(default=20, ge=1, le=100)
    offset: int = Field(default=0, ge=0)


class EntitySearchResponse(BaseModel):
    """Response from entity search."""
    entities: list[dict[str, Any]]
    total: int
    has_more: bool


class GraphStatsResponse(BaseModel):
    """Response containing graph statistics."""
    entity_count: int
    claim_count: int
    source_count: int
    conflict_count: int
    truth_count: int
    resolution_count: int
    last_updated: datetime


class TruthComparisonRequest(BaseModel):
    """Request to compare truth values between time points."""
    entity_id: str
    predicate: Optional[str] = Field(default=None)
    time_point_1: datetime
    time_point_2: datetime


class TruthComparisonResponse(BaseModel):
    """Response from truth comparison."""
    entity_id: str
    changes: list[dict[str, Any]]
    added_predicates: list[str]
    removed_predicates: list[str]
    modified_predicates: list[str]


class ConflictAnalysisRequest(BaseModel):
    """Request for detailed conflict analysis."""
    conflict_id: str
    include_source_details: bool = Field(default=True)
    include_claim_history: bool = Field(default=True)


class ConflictAnalysisResponse(BaseModel):
    """Response from conflict analysis."""
    conflict_id: str
    conflict_type: str
    severity: str
    claims: list[dict[str, Any]]
    recommendation: Optional[str]
    confidence_scores: dict[str, float]
    source_reliability_scores: dict[str, float]


class ResolutionPreviewRequest(BaseModel):
    """Request to preview a resolution without applying it."""
    conflict_id: str
    strategy: str
    winning_claim_id: Optional[str] = Field(default=None)


class ResolutionPreviewResponse(BaseModel):
    """Response from resolution preview."""
    conflict_id: str
    proposed_winner: Optional[str]
    proposed_losers: list[str]
    confidence: float
    rationale: str
    side_effects: list[str]


class EntityRelationship(BaseModel):
    """Represents a relationship between entities."""
    source_entity_id: str
    target_entity_id: str
    relationship_type: str
    predicate: str
    confidence: float = Field(default=0.8, ge=0.0, le=1.0)
    claim_ids: list[str] = Field(default_factory=list)


class EntityGraph(BaseModel):
    """Represents a subgraph centered on an entity."""
    center_entity_id: str
    entities: list[dict[str, Any]]
    relationships: list[EntityRelationship]
    depth: int


class ClaimLineage(BaseModel):
    """Represents the lineage of a claim through supersessions."""
    claim_id: str
    version: int
    predecessors: list[str]
    successors: list[str]
    status: str
    created_at: datetime
    superseded_at: Optional[datetime] = None


class SourceImpactAnalysis(BaseModel):
    """Analysis of a source's impact on the knowledge graph."""
    source_id: str
    claim_count: int
    entity_count: int
    conflict_count: int
    superseded_count: int
    current_count: int
    reliability_trend: list[dict[str, Any]]


class BulkOperationStatus(BaseModel):
    """Status of a bulk operation in progress."""
    operation_id: str
    operation_type: str
    status: str
    total_items: int
    processed_items: int
    failed_items: int
    started_at: datetime
    estimated_completion: Optional[datetime] = None
    errors: list[str] = Field(default_factory=list)


class WebhookConfig(BaseModel):
    """Configuration for event webhooks."""
    url: str = Field(..., min_length=10, max_length=500)
    events: list[str] = Field(default_factory=lambda: ["conflict_detected", "resolution_applied"])
    secret: Optional[str] = Field(default=None, min_length=16, max_length=128)
    enabled: bool = Field(default=True)
    retry_count: int = Field(default=3, ge=0, le=10)

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        """Validate webhook URL."""
        if not v.startswith(("http://", "https://")):
            raise ValueError("URL must start with http:// or https://")
        return v


class MetricsSnapshot(BaseModel):
    """Snapshot of service metrics."""
    timestamp: datetime
    claims_processed_total: int
    claims_processed_per_minute: float
    conflicts_detected_total: int
    conflicts_resolved_total: int
    average_resolution_time_ms: float
    cache_hit_rate: float
    neo4j_query_latency_ms: float
    kafka_consumer_lag: int
