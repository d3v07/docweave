"""
Graph Updater Service - Phase 4: Knowledge Graph and Conflict Resolution
=========================================================================

This service maintains the DocWeave knowledge graph and handles:
    - Entity management and merging
    - Claim processing with conflict detection
    - Conflict resolution with multiple strategies
    - Governed truth layer with history tracking
    - Background Kafka consumer for extracted claims

API Endpoints:
    - POST /entity/merge - Merge entities with aliases
    - POST /claim - Add claim with conflict check
    - GET /entity/{id}/truth - Get current truth
    - GET /entity/{id}/history - Get claim history
    - GET /conflicts - List unresolved conflicts
    - POST /conflicts/{id}/resolve - Resolve with strategy
    - GET /conflicts/{id}/audit - Resolution audit trail
"""

import asyncio
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Optional, Union

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator

from shared.config.settings import settings, get_settings

# Security imports - OWASP best practices
from services.graph_updater.security import (
    SecurityConfig,
    SecurityHeadersMiddleware,
    RateLimitMiddleware,
    InputSanitizer,
    verify_admin_api_key,
    SecureClaimRequest,
    SecureBatchClaimRequest,
    SecureEntityMergeRequest,
    SecureResolveConflictRequest,
)
from shared.utils.neo4j_client import Neo4jClient

from services.graph_updater.operations import (
    GraphOperations,
    EntityMergeRequest,
    EntityMergeResult,
    ClaimVersionInfo,
)
from services.graph_updater.conflict_detector import (
    ConflictDetector,
    ConflictDetectorConfig,
    ConflictType,
    DetectedConflict,
)
from services.graph_updater.conflict_resolver import (
    ConflictResolver,
    ConflictResolverConfig,
    ResolutionStrategy,
    ResolutionResult,
    ManualResolutionRequest,
    AutoResolutionRequest,
)
from services.graph_updater.truth_layer import (
    TruthLayer,
    TruthLayerConfig,
    EntityTruth,
    TruthValue,
)
from services.graph_updater.consumer import ClaimsConsumer, GraphUpdateConsumer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# =============================================================================
# Request/Response Models
# =============================================================================

class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    service: str
    timestamp: str
    version: str = "0.4.0"


class ReadinessResponse(BaseModel):
    """Readiness check response."""
    ready: bool
    checks: dict[str, Union[bool, str]]
    timestamp: str


class ClaimRequest(BaseModel):
    """
    Request to add a new claim.

    Security: All fields have strict validation to prevent injection attacks.
    """
    subject_entity_id: str = Field(
        ...,
        min_length=1,
        max_length=100,
        pattern=r'^[a-zA-Z0-9_-]+$',
        description="Entity ID (alphanumeric with underscores/hyphens)"
    )
    predicate: str = Field(
        ...,
        min_length=1,
        max_length=200,
        pattern=r'^[a-zA-Z][a-zA-Z0-9_]*$',
        description="Predicate in snake_case format"
    )
    object_value: Any
    source_id: str = Field(
        ...,
        min_length=1,
        max_length=200,
        description="Source document ID"
    )
    confidence: float = Field(default=0.8, ge=0.0, le=1.0)
    extracted_text: Optional[str] = Field(default=None, max_length=10000)
    object_entity_id: Optional[str] = Field(
        default=None,
        max_length=100,
        pattern=r'^[a-zA-Z0-9_-]*$'
    )
    valid_from: Optional[datetime] = None
    valid_until: Optional[datetime] = None
    check_conflicts: bool = Field(default=True)
    auto_supersede: bool = Field(default=False)

    model_config = {"extra": "forbid"}  # Reject unexpected fields

    @field_validator('object_value')
    @classmethod
    def validate_object_value(cls, v: Any) -> Any:
        """Validate object value to prevent injection."""
        if isinstance(v, str) and len(v) > 10000:
            raise ValueError("Value too long (max 10000 characters)")
        return v


class ClaimResponse(BaseModel):
    """Response after adding a claim."""
    claim_id: str
    status: str
    conflicts_detected: int
    conflicts: list[dict[str, Any]]
    superseded_claims: list[str]


class EntityTruthResponse(BaseModel):
    """Response containing entity truth."""
    entity_id: str
    entity_name: str
    entity_type: str
    truths: dict[str, Any]
    has_conflicts: bool
    conflict_count: int
    computed_at: str


class HistoryResponse(BaseModel):
    """Response containing claim history."""
    entity_id: str
    predicate: str
    history: list[dict[str, Any]]
    versions: list[ClaimVersionInfo]


class ConflictListResponse(BaseModel):
    """Response containing list of conflicts."""
    conflicts: list[dict[str, Any]]
    total: int
    has_more: bool


class ResolveConflictRequest(BaseModel):
    """
    Request to resolve a conflict.

    Security: Strict validation on all inputs.
    """
    strategy: Optional[ResolutionStrategy] = None
    winning_claim_id: Optional[str] = Field(
        default=None,
        max_length=100,
        pattern=r'^[a-zA-Z0-9_-]*$'
    )
    reason: Optional[str] = Field(default=None, max_length=2000)
    resolved_by: str = Field(
        default="api_user",
        max_length=100,
        pattern=r'^[a-zA-Z0-9_-]+$'
    )
    force: bool = Field(default=False)

    model_config = {"extra": "forbid"}


class ResolveConflictResponse(BaseModel):
    """Response after resolving a conflict."""
    conflict_id: str
    outcome: str
    winning_claim_id: Optional[str]
    losing_claim_ids: list[str]
    strategy_used: str
    reason: str
    confidence: float
    actions_taken: list[str]


class AuditTrailResponse(BaseModel):
    """Response containing audit trail."""
    conflict_id: str
    entries: list[dict[str, Any]]
    total_entries: int


class ConsumerStatsResponse(BaseModel):
    """Response containing consumer statistics."""
    claims_consumer: dict[str, Any]
    updates_consumer: Optional[dict[str, Any]]


class EntitySearchResponse(BaseModel):
    """Response containing entity search results."""
    entities: list[dict[str, Any]]
    total: int
    has_more: bool


class EntityGraphResponse(BaseModel):
    """Response containing entity graph data."""
    center_entity_id: str
    entities: list[dict[str, Any]]
    relationships: list[dict[str, Any]]
    depth: int


class GraphStatsResponse(BaseModel):
    """Response containing graph statistics."""
    entity_count: int
    claim_count: int
    source_count: int
    conflict_count: int
    truth_count: int
    resolution_count: int
    last_updated: str


class BatchClaimRequest(BaseModel):
    """
    Request to add multiple claims at once.

    Security: Limited batch size to prevent DoS.
    """
    claims: list[dict[str, Any]] = Field(
        ...,
        max_length=100,  # Max 100 claims per batch to prevent DoS
        description="List of claims (max 100 per batch)"
    )
    check_conflicts: bool = Field(default=True)
    auto_resolve: bool = Field(default=False)
    resolution_strategy: Optional[str] = Field(
        default=None,
        max_length=50,
        pattern=r'^[a-z_]*$'
    )
    source_id: Optional[str] = Field(default=None, max_length=200)

    model_config = {"extra": "forbid"}


class BatchClaimResponse(BaseModel):
    """Response after adding batch claims."""
    total_claims: int
    successful: int
    failed: int
    conflicts_detected: int
    claim_ids: list[str]
    errors: list[dict[str, Any]]
    processing_time_ms: float


# =============================================================================
# Global State
# =============================================================================

neo4j_client: Optional[Neo4jClient] = None
graph_ops: Optional[GraphOperations] = None
conflict_detector: Optional[ConflictDetector] = None
conflict_resolver: Optional[ConflictResolver] = None
truth_layer: Optional[TruthLayer] = None
claims_consumer: Optional[ClaimsConsumer] = None
updates_consumer: Optional[GraphUpdateConsumer] = None


# =============================================================================
# Application Lifecycle
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global neo4j_client, graph_ops, conflict_detector, conflict_resolver
    global truth_layer, claims_consumer, updates_consumer

    logger.info("Starting Graph Updater Service...")

    try:
        # Initialize Neo4j client
        neo4j_client = Neo4jClient()
        await neo4j_client.init()
        logger.info("Neo4j client initialized")

        # Initialize graph operations
        graph_ops = GraphOperations(neo4j_client)
        logger.info("Graph operations initialized")

        # Initialize conflict detector
        detector_config = ConflictDetectorConfig()
        conflict_detector = ConflictDetector(neo4j_client, detector_config)
        logger.info("Conflict detector initialized")

        # Initialize conflict resolver
        resolver_config = ConflictResolverConfig()
        conflict_resolver = ConflictResolver(neo4j_client, resolver_config)
        logger.info("Conflict resolver initialized")

        # Initialize truth layer
        truth_config = TruthLayerConfig()
        truth_layer = TruthLayer(neo4j_client, truth_config)
        logger.info("Truth layer initialized")

        # Initialize and start Kafka consumers
        claims_consumer = ClaimsConsumer(
            graph_operations=graph_ops,
            conflict_detector=conflict_detector,
            truth_layer=truth_layer,
            group_id="graph-updater-claims",
        )

        updates_consumer = GraphUpdateConsumer(
            graph_operations=graph_ops,
            truth_layer=truth_layer,
            group_id="graph-updater-updates",
        )

        # Start consumers in background (non-blocking)
        # Using asyncio.create_task to not block service startup
        async def start_consumers_with_delay():
            """Start consumers with a delay to allow Kafka to be fully ready."""
            # Wait a bit for Kafka to be fully available
            await asyncio.sleep(2)

            try:
                await claims_consumer.start()
                logger.info("Claims consumer started successfully")
            except Exception as e:
                logger.warning(f"Could not start claims consumer: {e}")

            try:
                await updates_consumer.start()
                logger.info("Updates consumer started successfully")
            except Exception as e:
                logger.warning(f"Could not start updates consumer: {e}")

        # Start consumer initialization in background
        asyncio.create_task(start_consumers_with_delay())

        logger.info("Graph Updater Service started successfully")
        yield

    except Exception as e:
        logger.error(f"Failed to start service: {e}", exc_info=True)
        raise

    finally:
        # Cleanup
        logger.info("Shutting down Graph Updater Service...")

        if claims_consumer:
            await claims_consumer.stop()

        if updates_consumer:
            await updates_consumer.stop()

        if neo4j_client:
            await neo4j_client.close()

        logger.info("Graph Updater Service shutdown complete")


# =============================================================================
# FastAPI Application
# =============================================================================

app = FastAPI(
    title="DocWeave Graph Updater Service",
    description="Phase 4: Knowledge Graph and Conflict Resolution",
    version="0.4.0",
    lifespan=lifespan,
    # Security: Disable docs in production if needed
    # docs_url=None if os.getenv("ENVIRONMENT") == "production" else "/docs",
    # redoc_url=None if os.getenv("ENVIRONMENT") == "production" else "/redoc",
)

# =============================================================================
# Security Middleware (Order matters - applied in reverse order)
# =============================================================================

# 1. Security headers (applied last, so headers are on all responses)
app.add_middleware(SecurityHeadersMiddleware)

# 2. Rate limiting (applied before request processing)
app.add_middleware(RateLimitMiddleware)

# 3. CORS middleware for frontend
# Security: Explicitly list allowed origins, methods, and headers
app.add_middleware(
    CORSMiddleware,
    allow_origins=SecurityConfig.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=[
        "Content-Type",
        "Authorization",
        "X-API-Key",
        "X-Request-ID",
    ],
    expose_headers=[
        "X-RateLimit-Limit",
        "X-RateLimit-Remaining",
        "X-RateLimit-Reset",
        "X-Request-ID",
    ],
)


# =============================================================================
# Health Endpoints
# =============================================================================

@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Check service health."""
    return HealthResponse(
        status="healthy",
        service="graph-updater",
        timestamp=datetime.utcnow().isoformat(),
        version="0.4.0",
    )


@app.get("/ready", response_model=ReadinessResponse, tags=["Health"])
async def readiness_check():
    """Check service readiness."""
    neo4j_ok = False
    if neo4j_client:
        try:
            neo4j_ok = await neo4j_client.health_check()
        except Exception:
            neo4j_ok = False

    # Check Kafka consumer status using the new status field
    kafka_ok = False
    kafka_status = "not_initialized"
    if claims_consumer:
        kafka_ok = claims_consumer._status == ClaimsConsumer.STATUS_RUNNING
        kafka_status = claims_consumer._status

    return ReadinessResponse(
        ready=neo4j_ok,
        checks={
            "neo4j": neo4j_ok,
            "kafka_consumer": kafka_ok,
            "kafka_consumer_status": kafka_status,
            "graph_operations": graph_ops is not None,
            "conflict_detector": conflict_detector is not None,
            "truth_layer": truth_layer is not None,
        },
        timestamp=datetime.utcnow().isoformat(),
    )


# =============================================================================
# Entity Endpoints
# =============================================================================

@app.post("/entity", tags=["Entities"])
async def create_entity(
    name: str = Query(..., min_length=1, max_length=500),
    entity_type: str = Query(..., min_length=1, max_length=100, pattern=r'^[A-Z_]+$'),
    aliases: Optional[list[str]] = Query(default=None, max_length=50),
):
    """
    Create or merge an entity in the graph.

    Security: All inputs are validated and sanitized.
    """
    if not graph_ops:
        raise HTTPException(status_code=503, detail="Graph operations not available")

    # Sanitize inputs
    name = InputSanitizer.sanitize_string(name, max_length=500)
    sanitized_aliases = []
    if aliases:
        for alias in aliases[:50]:  # Max 50 aliases
            sanitized_aliases.append(
                InputSanitizer.sanitize_string(alias, max_length=500)
            )

    entity_id = await graph_ops.merge_entity(name, entity_type, sanitized_aliases)
    return {
        "id": entity_id,
        "name": name,
        "type": entity_type,
        "aliases": sanitized_aliases,
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
    }


@app.get("/entity/search", tags=["Entities"])
async def search_entities(
    query: str = Query(default="*", max_length=1000),
    entity_type: Optional[str] = Query(default=None, max_length=100, pattern=r'^[a-zA-Z_]*$'),
    include_merged: bool = Query(default=False),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0, le=10000),
):
    """
    Search for entities by name, type, or aliases.

    Args:
        query: Search query (use * for all, max 1000 chars)
        entity_type: Filter by entity type (e.g., PERSON, ORGANIZATION)
        include_merged: Include merged entities
        limit: Maximum results to return (1-200)
        offset: Pagination offset (0-10000)

    Security: Query is sanitized to prevent injection.
    """
    if not neo4j_client:
        raise HTTPException(status_code=503, detail="Database not available")

    # Sanitize query input
    if query != "*":
        query = InputSanitizer.sanitize_string(query, max_length=1000)

    # Build the query based on search parameters
    if query == "*":
        # Return all entities
        cypher_query = """
        MATCH (e:Entity)
        WHERE ($entity_type IS NULL OR e.type = $entity_type)
        AND ($include_merged = true OR NOT exists(e.merged_into))
        RETURN e.id as id, e.name as name, e.type as type,
               e.aliases as aliases, e.created_at as created_at,
               e.updated_at as updated_at
        ORDER BY e.name
        SKIP $offset
        LIMIT $limit
        """
    else:
        # Search by name or aliases
        cypher_query = """
        MATCH (e:Entity)
        WHERE (toLower(e.name) CONTAINS toLower($query)
               OR any(alias IN e.aliases WHERE toLower(alias) CONTAINS toLower($query)))
        AND ($entity_type IS NULL OR e.type = $entity_type)
        AND ($include_merged = true OR NOT exists(e.merged_into))
        RETURN e.id as id, e.name as name, e.type as type,
               e.aliases as aliases, e.created_at as created_at,
               e.updated_at as updated_at
        ORDER BY e.name
        SKIP $offset
        LIMIT $limit
        """

    params = {
        "query": query,
        "entity_type": entity_type,
        "include_merged": include_merged,
        "offset": offset,
        "limit": limit + 1,  # Get one extra to check for more
    }

    result = await neo4j_client.execute_query(cypher_query, params)

    entities = []
    for record in result.records[:limit]:
        entities.append({
            "id": record.get("id"),
            "name": record.get("name"),
            "type": record.get("type"),
            "aliases": record.get("aliases") or [],
            "created_at": record.get("created_at", datetime.utcnow().isoformat()),
            "updated_at": record.get("updated_at", datetime.utcnow().isoformat()),
        })

    has_more = len(result.records) > limit

    # Get total count
    count_query = """
    MATCH (e:Entity)
    WHERE ($query = '*' OR toLower(e.name) CONTAINS toLower($query)
           OR any(alias IN e.aliases WHERE toLower(alias) CONTAINS toLower($query)))
    AND ($entity_type IS NULL OR e.type = $entity_type)
    AND ($include_merged = true OR NOT exists(e.merged_into))
    RETURN count(e) as total
    """
    count_result = await neo4j_client.execute_query(count_query, {
        "query": query,
        "entity_type": entity_type,
        "include_merged": include_merged,
    })
    total = count_result.records[0].get("total", 0) if count_result.records else 0

    return {
        "entities": entities,
        "total": total,
        "has_more": has_more,
    }


@app.get("/entity/{entity_id}/graph", tags=["Entities"])
async def get_entity_graph(
    entity_id: str,
    depth: int = Query(default=2, ge=1, le=5),
):
    """
    Get the graph of relationships around an entity.

    Returns entities and their relationships up to the specified depth.

    Security: Entity ID is validated to prevent injection.
    """
    if not neo4j_client:
        raise HTTPException(status_code=503, detail="Database not available")

    # Validate entity_id format
    entity_id = InputSanitizer.sanitize_entity_id(entity_id)

    # Get the center entity first
    center_query = """
    MATCH (e:Entity {id: $entity_id})
    RETURN e.id as id, e.name as name, e.type as type,
           e.aliases as aliases, e.created_at as created_at,
           e.updated_at as updated_at
    """
    center_result = await neo4j_client.execute_query(center_query, {"entity_id": entity_id})

    if not center_result.records:
        raise HTTPException(status_code=404, detail=f"Entity {entity_id} not found")

    # Get related entities and relationships up to depth
    graph_query = """
    MATCH path = (center:Entity {id: $entity_id})-[r*1..""" + str(depth) + """]-(related:Entity)
    WITH center, related, relationships(path) as rels
    UNWIND rels as rel
    WITH center, related, rel
    MATCH (source:Entity)-[rel]->(target:Entity)
    RETURN DISTINCT
        related.id as entity_id, related.name as entity_name,
        related.type as entity_type, related.aliases as aliases,
        related.created_at as created_at, related.updated_at as updated_at,
        source.id as source_entity_id, target.id as target_entity_id,
        type(rel) as relationship_type,
        CASE WHEN rel.predicate IS NOT NULL THEN rel.predicate ELSE type(rel) END as predicate,
        COALESCE(rel.confidence, 1.0) as confidence,
        COALESCE(rel.claim_ids, []) as claim_ids
    LIMIT 100
    """

    graph_result = await neo4j_client.execute_query(graph_query, {"entity_id": entity_id})

    # Build entities set and relationships list
    entities_map = {}
    relationships = []

    # Add center entity
    center_record = center_result.records[0]
    entities_map[entity_id] = {
        "id": center_record.get("id"),
        "name": center_record.get("name"),
        "type": center_record.get("type"),
        "aliases": center_record.get("aliases") or [],
        "created_at": center_record.get("created_at", datetime.utcnow().isoformat()),
        "updated_at": center_record.get("updated_at", datetime.utcnow().isoformat()),
    }

    # Process graph results
    seen_relationships = set()
    for record in graph_result.records:
        # Add entity
        ent_id = record.get("entity_id")
        if ent_id and ent_id not in entities_map:
            entities_map[ent_id] = {
                "id": ent_id,
                "name": record.get("entity_name"),
                "type": record.get("entity_type"),
                "aliases": record.get("aliases") or [],
                "created_at": record.get("created_at", datetime.utcnow().isoformat()),
                "updated_at": record.get("updated_at", datetime.utcnow().isoformat()),
            }

        # Add relationship
        source_id = record.get("source_entity_id")
        target_id = record.get("target_entity_id")
        rel_type = record.get("relationship_type")

        if source_id and target_id:
            rel_key = f"{source_id}-{rel_type}-{target_id}"
            if rel_key not in seen_relationships:
                seen_relationships.add(rel_key)
                relationships.append({
                    "source_entity_id": source_id,
                    "target_entity_id": target_id,
                    "relationship_type": rel_type,
                    "predicate": record.get("predicate", rel_type),
                    "confidence": record.get("confidence", 1.0),
                    "claim_ids": record.get("claim_ids", []),
                })

    return {
        "center_entity_id": entity_id,
        "entities": list(entities_map.values()),
        "relationships": relationships,
        "depth": depth,
    }


@app.post("/entity/merge", response_model=EntityMergeResult, tags=["Entities"])
async def merge_entities(request: EntityMergeRequest):
    """
    Merge multiple entities into a target entity.

    This operation:
    - Combines aliases from all source entities
    - Transfers all claims to the target entity
    - Updates relationships pointing to source entities
    - Marks source entities as merged
    """
    if not graph_ops:
        raise HTTPException(status_code=503, detail="Graph operations not available")

    result = await graph_ops.merge_entities(
        target_entity_id=request.target_entity_id,
        source_entity_ids=request.source_entity_ids,
        merge_aliases=request.merge_aliases,
        merge_claims=request.merge_claims,
        resolved_by=request.resolved_by,
    )

    if not result.success:
        raise HTTPException(status_code=400, detail=result.message)

    return result


@app.get("/entity/{entity_id}/truth", response_model=EntityTruthResponse, tags=["Truth"])
async def get_entity_truth(entity_id: str):
    """
    Get the current truth for an entity.

    Returns the best-known values for all attributes of the entity,
    computed from current claims with conflict awareness.

    Security: Entity ID is validated.
    """
    # Validate entity_id format
    entity_id = InputSanitizer.sanitize_entity_id(entity_id)

    if not truth_layer:
        raise HTTPException(status_code=503, detail="Truth layer not available")

    entity_truth = await truth_layer.get_entity_truth(entity_id)

    if not entity_truth:
        raise HTTPException(status_code=404, detail=f"Entity {entity_id} not found")

    return EntityTruthResponse(
        entity_id=entity_truth.entity_id,
        entity_name=entity_truth.entity_name,
        entity_type=entity_truth.entity_type,
        truths={k: v.to_dict() for k, v in entity_truth.truths.items()},
        has_conflicts=entity_truth.has_conflicts,
        conflict_count=entity_truth.conflict_count,
        computed_at=entity_truth.computed_at.isoformat(),
    )


@app.get("/entity/{entity_id}/history", response_model=HistoryResponse, tags=["Truth"])
async def get_entity_history(
    entity_id: str,
    predicate: str = Query(..., max_length=200, pattern=r'^[a-zA-Z][a-zA-Z0-9_]*$'),
    include_superseded: bool = Query(default=True),
):
    """
    Get the claim history for an entity attribute.

    Shows how the value has changed over time, including
    superseded claims and version information.

    Security: All inputs are validated.
    """
    # Validate inputs
    entity_id = InputSanitizer.sanitize_entity_id(entity_id)
    predicate = InputSanitizer.sanitize_predicate(predicate)

    if not graph_ops:
        raise HTTPException(status_code=503, detail="Graph operations not available")

    history = await graph_ops.get_claim_history(entity_id, predicate, include_superseded)
    versions = await graph_ops.get_claim_versions(entity_id, predicate)

    return HistoryResponse(
        entity_id=entity_id,
        predicate=predicate,
        history=history,
        versions=versions,
    )


@app.get("/entity/{entity_id}/truth/at", tags=["Truth"])
async def get_truth_at_time(
    entity_id: str,
    predicate: str,
    as_of: datetime,
):
    """
    Get the truth value at a specific point in time.

    Useful for historical queries and auditing.
    """
    if not truth_layer:
        raise HTTPException(status_code=503, detail="Truth layer not available")

    historical = await truth_layer.get_truth_at_time(entity_id, predicate, as_of)

    if not historical:
        raise HTTPException(
            status_code=404,
            detail=f"No truth found for {entity_id}.{predicate} at {as_of}"
        )

    return historical.to_dict()


# =============================================================================
# Claim Endpoints
# =============================================================================

@app.post("/claim", response_model=ClaimResponse, tags=["Claims"])
async def add_claim(request: ClaimRequest):
    """
    Add a new claim to the knowledge graph.

    Optionally checks for conflicts with existing claims and
    can auto-supersede older claims for the same predicate.
    """
    if not graph_ops:
        raise HTTPException(status_code=503, detail="Graph operations not available")

    conflicts = []
    superseded_claims = []
    claim_id = ""

    if request.auto_supersede:
        # Add with versioning (supersede existing)
        claim_id, superseded_claims = await graph_ops.add_claim_versioned(
            subject_entity_id=request.subject_entity_id,
            predicate=request.predicate,
            object_value=request.object_value,
            source_id=request.source_id,
            confidence=request.confidence,
            extracted_text=request.extracted_text or "",
            supersede_existing=True,
        )
        status = "added_with_supersede"
    elif request.check_conflicts:
        # Add with conflict check
        claim_id, conflict_records = await graph_ops.add_claim_with_conflict_check(
            subject_entity_id=request.subject_entity_id,
            predicate=request.predicate,
            object_value=request.object_value,
            source_id=request.source_id,
            confidence=request.confidence,
            extracted_text=request.extracted_text or "",
        )
        conflicts = conflict_records
        status = "conflicting" if conflicts else "current"
    else:
        # Simple add without checks
        claim_id = await graph_ops.add_claim(
            subject_entity_id=request.subject_entity_id,
            predicate=request.predicate,
            object_value=request.object_value,
            source_id=request.source_id,
            confidence=request.confidence,
            extracted_text=request.extracted_text or "",
            object_entity_id=request.object_entity_id,
            valid_from=request.valid_from,
            valid_until=request.valid_until,
        )
        status = "added"

    # Invalidate truth cache
    if truth_layer:
        truth_layer.invalidate_cache(request.subject_entity_id)

    return ClaimResponse(
        claim_id=claim_id,
        status=status,
        conflicts_detected=len(conflicts),
        conflicts=conflicts,
        superseded_claims=superseded_claims,
    )


@app.get("/claim/{claim_id}", tags=["Claims"])
async def get_claim(claim_id: str):
    """
    Get details of a specific claim.

    Security: Claim ID is validated.
    """
    # Validate claim_id format
    if not re.match(r'^[a-zA-Z0-9_-]+$', claim_id) or len(claim_id) > 100:
        raise HTTPException(status_code=400, detail="Invalid claim ID format")

    if not neo4j_client:
        raise HTTPException(status_code=503, detail="Database not available")

    query = """
    MATCH (c:Claim {id: $claim_id})
    MATCH (e:Entity)-[:HAS_CLAIM]->(c)
    OPTIONAL MATCH (c)-[:SOURCED_FROM]->(s:Source)
    OPTIONAL MATCH (c)-[:SUPERSEDES]->(older:Claim)
    OPTIONAL MATCH (newer:Claim)-[:SUPERSEDES]->(c)
    RETURN c, e.id as entity_id, e.name as entity_name,
           s.id as source_id, s.uri as source_uri,
           older.id as supersedes_id, newer.id as superseded_by
    """

    result = await neo4j_client.execute_query(query, {"claim_id": claim_id})

    if not result.records:
        raise HTTPException(status_code=404, detail=f"Claim {claim_id} not found")

    return result.records[0]


@app.post("/claim/batch", response_model=BatchClaimResponse, tags=["Claims"])
async def add_batch_claims(request: BatchClaimRequest):
    """
    Add multiple claims to the knowledge graph in a batch.

    Optionally checks for conflicts and can auto-resolve using the specified strategy.
    """
    import time
    start_time = time.time()

    if not graph_ops:
        raise HTTPException(status_code=503, detail="Graph operations not available")

    successful = 0
    failed = 0
    conflicts_detected = 0
    claim_ids = []
    errors = []

    for i, claim_data in enumerate(request.claims):
        try:
            # Get source_id from claim or request default
            source_id = claim_data.get("source_id") or request.source_id
            if not source_id:
                errors.append({"index": i, "error": "Missing source_id"})
                failed += 1
                continue

            # Validate required fields
            subject_entity_id = claim_data.get("subject_entity_id")
            predicate = claim_data.get("predicate")
            object_value = claim_data.get("object_value")

            if not all([subject_entity_id, predicate, object_value is not None]):
                errors.append({"index": i, "error": "Missing required fields"})
                failed += 1
                continue

            confidence = claim_data.get("confidence", 0.8)

            if request.check_conflicts:
                claim_id, conflict_records = await graph_ops.add_claim_with_conflict_check(
                    subject_entity_id=subject_entity_id,
                    predicate=predicate,
                    object_value=object_value,
                    source_id=source_id,
                    confidence=confidence,
                    extracted_text=claim_data.get("extracted_text", ""),
                )
                conflicts_detected += len(conflict_records)
            else:
                claim_id = await graph_ops.add_claim(
                    subject_entity_id=subject_entity_id,
                    predicate=predicate,
                    object_value=object_value,
                    source_id=source_id,
                    confidence=confidence,
                    extracted_text=claim_data.get("extracted_text", ""),
                )

            claim_ids.append(claim_id)
            successful += 1

            # Invalidate truth cache
            if truth_layer:
                truth_layer.invalidate_cache(subject_entity_id)

        except Exception as e:
            errors.append({"index": i, "error": str(e)})
            failed += 1

    processing_time_ms = (time.time() - start_time) * 1000

    return BatchClaimResponse(
        total_claims=len(request.claims),
        successful=successful,
        failed=failed,
        conflicts_detected=conflicts_detected,
        claim_ids=claim_ids,
        errors=errors,
        processing_time_ms=processing_time_ms,
    )


# =============================================================================
# Conflict Endpoints
# =============================================================================

@app.get("/conflicts", response_model=ConflictListResponse, tags=["Conflicts"])
async def list_conflicts(
    entity_id: Optional[str] = None,
    conflict_type: Optional[ConflictType] = None,
    limit: int = Query(default=100, le=500),
    offset: int = Query(default=0, ge=0),
):
    """
    List unresolved conflicts in the knowledge graph.

    Can filter by entity or conflict type.
    """
    if not graph_ops:
        raise HTTPException(status_code=503, detail="Graph operations not available")

    conflicts = await graph_ops.get_unresolved_conflicts(
        entity_id=entity_id,
        limit=limit + 1,  # Get one extra to check if there are more
    )

    # Filter by conflict type if specified
    if conflict_type:
        conflicts = [c for c in conflicts if c.get("conflict_type") == conflict_type.value]

    # Apply offset
    conflicts = conflicts[offset:]

    # Check if there are more results
    has_more = len(conflicts) > limit
    conflicts = conflicts[:limit]

    return ConflictListResponse(
        conflicts=conflicts,
        total=len(conflicts),
        has_more=has_more,
    )


@app.get("/conflicts/{conflict_id}", tags=["Conflicts"])
async def get_conflict(conflict_id: str):
    """
    Get details of a specific conflict.

    Security: Conflict ID is validated.
    """
    # Validate conflict_id format (same format as entity IDs)
    if not re.match(r'^[a-zA-Z0-9_-]+$', conflict_id) or len(conflict_id) > 100:
        raise HTTPException(status_code=400, detail="Invalid conflict ID format")

    if not neo4j_client:
        raise HTTPException(status_code=503, detail="Database not available")

    query = """
    MATCH (c1:Claim)-[r:CONFLICTS_WITH {id: $conflict_id}]->(c2:Claim)
    MATCH (e:Entity)-[:HAS_CLAIM]->(c1)
    OPTIONAL MATCH (c1)-[:SOURCED_FROM]->(s1:Source)
    OPTIONAL MATCH (c2)-[:SOURCED_FROM]->(s2:Source)
    RETURN r.id as conflict_id, r.status as status,
           r.conflict_type as conflict_type, r.created_at as detected_at,
           c1.id as claim1_id, c1.predicate as predicate,
           c1.object_value as value1, c1.confidence as confidence1,
           c2.id as claim2_id, c2.object_value as value2, c2.confidence as confidence2,
           e.id as entity_id, e.name as entity_name,
           s1.reliability_score as reliability1, s2.reliability_score as reliability2
    """

    result = await neo4j_client.execute_query(query, {"conflict_id": conflict_id})

    if not result.records:
        raise HTTPException(status_code=404, detail=f"Conflict {conflict_id} not found")

    return result.records[0]


@app.post("/conflicts/{conflict_id}/resolve", response_model=ResolveConflictResponse, tags=["Conflicts"])
async def resolve_conflict(
    conflict_id: str,
    request: ResolveConflictRequest,
):
    """
    Resolve a conflict using the specified strategy.

    Strategies:
    - TIMESTAMP_BASED: Newer claim wins
    - SOURCE_RELIABILITY: Higher reliability source wins
    - CONFIDENCE_BASED: Higher confidence claim wins
    - MULTI_SOURCE_VOTING: Majority of sources wins
    - WEIGHTED_COMBINATION: Combined scoring (default)
    - MANUAL_REVIEW: Requires winning_claim_id to be specified

    Security: All inputs are validated. Resolution is audited.
    """
    # Validate conflict_id format
    if not re.match(r'^[a-zA-Z0-9_-]+$', conflict_id) or len(conflict_id) > 100:
        raise HTTPException(status_code=400, detail="Invalid conflict ID format")

    if not conflict_resolver:
        raise HTTPException(status_code=503, detail="Conflict resolver not available")

    # Get conflict details
    conflict_data = await _get_conflict_claim_data(conflict_id)
    if not conflict_data:
        raise HTTPException(status_code=404, detail=f"Conflict {conflict_id} not found")

    # If manual resolution with winning claim specified
    if request.winning_claim_id and request.reason:
        result = await conflict_resolver.resolve_by_manual_selection(
            conflict_id=conflict_id,
            winning_claim_id=request.winning_claim_id,
            reason=request.reason,
            resolved_by=request.resolved_by,
        )
    else:
        # Automatic resolution
        result = await conflict_resolver.resolve_conflict(
            conflict_id=conflict_id,
            claim_data=conflict_data,
            strategy=request.strategy,
            resolved_by=request.resolved_by,
            force_resolution=request.force,
        )

    # Update truth layer
    if truth_layer and result.winning_claim_id:
        entity_id = conflict_data[0].get("entity_id") if conflict_data else None
        if entity_id:
            truth_layer.invalidate_cache(entity_id)

    return ResolveConflictResponse(
        conflict_id=result.conflict_id,
        outcome=result.outcome.value,
        winning_claim_id=result.winning_claim_id,
        losing_claim_ids=result.losing_claim_ids,
        strategy_used=result.strategy_used.value,
        reason=result.reason,
        confidence=result.confidence,
        actions_taken=result.actions_taken,
    )


@app.get("/conflicts/{conflict_id}/audit", response_model=AuditTrailResponse, tags=["Conflicts"])
async def get_conflict_audit(conflict_id: str):
    """
    Get the audit trail for a conflict.

    Shows all resolution attempts and decisions made.
    """
    if not conflict_resolver:
        raise HTTPException(status_code=503, detail="Conflict resolver not available")

    entries = await conflict_resolver.get_audit_trail(conflict_id=conflict_id)

    return AuditTrailResponse(
        conflict_id=conflict_id,
        entries=[e.to_dict() for e in entries],
        total_entries=len(entries),
    )


class ResolutionPreviewRequest(BaseModel):
    """Request to preview a resolution."""
    conflict_id: str
    strategy: str
    winning_claim_id: Optional[str] = None


class ResolutionPreviewResponse(BaseModel):
    """Response containing resolution preview."""
    conflict_id: str
    proposed_winner: Optional[str]
    proposed_losers: list[str]
    confidence: float
    rationale: str
    side_effects: list[str]


@app.post("/conflicts/preview", response_model=ResolutionPreviewResponse, tags=["Conflicts"])
async def preview_resolution(request: ResolutionPreviewRequest):
    """
    Preview the outcome of a conflict resolution strategy.

    Shows what would happen without actually resolving the conflict.
    """
    if not conflict_resolver:
        raise HTTPException(status_code=503, detail="Conflict resolver not available")

    # Get conflict details
    conflict_data = await _get_conflict_claim_data(request.conflict_id)
    if not conflict_data:
        raise HTTPException(status_code=404, detail=f"Conflict {request.conflict_id} not found")

    # If winning_claim_id specified, use manual preview
    if request.winning_claim_id:
        proposed_winner = request.winning_claim_id
        proposed_losers = [c.get("id") for c in conflict_data if c.get("id") != request.winning_claim_id]
        confidence = 1.0
        rationale = "Manual selection"
    else:
        # Simulate strategy resolution
        strategy = ResolutionStrategy(request.strategy) if request.strategy else ResolutionStrategy.WEIGHTED_COMBINATION

        # Determine winner based on strategy
        if strategy == ResolutionStrategy.TIMESTAMP_BASED:
            # Newer wins
            sorted_claims = sorted(conflict_data, key=lambda c: c.get("timestamp", ""), reverse=True)
            proposed_winner = sorted_claims[0].get("id") if sorted_claims else None
            rationale = "Most recent claim would win based on timestamp"
            confidence = 0.85
        elif strategy == ResolutionStrategy.CONFIDENCE_BASED:
            # Higher confidence wins
            sorted_claims = sorted(conflict_data, key=lambda c: c.get("confidence", 0), reverse=True)
            proposed_winner = sorted_claims[0].get("id") if sorted_claims else None
            rationale = f"Highest confidence claim ({sorted_claims[0].get('confidence', 0):.2f}) would win"
            confidence = sorted_claims[0].get("confidence", 0.8) if sorted_claims else 0.8
        elif strategy == ResolutionStrategy.SOURCE_RELIABILITY:
            # Higher reliability wins
            sorted_claims = sorted(conflict_data, key=lambda c: c.get("source_reliability", 0.5), reverse=True)
            proposed_winner = sorted_claims[0].get("id") if sorted_claims else None
            rationale = f"Most reliable source ({sorted_claims[0].get('source_reliability', 0.5):.2f}) would win"
            confidence = sorted_claims[0].get("source_reliability", 0.8) if sorted_claims else 0.8
        else:
            # Default: weighted combination
            proposed_winner = conflict_data[0].get("id") if conflict_data else None
            rationale = "Weighted combination of factors would be used"
            confidence = 0.75

        proposed_losers = [c.get("id") for c in conflict_data if c.get("id") != proposed_winner]

    side_effects = [
        f"Losing claim(s) will be marked as superseded",
        f"Truth layer will be updated for the entity",
        f"Audit trail will be recorded",
    ]

    return ResolutionPreviewResponse(
        conflict_id=request.conflict_id,
        proposed_winner=proposed_winner,
        proposed_losers=proposed_losers,
        confidence=confidence,
        rationale=rationale,
        side_effects=side_effects,
    )


async def _get_conflict_claim_data(conflict_id: str) -> list[dict[str, Any]]:
    """Helper to get claim data for a conflict."""
    if not neo4j_client:
        return []

    query = """
    MATCH (c1:Claim)-[r:CONFLICTS_WITH {id: $conflict_id}]->(c2:Claim)
    MATCH (e:Entity)-[:HAS_CLAIM]->(c1)
    OPTIONAL MATCH (c1)-[:SOURCED_FROM]->(s1:Source)
    OPTIONAL MATCH (c2)-[:SOURCED_FROM]->(s2:Source)
    RETURN c1.id as id, c1.object_value as object_value,
           c1.confidence as confidence, c1.created_at as timestamp,
           s1.reliability_score as source_reliability, e.id as entity_id
    UNION
    MATCH (c1:Claim)-[r:CONFLICTS_WITH {id: $conflict_id}]->(c2:Claim)
    MATCH (e:Entity)-[:HAS_CLAIM]->(c2)
    OPTIONAL MATCH (c2)-[:SOURCED_FROM]->(s2:Source)
    RETURN c2.id as id, c2.object_value as object_value,
           c2.confidence as confidence, c2.created_at as timestamp,
           s2.reliability_score as source_reliability, e.id as entity_id
    """

    result = await neo4j_client.execute_query(query, {"conflict_id": conflict_id})
    return result.records


# =============================================================================
# Admin Endpoints
# =============================================================================

@app.get("/admin/stats", tags=["Admin"])
async def get_graph_stats(
    _: bool = Depends(verify_admin_api_key)
):
    """
    Get statistics about the knowledge graph.

    Security: Requires admin API key.
    """
    if not neo4j_client:
        raise HTTPException(status_code=503, detail="Database not available")

    stats_query = """
    MATCH (e:Entity) WITH count(e) as entity_count
    MATCH (c:Claim) WITH entity_count, count(c) as claim_count
    MATCH (s:Source) WITH entity_count, claim_count, count(s) as source_count
    OPTIONAL MATCH (c1:Claim)-[r:CONFLICTS_WITH {status: 'unresolved'}]->(c2:Claim)
    WITH entity_count, claim_count, source_count, count(DISTINCT r) as conflict_count
    OPTIONAL MATCH (t:Truth)
    WITH entity_count, claim_count, source_count, conflict_count, count(t) as truth_count
    OPTIONAL MATCH (c1:Claim)-[r:CONFLICTS_WITH {status: 'resolved'}]->(c2:Claim)
    RETURN entity_count, claim_count, source_count, conflict_count, truth_count,
           count(DISTINCT r) as resolution_count
    """

    try:
        result = await neo4j_client.execute_query(stats_query, {})

        if result.records:
            record = result.records[0]
            return {
                "entity_count": record.get("entity_count", 0),
                "claim_count": record.get("claim_count", 0),
                "source_count": record.get("source_count", 0),
                "conflict_count": record.get("conflict_count", 0),
                "truth_count": record.get("truth_count", 0),
                "resolution_count": record.get("resolution_count", 0),
                "last_updated": datetime.utcnow().isoformat(),
            }
    except Exception as e:
        logger.warning(f"Could not fetch graph stats: {e}")

    # Return empty stats on error
    return {
        "entity_count": 0,
        "claim_count": 0,
        "source_count": 0,
        "conflict_count": 0,
        "truth_count": 0,
        "resolution_count": 0,
        "last_updated": datetime.utcnow().isoformat(),
    }


@app.get("/admin/consumer/stats", response_model=ConsumerStatsResponse, tags=["Admin"])
async def get_consumer_stats(
    _: bool = Depends(verify_admin_api_key)
):
    """
    Get statistics for Kafka consumers.

    Security: Requires admin API key.
    """
    claims_stats = claims_consumer.get_stats() if claims_consumer else {"running": False, "status": "not_initialized"}
    updates_stats = updates_consumer.get_stats() if updates_consumer else {"running": False, "status": "not_initialized"}

    return ConsumerStatsResponse(
        claims_consumer=claims_stats,
        updates_consumer=updates_stats,
    )


@app.post("/admin/consumer/restart", tags=["Admin"])
async def restart_consumers(
    _: bool = Depends(verify_admin_api_key)
):
    """
    Restart Kafka consumers.

    Security: Requires admin API key. This is a sensitive operation.
    """
    results = {"claims_consumer": "not_initialized", "updates_consumer": "not_initialized"}

    if claims_consumer:
        try:
            await claims_consumer.stop()
            await claims_consumer.start()
            results["claims_consumer"] = "restarted"
        except Exception as e:
            results["claims_consumer"] = f"error: {str(e)}"

    if updates_consumer:
        try:
            await updates_consumer.stop()
            await updates_consumer.start()
            results["updates_consumer"] = "restarted"
        except Exception as e:
            results["updates_consumer"] = f"error: {str(e)}"

    return {"status": "completed", "results": results}


@app.post("/admin/truth/refresh/{entity_id}", tags=["Admin"])
async def refresh_entity_truth(
    entity_id: str,
    _: bool = Depends(verify_admin_api_key)
):
    """
    Force refresh of truth for an entity.

    Security: Requires admin API key.
    """
    # Validate entity_id format
    entity_id = InputSanitizer.sanitize_entity_id(entity_id)
    if not truth_layer:
        raise HTTPException(status_code=503, detail="Truth layer not available")

    entity_truth = await truth_layer.refresh_truth(entity_id)

    if not entity_truth:
        raise HTTPException(status_code=404, detail=f"Entity {entity_id} not found")

    return {
        "entity_id": entity_id,
        "refreshed": True,
        "truths_count": len(entity_truth.truths),
    }


@app.post("/admin/truth/materialize", tags=["Admin"])
async def materialize_truths(
    background_tasks: BackgroundTasks,
    limit: int = Query(default=1000, ge=1, le=10000),
    _: bool = Depends(verify_admin_api_key)
):
    """
    Materialize truth values for all entities.

    Runs in background for large datasets.

    Security: Requires admin API key. Resource-intensive operation.
    """
    if not truth_layer:
        raise HTTPException(status_code=503, detail="Truth layer not available")

    async def materialize_task():
        count = await truth_layer.materialize_all_truths(limit=limit)
        logger.info(f"Materialized {count} truth values")

    background_tasks.add_task(materialize_task)

    return {
        "status": "started",
        "message": f"Materializing truths for up to {limit} entities",
    }


# =============================================================================
# Detect Conflicts Endpoint
# =============================================================================

@app.post("/detect-conflicts", tags=["Conflicts"])
async def detect_conflicts_for_claim(
    subject_entity_id: str,
    predicate: str,
    object_value: Any,
    source_id: Optional[str] = None,
    confidence: float = 0.8,
):
    """
    Detect potential conflicts for a hypothetical claim.

    Does not add the claim - just checks what conflicts would exist.
    """
    if not conflict_detector:
        raise HTTPException(status_code=503, detail="Conflict detector not available")

    claim_data = {
        "subject_entity_id": subject_entity_id,
        "predicate": predicate,
        "object_value": object_value,
        "source_id": source_id,
        "confidence": confidence,
    }

    conflicts = await conflict_detector.detect_conflicts_for_claim(claim_data)

    return {
        "conflicts_detected": len(conflicts),
        "conflicts": [c.to_dict() for c in conflicts],
    }


@app.get("/scan-conflicts", tags=["Conflicts"])
async def scan_all_conflicts(
    entity_id: Optional[str] = None,
    predicate: Optional[str] = None,
    limit: int = Query(default=100, le=500),
):
    """
    Scan the graph for all conflicts.

    This is more comprehensive than /conflicts which only returns
    UNRESOLVED conflict relationships.
    """
    if not conflict_detector:
        raise HTTPException(status_code=503, detail="Conflict detector not available")

    conflicts = await conflict_detector.detect_all_conflicts(
        entity_id=entity_id,
        predicate=predicate,
        limit=limit,
    )

    return {
        "conflicts_found": len(conflicts),
        "conflicts": [c.to_dict() for c in conflicts],
    }
