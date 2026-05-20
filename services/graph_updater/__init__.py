"""
DocWeave Graph Updater Service
==============================

Phase 4: Knowledge Graph and Conflict Resolution

This service maintains the knowledge graph and handles:
    - Entity management with alias support and merging
    - Claim processing with versioning
    - Conflict detection (value mismatch, temporal, semantic)
    - Conflict resolution with multiple strategies
    - Governed truth layer with historical queries
    - Background Kafka consumer for claim processing

Components:
    - operations.py: Enhanced graph operations
    - conflict_detector.py: Conflict detection service
    - conflict_resolver.py: Resolution strategies and audit trail
    - truth_layer.py: Governed truth computation
    - consumer.py: Kafka consumer for claims
    - main.py: FastAPI application with all endpoints
    - health.py: Health checks and monitoring
    - middleware.py: Request middleware (logging, rate limiting, correlation)
    - exceptions.py: Custom exception classes
    - models.py: Additional Pydantic models
"""

from services.graph_updater.conflict_detector import (
    ConflictDetector,
    ConflictDetectorConfig,
    ConflictSeverity,
    ConflictType,
    DetectedConflict,
)
from services.graph_updater.conflict_resolver import (
    ConflictResolver,
    ConflictResolverConfig,
    ResolutionAuditEntry,
    ResolutionOutcome,
    ResolutionResult,
    ResolutionStrategy,
)
from services.graph_updater.consumer import (
    ClaimProcessor,
    ClaimsConsumer,
    GraphUpdateConsumer,
)
from services.graph_updater.exceptions import (
    ClaimError,
    ClaimNotFoundError,
    ClaimValidationError,
    ConflictAlreadyResolvedError,
    ConflictError,
    ConflictNotFoundError,
    DatabaseError,
    EntityError,
    EntityMergeError,
    EntityNotFoundError,
    GraphUpdaterError,
    Neo4jConnectionError,
    TruthLayerError,
    TruthNotFoundError,
)
from services.graph_updater.health import (
    ComponentHealth,
    HealthChecker,
    HealthStatus,
    MetricsCollector,
    ServiceHealth,
)
from services.graph_updater.operations import (
    ClaimVersionInfo,
    EntityMergeRequest,
    EntityMergeResult,
    GraphOperations,
)
from services.graph_updater.truth_layer import (
    EntityTruth,
    HistoricalTruth,
    TruthLayer,
    TruthLayerConfig,
    TruthValue,
)

__version__ = "0.4.0"
__all__ = [
    # Operations
    "GraphOperations",
    "EntityMergeRequest",
    "EntityMergeResult",
    "ClaimVersionInfo",
    # Conflict Detection
    "ConflictDetector",
    "ConflictDetectorConfig",
    "ConflictType",
    "ConflictSeverity",
    "DetectedConflict",
    # Conflict Resolution
    "ConflictResolver",
    "ConflictResolverConfig",
    "ResolutionStrategy",
    "ResolutionOutcome",
    "ResolutionResult",
    "ResolutionAuditEntry",
    # Truth Layer
    "TruthLayer",
    "TruthLayerConfig",
    "TruthValue",
    "EntityTruth",
    "HistoricalTruth",
    # Consumer
    "ClaimsConsumer",
    "GraphUpdateConsumer",
    "ClaimProcessor",
    # Health
    "HealthChecker",
    "HealthStatus",
    "ComponentHealth",
    "ServiceHealth",
    "MetricsCollector",
    # Exceptions
    "GraphUpdaterError",
    "EntityError",
    "EntityNotFoundError",
    "EntityMergeError",
    "ClaimError",
    "ClaimNotFoundError",
    "ClaimValidationError",
    "ConflictError",
    "ConflictNotFoundError",
    "ConflictAlreadyResolvedError",
    "TruthLayerError",
    "TruthNotFoundError",
    "DatabaseError",
    "Neo4jConnectionError",
]
