"""
Custom Exceptions for Graph Updater Service
=============================================

This module defines custom exceptions for the graph-updater service.
These exceptions provide more granular error handling and enable
consistent error responses across the API.
"""

from typing import Any, Optional


class GraphUpdaterError(Exception):
    """Base exception for graph-updater service."""

    def __init__(
        self,
        message: str,
        error_code: str = "GRAPH_UPDATER_ERROR",
        details: Optional[dict[str, Any]] = None,
    ):
        """
        Initialize the exception.

        Args:
            message: Human-readable error message
            error_code: Machine-readable error code
            details: Additional error details
        """
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        """Convert exception to dictionary for API responses."""
        return {
            "error": self.error_code,
            "message": self.message,
            "details": self.details,
        }


# Entity Exceptions
class EntityError(GraphUpdaterError):
    """Base exception for entity-related errors."""
    pass


class EntityNotFoundError(EntityError):
    """Raised when an entity is not found."""

    def __init__(self, entity_id: str, message: Optional[str] = None):
        super().__init__(
            message=message or f"Entity not found: {entity_id}",
            error_code="ENTITY_NOT_FOUND",
            details={"entity_id": entity_id},
        )
        self.entity_id = entity_id


class EntityMergeError(EntityError):
    """Raised when entity merge fails."""

    def __init__(
        self,
        target_id: str,
        source_ids: list[str],
        reason: str,
    ):
        super().__init__(
            message=f"Failed to merge entities into {target_id}: {reason}",
            error_code="ENTITY_MERGE_FAILED",
            details={
                "target_entity_id": target_id,
                "source_entity_ids": source_ids,
                "reason": reason,
            },
        )
        self.target_id = target_id
        self.source_ids = source_ids


class DuplicateEntityError(EntityError):
    """Raised when attempting to create a duplicate entity."""

    def __init__(self, name: str, entity_type: str, existing_id: str):
        super().__init__(
            message=f"Entity already exists: {name} ({entity_type})",
            error_code="DUPLICATE_ENTITY",
            details={
                "name": name,
                "entity_type": entity_type,
                "existing_entity_id": existing_id,
            },
        )


# Claim Exceptions
class ClaimError(GraphUpdaterError):
    """Base exception for claim-related errors."""
    pass


class ClaimNotFoundError(ClaimError):
    """Raised when a claim is not found."""

    def __init__(self, claim_id: str, message: Optional[str] = None):
        super().__init__(
            message=message or f"Claim not found: {claim_id}",
            error_code="CLAIM_NOT_FOUND",
            details={"claim_id": claim_id},
        )
        self.claim_id = claim_id


class ClaimValidationError(ClaimError):
    """Raised when claim validation fails."""

    def __init__(self, errors: list[str]):
        super().__init__(
            message=f"Claim validation failed: {', '.join(errors)}",
            error_code="CLAIM_VALIDATION_FAILED",
            details={"validation_errors": errors},
        )
        self.errors = errors


class ClaimSupersededError(ClaimError):
    """Raised when attempting to modify a superseded claim."""

    def __init__(self, claim_id: str, superseded_by: str):
        super().__init__(
            message=f"Claim {claim_id} has been superseded by {superseded_by}",
            error_code="CLAIM_SUPERSEDED",
            details={
                "claim_id": claim_id,
                "superseded_by": superseded_by,
            },
        )


# Conflict Exceptions
class ConflictError(GraphUpdaterError):
    """Base exception for conflict-related errors."""
    pass


class ConflictNotFoundError(ConflictError):
    """Raised when a conflict is not found."""

    def __init__(self, conflict_id: str):
        super().__init__(
            message=f"Conflict not found: {conflict_id}",
            error_code="CONFLICT_NOT_FOUND",
            details={"conflict_id": conflict_id},
        )
        self.conflict_id = conflict_id


class ConflictAlreadyResolvedError(ConflictError):
    """Raised when attempting to resolve an already resolved conflict."""

    def __init__(self, conflict_id: str, resolved_at: str):
        super().__init__(
            message=f"Conflict {conflict_id} was already resolved at {resolved_at}",
            error_code="CONFLICT_ALREADY_RESOLVED",
            details={
                "conflict_id": conflict_id,
                "resolved_at": resolved_at,
            },
        )


class InvalidResolutionStrategyError(ConflictError):
    """Raised when an invalid resolution strategy is specified."""

    def __init__(self, strategy: str, valid_strategies: list[str]):
        super().__init__(
            message=f"Invalid resolution strategy: {strategy}",
            error_code="INVALID_RESOLUTION_STRATEGY",
            details={
                "strategy": strategy,
                "valid_strategies": valid_strategies,
            },
        )


class ResolutionFailedError(ConflictError):
    """Raised when conflict resolution fails."""

    def __init__(self, conflict_id: str, reason: str):
        super().__init__(
            message=f"Failed to resolve conflict {conflict_id}: {reason}",
            error_code="RESOLUTION_FAILED",
            details={
                "conflict_id": conflict_id,
                "reason": reason,
            },
        )


# Truth Layer Exceptions
class TruthLayerError(GraphUpdaterError):
    """Base exception for truth layer errors."""
    pass


class TruthNotFoundError(TruthLayerError):
    """Raised when truth value is not found."""

    def __init__(self, entity_id: str, predicate: Optional[str] = None):
        message = f"Truth not found for entity {entity_id}"
        if predicate:
            message += f" and predicate {predicate}"

        super().__init__(
            message=message,
            error_code="TRUTH_NOT_FOUND",
            details={
                "entity_id": entity_id,
                "predicate": predicate,
            },
        )


class TruthComputationError(TruthLayerError):
    """Raised when truth computation fails."""

    def __init__(self, entity_id: str, reason: str):
        super().__init__(
            message=f"Failed to compute truth for {entity_id}: {reason}",
            error_code="TRUTH_COMPUTATION_FAILED",
            details={
                "entity_id": entity_id,
                "reason": reason,
            },
        )


# Source Exceptions
class SourceError(GraphUpdaterError):
    """Base exception for source-related errors."""
    pass


class SourceNotFoundError(SourceError):
    """Raised when a source is not found."""

    def __init__(self, source_id: str):
        super().__init__(
            message=f"Source not found: {source_id}",
            error_code="SOURCE_NOT_FOUND",
            details={"source_id": source_id},
        )
        self.source_id = source_id


# Database Exceptions
class DatabaseError(GraphUpdaterError):
    """Base exception for database-related errors."""
    pass


class Neo4jConnectionError(DatabaseError):
    """Raised when Neo4j connection fails."""

    def __init__(self, message: str = "Failed to connect to Neo4j"):
        super().__init__(
            message=message,
            error_code="NEO4J_CONNECTION_ERROR",
        )


class QueryExecutionError(DatabaseError):
    """Raised when a database query fails."""

    def __init__(self, query: str, error: str):
        super().__init__(
            message=f"Query execution failed: {error}",
            error_code="QUERY_EXECUTION_ERROR",
            details={
                "query_snippet": query[:200] if len(query) > 200 else query,
                "error": error,
            },
        )


class TransactionError(DatabaseError):
    """Raised when a database transaction fails."""

    def __init__(self, operation: str, reason: str):
        super().__init__(
            message=f"Transaction failed during {operation}: {reason}",
            error_code="TRANSACTION_ERROR",
            details={
                "operation": operation,
                "reason": reason,
            },
        )


# Kafka Exceptions
class KafkaError(GraphUpdaterError):
    """Base exception for Kafka-related errors."""
    pass


class KafkaConnectionError(KafkaError):
    """Raised when Kafka connection fails."""

    def __init__(self, brokers: str, message: str = "Failed to connect to Kafka"):
        super().__init__(
            message=message,
            error_code="KAFKA_CONNECTION_ERROR",
            details={"brokers": brokers},
        )


class MessageProcessingError(KafkaError):
    """Raised when message processing fails."""

    def __init__(self, topic: str, offset: int, reason: str):
        super().__init__(
            message=f"Failed to process message from {topic} at offset {offset}: {reason}",
            error_code="MESSAGE_PROCESSING_ERROR",
            details={
                "topic": topic,
                "offset": offset,
                "reason": reason,
            },
        )


# Rate Limiting
class RateLimitExceededError(GraphUpdaterError):
    """Raised when rate limit is exceeded."""

    def __init__(self, limit: int, window_seconds: int, retry_after: int):
        super().__init__(
            message=f"Rate limit exceeded: {limit} requests per {window_seconds} seconds",
            error_code="RATE_LIMIT_EXCEEDED",
            details={
                "limit": limit,
                "window_seconds": window_seconds,
                "retry_after_seconds": retry_after,
            },
        )
        self.retry_after = retry_after


# Validation Exceptions
class ValidationError(GraphUpdaterError):
    """Raised when input validation fails."""

    def __init__(self, field: str, message: str, value: Any = None):
        super().__init__(
            message=f"Validation error for {field}: {message}",
            error_code="VALIDATION_ERROR",
            details={
                "field": field,
                "message": message,
                "value": str(value) if value is not None else None,
            },
        )


class InvalidPredicateError(ValidationError):
    """Raised when predicate is invalid."""

    def __init__(self, predicate: str, reason: str):
        super().__init__(
            field="predicate",
            message=f"Invalid predicate '{predicate}': {reason}",
            value=predicate,
        )
        self.error_code = "INVALID_PREDICATE"
