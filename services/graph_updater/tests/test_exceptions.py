"""
Tests for Custom Exceptions
===========================

Unit tests for the custom exception classes covering:
- Base GraphUpdaterError
- Entity exceptions (EntityNotFoundError, EntityMergeError, DuplicateEntityError)
- Claim exceptions (ClaimNotFoundError, ClaimValidationError, ClaimSupersededError)
- Conflict exceptions
- Truth layer exceptions
- Database exceptions
- Kafka exceptions
- Validation exceptions
"""

import pytest

# Import from conftest's path setup and mocking
from exceptions import (
    GraphUpdaterError,
    EntityError,
    EntityNotFoundError,
    EntityMergeError,
    DuplicateEntityError,
    ClaimError,
    ClaimNotFoundError,
    ClaimValidationError,
    ClaimSupersededError,
    ConflictError,
    ConflictNotFoundError,
    ConflictAlreadyResolvedError,
    InvalidResolutionStrategyError,
    ResolutionFailedError,
    TruthLayerError,
    TruthNotFoundError,
    TruthComputationError,
    SourceError,
    SourceNotFoundError,
    DatabaseError,
    Neo4jConnectionError,
    QueryExecutionError,
    TransactionError,
    KafkaError,
    KafkaConnectionError,
    MessageProcessingError,
    RateLimitExceededError,
    ValidationError,
    InvalidPredicateError,
)


class TestGraphUpdaterError:
    """Tests for base GraphUpdaterError."""

    def test_basic_exception(self):
        """Test basic exception creation."""
        error = GraphUpdaterError("Test error message")

        assert str(error) == "Test error message"
        assert error.message == "Test error message"
        assert error.error_code == "GRAPH_UPDATER_ERROR"
        assert error.details == {}

    def test_exception_with_custom_code(self):
        """Test exception with custom error code."""
        error = GraphUpdaterError(
            message="Custom error",
            error_code="CUSTOM_ERROR_CODE",
        )

        assert error.error_code == "CUSTOM_ERROR_CODE"

    def test_exception_with_details(self):
        """Test exception with details."""
        details = {"field": "value", "count": 42}
        error = GraphUpdaterError(
            message="Error with details",
            details=details,
        )

        assert error.details == details
        assert error.details["field"] == "value"

    def test_to_dict(self):
        """Test serialization to dictionary."""
        error = GraphUpdaterError(
            message="Test message",
            error_code="TEST_CODE",
            details={"key": "value"},
        )

        data = error.to_dict()

        assert data["error"] == "TEST_CODE"
        assert data["message"] == "Test message"
        assert data["details"]["key"] == "value"

    def test_exception_inheritance(self):
        """Test that GraphUpdaterError inherits from Exception."""
        error = GraphUpdaterError("Test")

        assert isinstance(error, Exception)


class TestEntityExceptions:
    """Tests for entity-related exceptions."""

    def test_entity_not_found_error(self):
        """Test EntityNotFoundError."""
        error = EntityNotFoundError("ent_123")

        assert "ent_123" in str(error)
        assert error.entity_id == "ent_123"
        assert error.error_code == "ENTITY_NOT_FOUND"
        assert error.details["entity_id"] == "ent_123"

    def test_entity_not_found_with_custom_message(self):
        """Test EntityNotFoundError with custom message."""
        error = EntityNotFoundError("ent_456", message="Custom not found message")

        assert "Custom not found message" in str(error)
        assert error.entity_id == "ent_456"

    def test_entity_merge_error(self):
        """Test EntityMergeError."""
        error = EntityMergeError(
            target_id="ent_target",
            source_ids=["ent_src1", "ent_src2"],
            reason="Conflicting types",
        )

        assert error.target_id == "ent_target"
        assert error.source_ids == ["ent_src1", "ent_src2"]
        assert error.error_code == "ENTITY_MERGE_FAILED"
        assert "ent_target" in str(error)
        assert "Conflicting types" in str(error)

    def test_duplicate_entity_error(self):
        """Test DuplicateEntityError."""
        error = DuplicateEntityError(
            name="Acme Corporation",
            entity_type="ORGANIZATION",
            existing_id="ent_existing",
        )

        assert error.error_code == "DUPLICATE_ENTITY"
        assert "Acme Corporation" in str(error)
        assert error.details["name"] == "Acme Corporation"
        assert error.details["existing_entity_id"] == "ent_existing"

    def test_entity_error_inheritance(self):
        """Test EntityError inheritance hierarchy."""
        error = EntityNotFoundError("ent_123")

        assert isinstance(error, EntityError)
        assert isinstance(error, GraphUpdaterError)
        assert isinstance(error, Exception)


class TestClaimExceptions:
    """Tests for claim-related exceptions."""

    def test_claim_not_found_error(self):
        """Test ClaimNotFoundError."""
        error = ClaimNotFoundError("claim_123")

        assert error.claim_id == "claim_123"
        assert error.error_code == "CLAIM_NOT_FOUND"
        assert "claim_123" in str(error)

    def test_claim_validation_error(self):
        """Test ClaimValidationError."""
        errors = ["confidence must be between 0 and 1", "predicate is required"]
        error = ClaimValidationError(errors)

        assert error.errors == errors
        assert error.error_code == "CLAIM_VALIDATION_FAILED"
        assert "confidence" in str(error)
        assert error.details["validation_errors"] == errors

    def test_claim_superseded_error(self):
        """Test ClaimSupersededError."""
        error = ClaimSupersededError(
            claim_id="claim_old",
            superseded_by="claim_new",
        )

        assert error.error_code == "CLAIM_SUPERSEDED"
        assert "claim_old" in str(error)
        assert "claim_new" in str(error)
        assert error.details["superseded_by"] == "claim_new"

    def test_claim_error_inheritance(self):
        """Test ClaimError inheritance hierarchy."""
        error = ClaimNotFoundError("claim_123")

        assert isinstance(error, ClaimError)
        assert isinstance(error, GraphUpdaterError)


class TestConflictExceptions:
    """Tests for conflict-related exceptions."""

    def test_conflict_not_found_error(self):
        """Test ConflictNotFoundError."""
        error = ConflictNotFoundError("conflict_123")

        assert error.conflict_id == "conflict_123"
        assert error.error_code == "CONFLICT_NOT_FOUND"
        assert "conflict_123" in str(error)

    def test_conflict_already_resolved_error(self):
        """Test ConflictAlreadyResolvedError."""
        error = ConflictAlreadyResolvedError(
            conflict_id="conflict_456",
            resolved_at="2024-01-15T12:00:00",
        )

        assert error.error_code == "CONFLICT_ALREADY_RESOLVED"
        assert "conflict_456" in str(error)
        assert "2024-01-15T12:00:00" in str(error)

    def test_invalid_resolution_strategy_error(self):
        """Test InvalidResolutionStrategyError."""
        error = InvalidResolutionStrategyError(
            strategy="invalid_strategy",
            valid_strategies=["confidence_based", "timestamp_based", "source_reliability"],
        )

        assert error.error_code == "INVALID_RESOLUTION_STRATEGY"
        assert "invalid_strategy" in str(error)
        assert error.details["valid_strategies"] == ["confidence_based", "timestamp_based", "source_reliability"]

    def test_resolution_failed_error(self):
        """Test ResolutionFailedError."""
        error = ResolutionFailedError(
            conflict_id="conflict_789",
            reason="Both claims have equal scores",
        )

        assert error.error_code == "RESOLUTION_FAILED"
        assert "conflict_789" in str(error)
        assert "equal scores" in str(error)

    def test_conflict_error_inheritance(self):
        """Test ConflictError inheritance hierarchy."""
        error = ConflictNotFoundError("conflict_123")

        assert isinstance(error, ConflictError)
        assert isinstance(error, GraphUpdaterError)


class TestTruthLayerExceptions:
    """Tests for truth layer exceptions."""

    def test_truth_not_found_error_basic(self):
        """Test TruthNotFoundError without predicate."""
        error = TruthNotFoundError(entity_id="ent_123")

        assert error.error_code == "TRUTH_NOT_FOUND"
        assert "ent_123" in str(error)
        assert error.details["entity_id"] == "ent_123"
        assert error.details["predicate"] is None

    def test_truth_not_found_error_with_predicate(self):
        """Test TruthNotFoundError with predicate."""
        error = TruthNotFoundError(entity_id="ent_123", predicate="ceo")

        assert "ent_123" in str(error)
        assert "ceo" in str(error)
        assert error.details["predicate"] == "ceo"

    def test_truth_computation_error(self):
        """Test TruthComputationError."""
        error = TruthComputationError(
            entity_id="ent_123",
            reason="Circular reference detected",
        )

        assert error.error_code == "TRUTH_COMPUTATION_FAILED"
        assert "ent_123" in str(error)
        assert "Circular reference" in str(error)

    def test_truth_layer_error_inheritance(self):
        """Test TruthLayerError inheritance hierarchy."""
        error = TruthNotFoundError("ent_123")

        assert isinstance(error, TruthLayerError)
        assert isinstance(error, GraphUpdaterError)


class TestSourceExceptions:
    """Tests for source-related exceptions."""

    def test_source_not_found_error(self):
        """Test SourceNotFoundError."""
        error = SourceNotFoundError("src_123")

        assert error.source_id == "src_123"
        assert error.error_code == "SOURCE_NOT_FOUND"
        assert "src_123" in str(error)


class TestDatabaseExceptions:
    """Tests for database-related exceptions."""

    def test_neo4j_connection_error(self):
        """Test Neo4jConnectionError."""
        error = Neo4jConnectionError()

        assert error.error_code == "NEO4J_CONNECTION_ERROR"
        assert "Neo4j" in str(error)

    def test_neo4j_connection_error_custom_message(self):
        """Test Neo4jConnectionError with custom message."""
        error = Neo4jConnectionError("Connection timed out after 30s")

        assert "timed out" in str(error)

    def test_query_execution_error(self):
        """Test QueryExecutionError."""
        error = QueryExecutionError(
            query="MATCH (n:Entity) RETURN n",
            error="Syntax error in query",
        )

        assert error.error_code == "QUERY_EXECUTION_ERROR"
        assert "Syntax error" in str(error)
        assert "MATCH" in error.details["query_snippet"]

    def test_query_execution_error_long_query(self):
        """Test QueryExecutionError truncates long queries."""
        long_query = "MATCH (n:Entity) WHERE " + "n.prop = 'value' OR " * 50 + "n.id = '123'"
        error = QueryExecutionError(query=long_query, error="Query too complex")

        # Query snippet should be truncated
        assert len(error.details["query_snippet"]) <= 200

    def test_transaction_error(self):
        """Test TransactionError."""
        error = TransactionError(
            operation="entity_merge",
            reason="Deadlock detected",
        )

        assert error.error_code == "TRANSACTION_ERROR"
        assert "entity_merge" in str(error)
        assert "Deadlock" in str(error)

    def test_database_error_inheritance(self):
        """Test DatabaseError inheritance hierarchy."""
        error = Neo4jConnectionError()

        assert isinstance(error, DatabaseError)
        assert isinstance(error, GraphUpdaterError)


class TestKafkaExceptions:
    """Tests for Kafka-related exceptions."""

    def test_kafka_connection_error(self):
        """Test KafkaConnectionError."""
        error = KafkaConnectionError(
            brokers="localhost:9092,localhost:9093",
        )

        assert error.error_code == "KAFKA_CONNECTION_ERROR"
        assert error.details["brokers"] == "localhost:9092,localhost:9093"

    def test_kafka_connection_error_custom_message(self):
        """Test KafkaConnectionError with custom message."""
        error = KafkaConnectionError(
            brokers="broker1:9092",
            message="All brokers are unavailable",
        )

        assert "All brokers" in str(error)

    def test_message_processing_error(self):
        """Test MessageProcessingError."""
        error = MessageProcessingError(
            topic="extracted-claims",
            offset=12345,
            reason="Invalid JSON format",
        )

        assert error.error_code == "MESSAGE_PROCESSING_ERROR"
        assert "extracted-claims" in str(error)
        assert "12345" in str(error)
        assert error.details["topic"] == "extracted-claims"
        assert error.details["offset"] == 12345

    def test_kafka_error_inheritance(self):
        """Test KafkaError inheritance hierarchy."""
        error = KafkaConnectionError("localhost:9092")

        assert isinstance(error, KafkaError)
        assert isinstance(error, GraphUpdaterError)


class TestRateLimitException:
    """Tests for rate limiting exception."""

    def test_rate_limit_exceeded_error(self):
        """Test RateLimitExceededError."""
        error = RateLimitExceededError(
            limit=100,
            window_seconds=60,
            retry_after=45,
        )

        assert error.error_code == "RATE_LIMIT_EXCEEDED"
        assert error.retry_after == 45
        assert "100" in str(error)
        assert "60" in str(error)
        assert error.details["limit"] == 100
        assert error.details["retry_after_seconds"] == 45


class TestValidationExceptions:
    """Tests for validation exceptions."""

    def test_validation_error_basic(self):
        """Test basic ValidationError."""
        error = ValidationError(
            field="confidence",
            message="must be between 0 and 1",
        )

        assert error.error_code == "VALIDATION_ERROR"
        assert "confidence" in str(error)
        assert "0 and 1" in str(error)

    def test_validation_error_with_value(self):
        """Test ValidationError with value."""
        error = ValidationError(
            field="entity_type",
            message="invalid type",
            value="INVALID_TYPE",
        )

        assert error.details["field"] == "entity_type"
        assert error.details["value"] == "INVALID_TYPE"

    def test_invalid_predicate_error(self):
        """Test InvalidPredicateError."""
        error = InvalidPredicateError(
            predicate="invalid predicate",
            reason="contains spaces",
        )

        assert error.error_code == "INVALID_PREDICATE"
        assert "invalid predicate" in str(error)
        assert "spaces" in str(error)

    def test_invalid_predicate_error_inheritance(self):
        """Test InvalidPredicateError inheritance."""
        error = InvalidPredicateError("test", "reason")

        # Should inherit from ValidationError
        assert isinstance(error, ValidationError)


class TestExceptionUsability:
    """Tests for exception usability in try/except blocks."""

    def test_catch_base_error(self):
        """Test catching base GraphUpdaterError."""
        def raise_entity_error():
            raise EntityNotFoundError("ent_123")

        with pytest.raises(GraphUpdaterError) as exc_info:
            raise_entity_error()

        assert exc_info.value.error_code == "ENTITY_NOT_FOUND"

    def test_catch_specific_error(self):
        """Test catching specific error type."""
        def raise_claim_error():
            raise ClaimValidationError(["error1", "error2"])

        with pytest.raises(ClaimValidationError) as exc_info:
            raise_claim_error()

        assert len(exc_info.value.errors) == 2

    def test_exception_in_api_response(self):
        """Test using exception for API response."""
        error = EntityNotFoundError("ent_missing")
        response_body = error.to_dict()

        assert response_body["error"] == "ENTITY_NOT_FOUND"
        assert "ent_missing" in response_body["message"]
        assert "entity_id" in response_body["details"]
