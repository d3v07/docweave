"""
Tests for Pydantic Models
=========================

Unit tests for the graph-updater service Pydantic models covering:
- Model instantiation
- Field validation
- Serialization/deserialization
- Default values
- Edge cases
"""

import pytest
from datetime import datetime
from pydantic import ValidationError

from models import (
    BatchClaimRequest,
    BatchClaimResponse,
    EntitySearchRequest,
    EntitySearchResponse,
    GraphStatsResponse,
    TruthComparisonRequest,
    TruthComparisonResponse,
    ConflictAnalysisRequest,
    ConflictAnalysisResponse,
    ResolutionPreviewRequest,
    ResolutionPreviewResponse,
    EntityRelationship,
    EntityGraph,
    ClaimLineage,
    SourceImpactAnalysis,
    BulkOperationStatus,
    WebhookConfig,
    MetricsSnapshot,
)


class TestBatchClaimRequest:
    """Tests for BatchClaimRequest model."""

    def test_valid_batch_claim_request(self):
        """Test creating valid BatchClaimRequest."""
        request = BatchClaimRequest(
            claims=[{"id": "claim_1", "predicate": "test"}],
            check_conflicts=True,
            auto_resolve=False,
        )

        assert len(request.claims) == 1
        assert request.check_conflicts is True
        assert request.auto_resolve is False

    def test_batch_claim_request_default_values(self):
        """Test default values for BatchClaimRequest."""
        request = BatchClaimRequest(
            claims=[{"id": "claim_1"}]
        )

        assert request.check_conflicts is True
        assert request.auto_resolve is False
        assert request.resolution_strategy is None
        assert request.source_id is None

    def test_batch_claim_request_empty_claims(self):
        """Test that empty claims list fails validation."""
        with pytest.raises(ValidationError) as exc_info:
            BatchClaimRequest(claims=[])

        assert "min_length" in str(exc_info.value).lower() or "at least 1" in str(exc_info.value).lower()

    def test_batch_claim_request_too_many_claims(self):
        """Test that too many claims fails validation."""
        claims = [{"id": f"claim_{i}"} for i in range(1001)]

        with pytest.raises(ValidationError) as exc_info:
            BatchClaimRequest(claims=claims)

        assert "max_length" in str(exc_info.value).lower() or "at most 1000" in str(exc_info.value).lower()

    def test_batch_claim_request_with_all_fields(self):
        """Test BatchClaimRequest with all fields specified."""
        request = BatchClaimRequest(
            claims=[{"id": "claim_1"}, {"id": "claim_2"}],
            check_conflicts=False,
            auto_resolve=True,
            resolution_strategy="newest_wins",
            source_id="src_123",
        )

        assert len(request.claims) == 2
        assert request.check_conflicts is False
        assert request.auto_resolve is True
        assert request.resolution_strategy == "newest_wins"
        assert request.source_id == "src_123"


class TestBatchClaimResponse:
    """Tests for BatchClaimResponse model."""

    def test_valid_batch_claim_response(self):
        """Test creating valid BatchClaimResponse."""
        response = BatchClaimResponse(
            total_claims=10,
            successful=8,
            failed=2,
            conflicts_detected=3,
            claim_ids=["claim_1", "claim_2", "claim_3"],
            errors=[{"claim_id": "claim_4", "error": "validation failed"}],
            processing_time_ms=150.5,
        )

        assert response.total_claims == 10
        assert response.successful == 8
        assert response.failed == 2
        assert response.conflicts_detected == 3
        assert len(response.claim_ids) == 3
        assert len(response.errors) == 1
        assert response.processing_time_ms == 150.5

    def test_batch_claim_response_empty_lists(self):
        """Test BatchClaimResponse with empty lists."""
        response = BatchClaimResponse(
            total_claims=0,
            successful=0,
            failed=0,
            conflicts_detected=0,
            claim_ids=[],
            errors=[],
            processing_time_ms=0.0,
        )

        assert response.claim_ids == []
        assert response.errors == []


class TestEntitySearchRequest:
    """Tests for EntitySearchRequest model."""

    def test_valid_entity_search_request(self):
        """Test creating valid EntitySearchRequest."""
        request = EntitySearchRequest(
            query="Acme Corporation",
        )

        assert request.query == "Acme Corporation"
        assert request.entity_type is None
        assert request.include_merged is False
        assert request.limit == 20
        assert request.offset == 0

    def test_entity_search_request_with_all_fields(self):
        """Test EntitySearchRequest with all fields."""
        request = EntitySearchRequest(
            query="test query",
            entity_type="ORGANIZATION",
            include_merged=True,
            limit=50,
            offset=10,
        )

        assert request.query == "test query"
        assert request.entity_type == "ORGANIZATION"
        assert request.include_merged is True
        assert request.limit == 50
        assert request.offset == 10

    def test_entity_search_request_empty_query(self):
        """Test that empty query fails validation."""
        with pytest.raises(ValidationError):
            EntitySearchRequest(query="")

    def test_entity_search_request_query_too_long(self):
        """Test that query exceeding max length fails validation."""
        long_query = "a" * 501

        with pytest.raises(ValidationError):
            EntitySearchRequest(query=long_query)

    def test_entity_search_request_limit_boundaries(self):
        """Test limit boundary validation."""
        # Minimum limit
        request = EntitySearchRequest(query="test", limit=1)
        assert request.limit == 1

        # Maximum limit
        request = EntitySearchRequest(query="test", limit=100)
        assert request.limit == 100

        # Below minimum
        with pytest.raises(ValidationError):
            EntitySearchRequest(query="test", limit=0)

        # Above maximum
        with pytest.raises(ValidationError):
            EntitySearchRequest(query="test", limit=101)

    def test_entity_search_request_negative_offset(self):
        """Test that negative offset fails validation."""
        with pytest.raises(ValidationError):
            EntitySearchRequest(query="test", offset=-1)


class TestEntitySearchResponse:
    """Tests for EntitySearchResponse model."""

    def test_valid_entity_search_response(self):
        """Test creating valid EntitySearchResponse."""
        response = EntitySearchResponse(
            entities=[{"id": "ent_1", "name": "Test"}],
            total=100,
            has_more=True,
        )

        assert len(response.entities) == 1
        assert response.total == 100
        assert response.has_more is True

    def test_entity_search_response_empty(self):
        """Test EntitySearchResponse with no results."""
        response = EntitySearchResponse(
            entities=[],
            total=0,
            has_more=False,
        )

        assert response.entities == []
        assert response.total == 0
        assert response.has_more is False


class TestGraphStatsResponse:
    """Tests for GraphStatsResponse model."""

    def test_valid_graph_stats_response(self):
        """Test creating valid GraphStatsResponse."""
        now = datetime.utcnow()
        response = GraphStatsResponse(
            entity_count=1000,
            claim_count=5000,
            source_count=100,
            conflict_count=50,
            truth_count=800,
            resolution_count=30,
            last_updated=now,
        )

        assert response.entity_count == 1000
        assert response.claim_count == 5000
        assert response.source_count == 100
        assert response.conflict_count == 50
        assert response.truth_count == 800
        assert response.resolution_count == 30
        assert response.last_updated == now


class TestTruthComparisonRequest:
    """Tests for TruthComparisonRequest model."""

    def test_valid_truth_comparison_request(self):
        """Test creating valid TruthComparisonRequest."""
        time1 = datetime(2024, 1, 1)
        time2 = datetime(2024, 6, 1)

        request = TruthComparisonRequest(
            entity_id="ent_123",
            time_point_1=time1,
            time_point_2=time2,
        )

        assert request.entity_id == "ent_123"
        assert request.predicate is None
        assert request.time_point_1 == time1
        assert request.time_point_2 == time2

    def test_truth_comparison_request_with_predicate(self):
        """Test TruthComparisonRequest with predicate."""
        request = TruthComparisonRequest(
            entity_id="ent_123",
            predicate="employee_count",
            time_point_1=datetime(2024, 1, 1),
            time_point_2=datetime(2024, 6, 1),
        )

        assert request.predicate == "employee_count"


class TestTruthComparisonResponse:
    """Tests for TruthComparisonResponse model."""

    def test_valid_truth_comparison_response(self):
        """Test creating valid TruthComparisonResponse."""
        response = TruthComparisonResponse(
            entity_id="ent_123",
            changes=[{"predicate": "ceo", "old": "John", "new": "Jane"}],
            added_predicates=["founded_date"],
            removed_predicates=["temp_location"],
            modified_predicates=["ceo", "employee_count"],
        )

        assert response.entity_id == "ent_123"
        assert len(response.changes) == 1
        assert "founded_date" in response.added_predicates
        assert "temp_location" in response.removed_predicates
        assert len(response.modified_predicates) == 2


class TestConflictAnalysisRequest:
    """Tests for ConflictAnalysisRequest model."""

    def test_valid_conflict_analysis_request(self):
        """Test creating valid ConflictAnalysisRequest."""
        request = ConflictAnalysisRequest(
            conflict_id="conflict_123",
        )

        assert request.conflict_id == "conflict_123"
        assert request.include_source_details is True
        assert request.include_claim_history is True

    def test_conflict_analysis_request_custom_flags(self):
        """Test ConflictAnalysisRequest with custom flags."""
        request = ConflictAnalysisRequest(
            conflict_id="conflict_456",
            include_source_details=False,
            include_claim_history=False,
        )

        assert request.include_source_details is False
        assert request.include_claim_history is False


class TestConflictAnalysisResponse:
    """Tests for ConflictAnalysisResponse model."""

    def test_valid_conflict_analysis_response(self):
        """Test creating valid ConflictAnalysisResponse."""
        response = ConflictAnalysisResponse(
            conflict_id="conflict_123",
            conflict_type="VALUE_MISMATCH",
            severity="HIGH",
            claims=[{"id": "claim_1"}, {"id": "claim_2"}],
            recommendation="Accept claim_1 based on source reliability",
            confidence_scores={"claim_1": 0.95, "claim_2": 0.8},
            source_reliability_scores={"src_1": 0.9, "src_2": 0.7},
        )

        assert response.conflict_id == "conflict_123"
        assert response.conflict_type == "VALUE_MISMATCH"
        assert response.severity == "HIGH"
        assert len(response.claims) == 2
        assert response.recommendation is not None

    def test_conflict_analysis_response_no_recommendation(self):
        """Test ConflictAnalysisResponse without recommendation."""
        response = ConflictAnalysisResponse(
            conflict_id="conflict_123",
            conflict_type="VALUE_MISMATCH",
            severity="LOW",
            claims=[],
            recommendation=None,
            confidence_scores={},
            source_reliability_scores={},
        )

        assert response.recommendation is None


class TestResolutionPreviewRequest:
    """Tests for ResolutionPreviewRequest model."""

    def test_valid_resolution_preview_request(self):
        """Test creating valid ResolutionPreviewRequest."""
        request = ResolutionPreviewRequest(
            conflict_id="conflict_123",
            strategy="manual",
            winning_claim_id="claim_1",
        )

        assert request.conflict_id == "conflict_123"
        assert request.strategy == "manual"
        assert request.winning_claim_id == "claim_1"

    def test_resolution_preview_request_auto_strategy(self):
        """Test ResolutionPreviewRequest with auto strategy."""
        request = ResolutionPreviewRequest(
            conflict_id="conflict_123",
            strategy="newest_wins",
        )

        assert request.strategy == "newest_wins"
        assert request.winning_claim_id is None


class TestResolutionPreviewResponse:
    """Tests for ResolutionPreviewResponse model."""

    def test_valid_resolution_preview_response(self):
        """Test creating valid ResolutionPreviewResponse."""
        response = ResolutionPreviewResponse(
            conflict_id="conflict_123",
            proposed_winner="claim_1",
            proposed_losers=["claim_2", "claim_3"],
            confidence=0.92,
            rationale="Selected based on source reliability and recency",
            side_effects=["Will supersede claim_2", "Will supersede claim_3"],
        )

        assert response.conflict_id == "conflict_123"
        assert response.proposed_winner == "claim_1"
        assert len(response.proposed_losers) == 2
        assert response.confidence == 0.92

    def test_resolution_preview_response_no_winner(self):
        """Test ResolutionPreviewResponse without clear winner."""
        response = ResolutionPreviewResponse(
            conflict_id="conflict_123",
            proposed_winner=None,
            proposed_losers=[],
            confidence=0.5,
            rationale="Unable to determine clear winner",
            side_effects=[],
        )

        assert response.proposed_winner is None


class TestEntityRelationship:
    """Tests for EntityRelationship model."""

    def test_valid_entity_relationship(self):
        """Test creating valid EntityRelationship."""
        relationship = EntityRelationship(
            source_entity_id="ent_1",
            target_entity_id="ent_2",
            relationship_type="WORKS_FOR",
            predicate="employer",
        )

        assert relationship.source_entity_id == "ent_1"
        assert relationship.target_entity_id == "ent_2"
        assert relationship.relationship_type == "WORKS_FOR"
        assert relationship.confidence == 0.8  # default

    def test_entity_relationship_custom_confidence(self):
        """Test EntityRelationship with custom confidence."""
        relationship = EntityRelationship(
            source_entity_id="ent_1",
            target_entity_id="ent_2",
            relationship_type="OWNS",
            predicate="ownership",
            confidence=0.95,
            claim_ids=["claim_1", "claim_2"],
        )

        assert relationship.confidence == 0.95
        assert len(relationship.claim_ids) == 2

    def test_entity_relationship_confidence_bounds(self):
        """Test confidence boundary validation."""
        # Minimum confidence
        relationship = EntityRelationship(
            source_entity_id="ent_1",
            target_entity_id="ent_2",
            relationship_type="KNOWS",
            predicate="acquaintance",
            confidence=0.0,
        )
        assert relationship.confidence == 0.0

        # Maximum confidence
        relationship = EntityRelationship(
            source_entity_id="ent_1",
            target_entity_id="ent_2",
            relationship_type="IS",
            predicate="identity",
            confidence=1.0,
        )
        assert relationship.confidence == 1.0

        # Below minimum
        with pytest.raises(ValidationError):
            EntityRelationship(
                source_entity_id="ent_1",
                target_entity_id="ent_2",
                relationship_type="KNOWS",
                predicate="test",
                confidence=-0.1,
            )

        # Above maximum
        with pytest.raises(ValidationError):
            EntityRelationship(
                source_entity_id="ent_1",
                target_entity_id="ent_2",
                relationship_type="KNOWS",
                predicate="test",
                confidence=1.1,
            )


class TestEntityGraph:
    """Tests for EntityGraph model."""

    def test_valid_entity_graph(self):
        """Test creating valid EntityGraph."""
        graph = EntityGraph(
            center_entity_id="ent_center",
            entities=[{"id": "ent_1"}, {"id": "ent_2"}],
            relationships=[
                EntityRelationship(
                    source_entity_id="ent_center",
                    target_entity_id="ent_1",
                    relationship_type="RELATED",
                    predicate="related_to",
                )
            ],
            depth=2,
        )

        assert graph.center_entity_id == "ent_center"
        assert len(graph.entities) == 2
        assert len(graph.relationships) == 1
        assert graph.depth == 2

    def test_entity_graph_empty_relationships(self):
        """Test EntityGraph with no relationships."""
        graph = EntityGraph(
            center_entity_id="ent_center",
            entities=[{"id": "ent_center"}],
            relationships=[],
            depth=0,
        )

        assert graph.relationships == []


class TestClaimLineage:
    """Tests for ClaimLineage model."""

    def test_valid_claim_lineage(self):
        """Test creating valid ClaimLineage."""
        lineage = ClaimLineage(
            claim_id="claim_123",
            version=3,
            predecessors=["claim_121", "claim_122"],
            successors=["claim_124"],
            status="superseded",
            created_at=datetime(2024, 1, 15),
            superseded_at=datetime(2024, 6, 1),
        )

        assert lineage.claim_id == "claim_123"
        assert lineage.version == 3
        assert len(lineage.predecessors) == 2
        assert len(lineage.successors) == 1
        assert lineage.status == "superseded"
        assert lineage.superseded_at is not None

    def test_claim_lineage_current_claim(self):
        """Test ClaimLineage for current claim."""
        lineage = ClaimLineage(
            claim_id="claim_latest",
            version=1,
            predecessors=[],
            successors=[],
            status="current",
            created_at=datetime.utcnow(),
        )

        assert lineage.status == "current"
        assert lineage.superseded_at is None
        assert lineage.predecessors == []
        assert lineage.successors == []


class TestSourceImpactAnalysis:
    """Tests for SourceImpactAnalysis model."""

    def test_valid_source_impact_analysis(self):
        """Test creating valid SourceImpactAnalysis."""
        analysis = SourceImpactAnalysis(
            source_id="src_123",
            claim_count=100,
            entity_count=50,
            conflict_count=10,
            superseded_count=20,
            current_count=70,
            reliability_trend=[
                {"date": "2024-01-01", "score": 0.8},
                {"date": "2024-06-01", "score": 0.85},
            ],
        )

        assert analysis.source_id == "src_123"
        assert analysis.claim_count == 100
        assert analysis.entity_count == 50
        assert analysis.current_count == 70
        assert len(analysis.reliability_trend) == 2


class TestBulkOperationStatus:
    """Tests for BulkOperationStatus model."""

    def test_valid_bulk_operation_status(self):
        """Test creating valid BulkOperationStatus."""
        status = BulkOperationStatus(
            operation_id="op_123",
            operation_type="batch_import",
            status="in_progress",
            total_items=1000,
            processed_items=500,
            failed_items=10,
            started_at=datetime(2024, 1, 15, 10, 0, 0),
            estimated_completion=datetime(2024, 1, 15, 10, 30, 0),
            errors=["Item 42 failed validation", "Item 99 duplicate"],
        )

        assert status.operation_id == "op_123"
        assert status.operation_type == "batch_import"
        assert status.status == "in_progress"
        assert status.processed_items == 500
        assert len(status.errors) == 2

    def test_bulk_operation_status_completed(self):
        """Test BulkOperationStatus for completed operation."""
        status = BulkOperationStatus(
            operation_id="op_456",
            operation_type="entity_merge",
            status="completed",
            total_items=100,
            processed_items=100,
            failed_items=0,
            started_at=datetime(2024, 1, 15, 10, 0, 0),
        )

        assert status.status == "completed"
        assert status.failed_items == 0
        assert status.estimated_completion is None
        assert status.errors == []


class TestWebhookConfig:
    """Tests for WebhookConfig model."""

    def test_valid_webhook_config(self):
        """Test creating valid WebhookConfig."""
        config = WebhookConfig(
            url="https://example.com/webhook",
        )

        assert config.url == "https://example.com/webhook"
        assert config.enabled is True
        assert "conflict_detected" in config.events
        assert config.retry_count == 3

    def test_webhook_config_with_all_fields(self):
        """Test WebhookConfig with all fields."""
        config = WebhookConfig(
            url="https://api.example.com/hooks/graph",
            events=["conflict_detected", "resolution_applied", "entity_merged"],
            secret="super_secret_key_16",
            enabled=True,
            retry_count=5,
        )

        assert len(config.events) == 3
        assert config.secret == "super_secret_key_16"
        assert config.retry_count == 5

    def test_webhook_config_invalid_url_no_protocol(self):
        """Test that URL without protocol fails validation."""
        with pytest.raises(ValidationError) as exc_info:
            WebhookConfig(url="example.com/webhook")

        assert "url" in str(exc_info.value).lower()

    def test_webhook_config_url_too_short(self):
        """Test that URL too short fails validation."""
        with pytest.raises(ValidationError):
            WebhookConfig(url="http://a")

    def test_webhook_config_url_too_long(self):
        """Test that URL too long fails validation."""
        long_url = "https://example.com/" + "a" * 500

        with pytest.raises(ValidationError):
            WebhookConfig(url=long_url)

    def test_webhook_config_secret_too_short(self):
        """Test that secret too short fails validation."""
        with pytest.raises(ValidationError):
            WebhookConfig(
                url="https://example.com/webhook",
                secret="short",
            )

    def test_webhook_config_secret_too_long(self):
        """Test that secret too long fails validation."""
        long_secret = "a" * 129

        with pytest.raises(ValidationError):
            WebhookConfig(
                url="https://example.com/webhook",
                secret=long_secret,
            )

    def test_webhook_config_retry_count_bounds(self):
        """Test retry_count boundary validation."""
        # Minimum
        config = WebhookConfig(
            url="https://example.com/webhook",
            retry_count=0,
        )
        assert config.retry_count == 0

        # Maximum
        config = WebhookConfig(
            url="https://example.com/webhook",
            retry_count=10,
        )
        assert config.retry_count == 10

        # Below minimum
        with pytest.raises(ValidationError):
            WebhookConfig(
                url="https://example.com/webhook",
                retry_count=-1,
            )

        # Above maximum
        with pytest.raises(ValidationError):
            WebhookConfig(
                url="https://example.com/webhook",
                retry_count=11,
            )

    def test_webhook_config_http_url_allowed(self):
        """Test that http URL is allowed."""
        config = WebhookConfig(url="http://localhost:8080/webhook")
        assert config.url == "http://localhost:8080/webhook"


class TestMetricsSnapshot:
    """Tests for MetricsSnapshot model."""

    def test_valid_metrics_snapshot(self):
        """Test creating valid MetricsSnapshot."""
        snapshot = MetricsSnapshot(
            timestamp=datetime.utcnow(),
            claims_processed_total=10000,
            claims_processed_per_minute=150.5,
            conflicts_detected_total=500,
            conflicts_resolved_total=450,
            average_resolution_time_ms=250.0,
            cache_hit_rate=0.85,
            neo4j_query_latency_ms=15.5,
            kafka_consumer_lag=100,
        )

        assert snapshot.claims_processed_total == 10000
        assert snapshot.claims_processed_per_minute == 150.5
        assert snapshot.conflicts_detected_total == 500
        assert snapshot.conflicts_resolved_total == 450
        assert snapshot.average_resolution_time_ms == 250.0
        assert snapshot.cache_hit_rate == 0.85
        assert snapshot.neo4j_query_latency_ms == 15.5
        assert snapshot.kafka_consumer_lag == 100

    def test_metrics_snapshot_zero_values(self):
        """Test MetricsSnapshot with zero values."""
        snapshot = MetricsSnapshot(
            timestamp=datetime.utcnow(),
            claims_processed_total=0,
            claims_processed_per_minute=0.0,
            conflicts_detected_total=0,
            conflicts_resolved_total=0,
            average_resolution_time_ms=0.0,
            cache_hit_rate=0.0,
            neo4j_query_latency_ms=0.0,
            kafka_consumer_lag=0,
        )

        assert snapshot.claims_processed_total == 0
        assert snapshot.cache_hit_rate == 0.0


class TestModelSerialization:
    """Tests for model serialization."""

    def test_batch_claim_request_dict(self):
        """Test BatchClaimRequest serialization."""
        request = BatchClaimRequest(
            claims=[{"id": "claim_1"}],
            check_conflicts=True,
        )

        data = request.model_dump()

        assert "claims" in data
        assert "check_conflicts" in data
        assert data["check_conflicts"] is True

    def test_entity_relationship_json(self):
        """Test EntityRelationship JSON serialization."""
        relationship = EntityRelationship(
            source_entity_id="ent_1",
            target_entity_id="ent_2",
            relationship_type="WORKS_FOR",
            predicate="employer",
            confidence=0.9,
        )

        json_str = relationship.model_dump_json()

        assert "ent_1" in json_str
        assert "WORKS_FOR" in json_str
        assert "0.9" in json_str

    def test_graph_stats_response_excludes_none(self):
        """Test that model serialization handles all fields."""
        now = datetime.utcnow()
        response = GraphStatsResponse(
            entity_count=100,
            claim_count=500,
            source_count=50,
            conflict_count=10,
            truth_count=90,
            resolution_count=5,
            last_updated=now,
        )

        data = response.model_dump()

        assert data["entity_count"] == 100
        assert data["last_updated"] == now


class TestModelValidation:
    """Tests for model validation edge cases."""

    def test_entity_search_request_whitespace_query(self):
        """Test that whitespace-only query is handled."""
        # Single space passes min_length=1 validation since it is 1 character
        # This is expected behavior - whitespace stripping would need a custom validator
        request = EntitySearchRequest(query=" ")
        assert request.query == " "
        assert len(request.query) == 1

    def test_webhook_config_validates_url_format(self):
        """Test URL validation for various formats."""
        # Valid HTTPS
        config = WebhookConfig(url="https://api.example.com/v1/webhook")
        assert config.url.startswith("https://")

        # Valid HTTP
        config = WebhookConfig(url="http://internal-server.local/hook")
        assert config.url.startswith("http://")

    def test_entity_relationship_default_claim_ids(self):
        """Test EntityRelationship default claim_ids is empty list."""
        relationship = EntityRelationship(
            source_entity_id="ent_1",
            target_entity_id="ent_2",
            relationship_type="KNOWS",
            predicate="acquaintance",
        )

        assert relationship.claim_ids == []

    def test_bulk_operation_status_default_errors(self):
        """Test BulkOperationStatus default errors is empty list."""
        status = BulkOperationStatus(
            operation_id="op_1",
            operation_type="import",
            status="pending",
            total_items=100,
            processed_items=0,
            failed_items=0,
            started_at=datetime.utcnow(),
        )

        assert status.errors == []


class TestModelEquality:
    """Tests for model equality and hashing."""

    def test_entity_relationship_equality(self):
        """Test EntityRelationship equality."""
        rel1 = EntityRelationship(
            source_entity_id="ent_1",
            target_entity_id="ent_2",
            relationship_type="KNOWS",
            predicate="acquaintance",
            confidence=0.8,
        )

        rel2 = EntityRelationship(
            source_entity_id="ent_1",
            target_entity_id="ent_2",
            relationship_type="KNOWS",
            predicate="acquaintance",
            confidence=0.8,
        )

        assert rel1 == rel2

    def test_webhook_config_equality(self):
        """Test WebhookConfig equality."""
        config1 = WebhookConfig(
            url="https://example.com/hook",
            events=["conflict_detected"],
        )

        config2 = WebhookConfig(
            url="https://example.com/hook",
            events=["conflict_detected"],
        )

        assert config1 == config2
