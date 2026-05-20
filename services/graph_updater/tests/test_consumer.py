"""
Tests for Kafka Consumer
========================

Unit tests for the Kafka consumer classes covering:
- ClaimProcessor processing logic
- ClaimsConsumer start/stop and consumption
- GraphUpdateConsumer event handling
- Message validation
- Error handling
"""

import pytest
import json
import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

# Consumer module uses shared.utils.kafka_client which is mocked
from consumer import ClaimProcessor, ClaimsConsumer, GraphUpdateConsumer


class MockKafkaMessage:
    """Mock Kafka message."""

    def __init__(self, value, topic="test-topic", key=None, offset=0, partition=0):
        self._value = json.dumps(value) if isinstance(value, dict) else value
        self.value = value
        self._topic = topic
        self._key = key
        self._offset = offset
        self._partition = partition

    def topic(self):
        return self._topic

    def key(self):
        return self._key

    def offset(self):
        return self._offset

    def partition(self):
        return self._partition


class MockQueryResult:
    """Mock Neo4j query result."""

    def __init__(self, records=None):
        self.records = records or []


class MockConflict:
    """Mock conflict object."""

    def __init__(
        self,
        conflict_id="conflict_123",
        conflict_type=MagicMock(value="VALUE_MISMATCH"),
        severity=MagicMock(value="MEDIUM"),
        score=0.8,
        claim_ids=None,
        subject_entity_id="ent_123",
        predicate="employee_count",
        values=None,
    ):
        self.id = conflict_id
        self.conflict_type = conflict_type
        self.severity = severity
        self.score = score
        self.claim_ids = claim_ids or ["claim_1", "claim_2"]
        self.subject_entity_id = subject_entity_id
        self.predicate = predicate
        self.values = values or ["1000", "1500"]


@pytest.fixture
def mock_neo4j_client():
    """Create a mock Neo4j client."""
    client = AsyncMock()
    client.execute_query = AsyncMock(return_value=MockQueryResult([]))
    return client


@pytest.fixture
def mock_kafka_consumer():
    """Create a mock Kafka consumer."""
    consumer = AsyncMock()
    consumer.start = AsyncMock()
    consumer.stop = AsyncMock()
    consumer.commit = AsyncMock()
    return consumer


@pytest.fixture
def mock_kafka_producer():
    """Create a mock Kafka producer."""
    producer = AsyncMock()
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    producer.send = AsyncMock()
    return producer


@pytest.fixture
def mock_graph_operations():
    """Create a mock GraphOperations."""
    ops = MagicMock()
    ops.client = AsyncMock()
    ops.client.execute_query = AsyncMock(return_value=MockQueryResult([{"id": "claim_123"}]))
    ops.add_claim = AsyncMock(return_value="claim_123")
    ops.add_claim_with_conflict_check = AsyncMock(return_value=("claim_123", []))
    ops.merge_entity = AsyncMock(return_value="ent_123")
    ops.find_conflicting_claims = AsyncMock(return_value=[])
    return ops


@pytest.fixture
def mock_conflict_detector():
    """Create a mock ConflictDetector."""
    detector = AsyncMock()
    detector.detect_conflicts_for_claim = AsyncMock(return_value=[])
    return detector


@pytest.fixture
def mock_truth_layer():
    """Create mock truth layer."""
    truth_layer = AsyncMock()
    truth_layer.invalidate_cache = AsyncMock()
    truth_layer.propagate_resolution = AsyncMock()
    return truth_layer


@pytest.fixture
def claim_processor(mock_graph_operations, mock_conflict_detector, mock_truth_layer):
    """Create ClaimProcessor for testing."""
    processor = ClaimProcessor(
        graph_operations=mock_graph_operations,
        conflict_detector=mock_conflict_detector,
        truth_layer=mock_truth_layer,
    )
    return processor


@pytest.fixture
def claim_processor_with_producer(mock_graph_operations, mock_conflict_detector, mock_truth_layer, mock_kafka_producer):
    """Create ClaimProcessor with Kafka producer."""
    processor = ClaimProcessor(
        graph_operations=mock_graph_operations,
        conflict_detector=mock_conflict_detector,
        truth_layer=mock_truth_layer,
        producer=mock_kafka_producer,
    )
    return processor


@pytest.fixture
def mock_settings():
    """Create mock settings."""
    settings = MagicMock()
    settings.KAFKA_TOPIC_EXTRACTED_CLAIMS = "extracted-claims"
    settings.KAFKA_TOPIC_GRAPH_UPDATES = "graph-updates"
    settings.kafka = MagicMock()
    settings.kafka.bootstrap_servers = "localhost:9092"
    return settings


class TestClaimProcessor:
    """Tests for ClaimProcessor."""

    def test_claim_processor_creation(self, mock_graph_operations, mock_conflict_detector, mock_truth_layer):
        """Test ClaimProcessor can be created."""
        processor = ClaimProcessor(
            graph_operations=mock_graph_operations,
            conflict_detector=mock_conflict_detector,
            truth_layer=mock_truth_layer,
        )
        assert processor is not None
        assert processor.graph_ops == mock_graph_operations
        assert processor.detector == mock_conflict_detector
        assert processor.truth_layer == mock_truth_layer

    def test_claim_processor_with_producer(self, mock_graph_operations, mock_conflict_detector, mock_truth_layer, mock_kafka_producer):
        """Test ClaimProcessor can be created with producer."""
        processor = ClaimProcessor(
            graph_operations=mock_graph_operations,
            conflict_detector=mock_conflict_detector,
            truth_layer=mock_truth_layer,
            producer=mock_kafka_producer,
        )
        assert processor.producer == mock_kafka_producer


class TestClaimValidation:
    """Tests for claim validation."""

    def test_validate_claim_valid(self, claim_processor):
        """Test validation of valid claim data."""
        claim_data = {
            "subject_entity_id": "ent_123",
            "predicate": "employee_count",
            "object_value": "1500",
            "source_id": "src_report",
            "confidence": 0.95,
        }

        result = claim_processor._validate_claim(claim_data)

        assert result["valid"] is True
        assert len(result["errors"]) == 0

    def test_validate_claim_missing_fields(self, claim_processor):
        """Test validation with missing required fields."""
        claim_data = {
            "predicate": "employee_count",
            # Missing subject_entity_id, object_value, source_id
        }

        result = claim_processor._validate_claim(claim_data)

        assert result["valid"] is False
        assert len(result["errors"]) > 0

    def test_validate_claim_missing_subject_entity_id(self, claim_processor):
        """Test validation with missing subject_entity_id."""
        claim_data = {
            "predicate": "employee_count",
            "object_value": "1500",
            "source_id": "src_report",
        }

        result = claim_processor._validate_claim(claim_data)

        assert result["valid"] is False
        assert any("subject_entity_id" in e for e in result["errors"])

    def test_validate_claim_missing_predicate(self, claim_processor):
        """Test validation with missing predicate."""
        claim_data = {
            "subject_entity_id": "ent_123",
            "object_value": "1500",
            "source_id": "src_report",
        }

        result = claim_processor._validate_claim(claim_data)

        assert result["valid"] is False
        assert any("predicate" in e for e in result["errors"])

    def test_validate_claim_missing_object_value(self, claim_processor):
        """Test validation with missing object_value."""
        claim_data = {
            "subject_entity_id": "ent_123",
            "predicate": "employee_count",
            "source_id": "src_report",
        }

        result = claim_processor._validate_claim(claim_data)

        assert result["valid"] is False
        assert any("object_value" in e for e in result["errors"])

    def test_validate_claim_missing_source_id(self, claim_processor):
        """Test validation with missing source_id."""
        claim_data = {
            "subject_entity_id": "ent_123",
            "predicate": "employee_count",
            "object_value": "1500",
        }

        result = claim_processor._validate_claim(claim_data)

        assert result["valid"] is False
        assert any("source_id" in e for e in result["errors"])

    def test_validate_claim_invalid_confidence_too_high(self, claim_processor):
        """Test validation with confidence too high."""
        claim_data = {
            "subject_entity_id": "ent_123",
            "predicate": "employee_count",
            "object_value": "1500",
            "source_id": "src_report",
            "confidence": 1.5,  # Invalid
        }

        result = claim_processor._validate_claim(claim_data)

        assert result["valid"] is False
        assert any("confidence" in e.lower() for e in result["errors"])

    def test_validate_claim_invalid_confidence_negative(self, claim_processor):
        """Test validation with negative confidence."""
        claim_data = {
            "subject_entity_id": "ent_123",
            "predicate": "employee_count",
            "object_value": "1500",
            "source_id": "src_report",
            "confidence": -0.5,
        }

        result = claim_processor._validate_claim(claim_data)

        assert result["valid"] is False
        assert any("confidence" in e.lower() for e in result["errors"])

    def test_validate_claim_invalid_confidence_string(self, claim_processor):
        """Test validation with string confidence."""
        claim_data = {
            "subject_entity_id": "ent_123",
            "predicate": "employee_count",
            "object_value": "1500",
            "source_id": "src_report",
            "confidence": "high",
        }

        result = claim_processor._validate_claim(claim_data)

        assert result["valid"] is False
        assert any("confidence" in e.lower() for e in result["errors"])

    def test_validate_claim_default_confidence(self, claim_processor):
        """Test validation with no confidence (uses default)."""
        claim_data = {
            "subject_entity_id": "ent_123",
            "predicate": "employee_count",
            "object_value": "1500",
            "source_id": "src_report",
        }

        result = claim_processor._validate_claim(claim_data)

        assert result["valid"] is True


class TestClaimNormalization:
    """Tests for claim normalization."""

    def test_normalize_claim_adds_id(self, claim_processor):
        """Test that normalize adds ID if missing."""
        claim_data = {
            "subject_entity_id": "ent_123",
            "predicate": "Employee Count",
            "object_value": "1500",
            "source_id": "src_report",
        }

        result = claim_processor._normalize_claim(claim_data)

        assert "id" in result
        assert result["id"].startswith("claim_")

    def test_normalize_claim_preserves_existing_id(self, claim_processor):
        """Test that normalize preserves existing ID."""
        claim_data = {
            "id": "existing_id_123",
            "subject_entity_id": "ent_123",
            "predicate": "Employee Count",
            "object_value": "1500",
            "source_id": "src_report",
        }

        result = claim_processor._normalize_claim(claim_data)

        assert result["id"] == "existing_id_123"

    def test_normalize_claim_lowercases_predicate(self, claim_processor):
        """Test that predicate is lowercased."""
        claim_data = {
            "subject_entity_id": "ent_123",
            "predicate": "Employee Count",
            "object_value": "1500",
            "source_id": "src_report",
        }

        result = claim_processor._normalize_claim(claim_data)

        assert result["predicate"] == "employee_count"

    def test_normalize_claim_replaces_spaces_in_predicate(self, claim_processor):
        """Test that spaces in predicate become underscores."""
        claim_data = {
            "subject_entity_id": "ent_123",
            "predicate": "Total Employee Count",
            "object_value": "1500",
            "source_id": "src_report",
        }

        result = claim_processor._normalize_claim(claim_data)

        assert result["predicate"] == "total_employee_count"

    def test_normalize_claim_replaces_hyphens_in_predicate(self, claim_processor):
        """Test that hyphens in predicate become underscores."""
        claim_data = {
            "subject_entity_id": "ent_123",
            "predicate": "employee-count",
            "object_value": "1500",
            "source_id": "src_report",
        }

        result = claim_processor._normalize_claim(claim_data)

        assert result["predicate"] == "employee_count"

    def test_normalize_claim_ensures_float_confidence(self, claim_processor):
        """Test that confidence is converted to float."""
        claim_data = {
            "subject_entity_id": "ent_123",
            "predicate": "employee_count",
            "object_value": "1500",
            "source_id": "src_report",
            "confidence": 1,  # int
        }

        result = claim_processor._normalize_claim(claim_data)

        assert isinstance(result["confidence"], float)
        assert result["confidence"] == 1.0

    def test_normalize_claim_adds_timestamp(self, claim_processor):
        """Test that timestamp is added if missing."""
        claim_data = {
            "subject_entity_id": "ent_123",
            "predicate": "employee_count",
            "object_value": "1500",
            "source_id": "src_report",
        }

        result = claim_processor._normalize_claim(claim_data)

        assert "timestamp" in result

    def test_normalize_claim_adds_valid_from(self, claim_processor):
        """Test that valid_from is added if missing."""
        claim_data = {
            "subject_entity_id": "ent_123",
            "predicate": "employee_count",
            "object_value": "1500",
            "source_id": "src_report",
        }

        result = claim_processor._normalize_claim(claim_data)

        assert "valid_from" in result


class TestClaimProcessing:
    """Tests for claim processing."""

    @pytest.mark.asyncio
    async def test_process_claim_success(self, claim_processor, mock_graph_operations, mock_conflict_detector, mock_truth_layer):
        """Test successful claim processing."""
        claim_data = {
            "id": "claim_test",
            "subject_entity_id": "ent_123",
            "predicate": "employee_count",
            "object_value": "1500",
            "source_id": "src_report",
            "confidence": 0.9,
        }

        result = await claim_processor.process_claim(claim_data)

        assert result["status"] == "processed"
        assert result["claim_id"] == "claim_test"
        assert result["conflicts_detected"] == 0
        mock_truth_layer.invalidate_cache.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_claim_invalid_data(self, claim_processor):
        """Test processing with invalid claim data."""
        claim_data = {
            "predicate": "employee_count",
            # Missing required fields
        }

        result = await claim_processor.process_claim(claim_data)

        assert result["status"] == "rejected"
        assert result["reason"] == "validation_failed"
        assert "errors" in result

    @pytest.mark.asyncio
    async def test_process_claim_with_conflicts(self, claim_processor, mock_conflict_detector, mock_graph_operations, mock_truth_layer):
        """Test processing claim that has conflicts."""
        mock_conflict = MockConflict()
        mock_conflict_detector.detect_conflicts_for_claim = AsyncMock(return_value=[mock_conflict])

        claim_data = {
            "id": "claim_new",
            "subject_entity_id": "ent_123",
            "predicate": "employee_count",
            "object_value": "2000",
            "source_id": "src_new_report",
            "confidence": 0.95,
        }

        result = await claim_processor.process_claim(claim_data)

        assert result["status"] == "processed"
        assert result["conflicts_detected"] == 1
        assert len(result["conflict_ids"]) == 1

    @pytest.mark.asyncio
    async def test_process_claim_error_handling(self, claim_processor, mock_conflict_detector):
        """Test error handling in claim processing."""
        mock_conflict_detector.detect_conflicts_for_claim = AsyncMock(
            side_effect=Exception("Database error")
        )

        claim_data = {
            "id": "claim_error",
            "subject_entity_id": "ent_123",
            "predicate": "employee_count",
            "object_value": "1500",
            "source_id": "src_report",
        }

        result = await claim_processor.process_claim(claim_data)

        assert result["status"] == "error"
        assert "Database error" in result["reason"]

    @pytest.mark.asyncio
    async def test_process_claim_publishes_conflict_events(
        self, claim_processor_with_producer, mock_conflict_detector, mock_kafka_producer
    ):
        """Test that conflict events are published when conflicts detected."""
        mock_conflict = MockConflict()
        mock_conflict_detector.detect_conflicts_for_claim = AsyncMock(return_value=[mock_conflict])

        claim_data = {
            "id": "claim_conflict",
            "subject_entity_id": "ent_123",
            "predicate": "employee_count",
            "object_value": "2000",
            "source_id": "src_new_report",
        }

        await claim_processor_with_producer.process_claim(claim_data)

        mock_kafka_producer.send.assert_called()


class TestClaimsConsumer:
    """Tests for ClaimsConsumer."""

    @pytest.fixture
    def claims_consumer(self, mock_graph_operations, mock_conflict_detector, mock_truth_layer):
        """Create ClaimsConsumer for testing."""
        with patch("consumer.get_settings") as mock_get_settings:
            mock_settings = MagicMock()
            mock_settings.KAFKA_TOPIC_EXTRACTED_CLAIMS = "extracted-claims"
            mock_settings.kafka = MagicMock()
            mock_settings.kafka.bootstrap_servers = "localhost:9092"
            mock_get_settings.return_value = mock_settings

            consumer = ClaimsConsumer(
                graph_operations=mock_graph_operations,
                conflict_detector=mock_conflict_detector,
                truth_layer=mock_truth_layer,
                group_id="test-group",
            )
            return consumer

    def test_claims_consumer_creation(self, claims_consumer):
        """Test ClaimsConsumer can be created."""
        assert claims_consumer is not None
        assert claims_consumer.group_id == "test-group"
        assert claims_consumer._running is False

    def test_claims_consumer_initial_state(self, claims_consumer):
        """Test ClaimsConsumer initial state."""
        assert claims_consumer._processed_count == 0
        assert claims_consumer._error_count == 0
        assert claims_consumer._conflict_count == 0
        assert claims_consumer._consumer is None
        assert claims_consumer._producer is None
        assert claims_consumer._processor is None
        assert claims_consumer._task is None

    def test_get_stats(self, claims_consumer):
        """Test getting consumer stats."""
        claims_consumer._processed_count = 100
        claims_consumer._error_count = 5
        claims_consumer._conflict_count = 10

        stats = claims_consumer.get_stats()

        assert stats["running"] is False
        assert stats["group_id"] == "test-group"
        assert stats["processed_count"] == 100
        assert stats["error_count"] == 5
        assert stats["conflict_count"] == 10

    @pytest.mark.asyncio
    async def test_start_when_already_running(self, claims_consumer):
        """Test start when consumer already running."""
        claims_consumer._running = True

        with patch("consumer.logger") as mock_logger:
            await claims_consumer.start()

            mock_logger.warning.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_initializes_components(self, claims_consumer):
        """Test that start initializes Kafka components."""
        with patch("consumer.KafkaConsumer") as MockKafkaConsumer, \
             patch("consumer.KafkaProducer") as MockKafkaProducer:

            mock_consumer = AsyncMock()
            mock_consumer.start = AsyncMock()
            MockKafkaConsumer.return_value = mock_consumer

            mock_producer = AsyncMock()
            mock_producer.start = AsyncMock()
            MockKafkaProducer.return_value = mock_producer

            # Patch the consume loop to stop immediately
            async def immediate_stop():
                claims_consumer._running = False

            with patch.object(claims_consumer, "_consume_loop", immediate_stop):
                await claims_consumer.start()

            assert claims_consumer._running is True
            mock_consumer.start.assert_called_once()
            mock_producer.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_when_not_running(self, claims_consumer):
        """Test stop when consumer not running."""
        claims_consumer._running = False

        await claims_consumer.stop()  # Should not raise

    @pytest.mark.asyncio
    async def test_stop_stops_components(self, claims_consumer):
        """Test that stop stops all components."""
        claims_consumer._running = True

        mock_consumer = AsyncMock()
        mock_producer = AsyncMock()

        # Create a real task that completes immediately
        async def noop():
            pass

        mock_task = asyncio.create_task(noop())

        claims_consumer._consumer = mock_consumer
        claims_consumer._producer = mock_producer
        claims_consumer._task = mock_task

        await claims_consumer.stop()

        assert claims_consumer._running is False
        mock_consumer.stop.assert_called_once()
        mock_producer.stop.assert_called_once()


class TestGraphUpdateConsumer:
    """Tests for GraphUpdateConsumer."""

    @pytest.fixture
    def update_consumer(self, mock_graph_operations, mock_truth_layer):
        """Create GraphUpdateConsumer for testing."""
        with patch("consumer.get_settings") as mock_get_settings:
            mock_settings = MagicMock()
            mock_settings.KAFKA_TOPIC_GRAPH_UPDATES = "graph-updates"
            mock_settings.kafka = MagicMock()
            mock_settings.kafka.bootstrap_servers = "localhost:9092"
            mock_get_settings.return_value = mock_settings

            consumer = GraphUpdateConsumer(
                graph_operations=mock_graph_operations,
                truth_layer=mock_truth_layer,
                group_id="test-updates-group",
            )
            return consumer

    def test_update_consumer_creation(self, update_consumer):
        """Test GraphUpdateConsumer can be created."""
        assert update_consumer is not None
        assert update_consumer.group_id == "test-updates-group"
        assert update_consumer._running is False

    def test_update_consumer_initial_state(self, update_consumer):
        """Test GraphUpdateConsumer initial state."""
        assert update_consumer._consumer is None
        assert update_consumer._task is None

    @pytest.mark.asyncio
    async def test_start_when_already_running(self, update_consumer):
        """Test start when consumer already running."""
        update_consumer._running = True

        with patch("consumer.KafkaConsumer"):
            # Should return early without starting
            await update_consumer.start()
            # No exception means success

    @pytest.mark.asyncio
    async def test_start_initializes_consumer(self, update_consumer):
        """Test that start initializes Kafka consumer."""
        with patch("consumer.KafkaConsumer") as MockKafkaConsumer:
            mock_consumer = AsyncMock()
            mock_consumer.start = AsyncMock()
            MockKafkaConsumer.return_value = mock_consumer

            async def immediate_stop():
                update_consumer._running = False

            with patch.object(update_consumer, "_consume_loop", immediate_stop):
                await update_consumer.start()

            assert update_consumer._running is True
            mock_consumer.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_when_not_running(self, update_consumer):
        """Test stop when consumer not running."""
        update_consumer._running = False

        await update_consumer.stop()  # Should not raise

    @pytest.mark.asyncio
    async def test_stop_stops_consumer(self, update_consumer):
        """Test that stop stops consumer."""
        update_consumer._running = True

        mock_consumer = AsyncMock()

        # Create a real task that completes immediately
        async def noop():
            pass

        mock_task = asyncio.create_task(noop())

        update_consumer._consumer = mock_consumer
        update_consumer._task = mock_task

        await update_consumer.stop()

        assert update_consumer._running is False
        mock_consumer.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_entity_merged_event(self, update_consumer, mock_truth_layer):
        """Test handling entity merged event."""
        event = {
            "event_type": "entity_merged",
            "target_entity_id": "ent_target",
            "source_entity_ids": ["ent_source_1", "ent_source_2"],
        }

        await update_consumer._process_update(event)

        # Should invalidate cache for target and all sources
        assert mock_truth_layer.invalidate_cache.call_count == 3

    @pytest.mark.asyncio
    async def test_process_claim_superseded_event(self, update_consumer, mock_truth_layer):
        """Test handling claim superseded event."""
        event = {
            "event_type": "claim_superseded",
            "entity_id": "ent_123",
        }

        await update_consumer._process_update(event)

        mock_truth_layer.invalidate_cache.assert_called_once_with("ent_123")

    @pytest.mark.asyncio
    async def test_process_conflict_resolved_event(self, update_consumer, mock_truth_layer):
        """Test handling conflict resolved event."""
        event = {
            "event_type": "conflict_resolved",
            "entity_id": "ent_123",
            "predicate": "employee_count",
            "winning_claim_id": "claim_winner",
        }

        await update_consumer._process_update(event)

        mock_truth_layer.propagate_resolution.assert_called_once_with(
            "ent_123", "employee_count", "claim_winner"
        )

    @pytest.mark.asyncio
    async def test_process_conflict_resolved_missing_fields(self, update_consumer, mock_truth_layer):
        """Test handling conflict resolved event with missing fields."""
        event = {
            "event_type": "conflict_resolved",
            "entity_id": "ent_123",
            # Missing predicate and winning_claim_id
        }

        await update_consumer._process_update(event)

        # Should not call propagate_resolution with missing fields
        mock_truth_layer.propagate_resolution.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_unknown_event_type(self, update_consumer):
        """Test handling unknown event type."""
        event = {
            "event_type": "unknown_event",
            "data": "test",
        }

        with patch("consumer.logger") as mock_logger:
            await update_consumer._process_update(event)

            mock_logger.warning.assert_called_once()


class TestMessageDeserialization:
    """Tests for message deserialization."""

    def test_deserialize_valid_json(self):
        """Test deserializing valid JSON message."""
        message_bytes = b'{"type": "claim", "data": {"id": "123"}}'

        with patch('consumer.KafkaConsumer'), patch('consumer.KafkaProducer'):
            data = json.loads(message_bytes.decode('utf-8'))
            assert data["type"] == "claim"
            assert data["data"]["id"] == "123"

    def test_deserialize_invalid_json(self):
        """Test deserializing invalid JSON message."""
        message_bytes = b'not valid json'

        with pytest.raises(json.JSONDecodeError):
            json.loads(message_bytes.decode('utf-8'))


class TestAddClaimToGraph:
    """Tests for _add_claim_to_graph method."""

    @pytest.mark.asyncio
    async def test_add_claim_without_conflicts(self, claim_processor, mock_graph_operations):
        """Test adding claim without conflicts."""
        claim_data = {
            "id": "claim_123",
            "subject_entity_id": "ent_123",
            "predicate": "employee_count",
            "object_value": "1500",
            "source_id": "src_report",
            "confidence": 0.9,
            "extracted_text": "1500 employees",
            "valid_from": datetime.utcnow().isoformat(),
        }

        result = await claim_processor._add_claim_to_graph(claim_data, [])

        assert result == "claim_123"
        mock_graph_operations.client.execute_query.assert_called_once()

    @pytest.mark.asyncio
    async def test_add_claim_with_conflicts(self, claim_processor, mock_graph_operations):
        """Test adding claim with conflicts."""
        claim_data = {
            "id": "claim_123",
            "subject_entity_id": "ent_123",
            "predicate": "employee_count",
            "object_value": "1500",
            "source_id": "src_report",
            "confidence": 0.9,
            "valid_from": datetime.utcnow().isoformat(),
        }

        conflicts = [MockConflict()]

        result = await claim_processor._add_claim_to_graph(claim_data, conflicts)

        assert result == "claim_123"


class TestCreateConflictRecord:
    """Tests for _create_conflict_record method."""

    @pytest.mark.asyncio
    async def test_create_conflict_record(self, claim_processor, mock_graph_operations):
        """Test creating conflict record."""
        mock_conflict = MockConflict(claim_ids=["claim_old", "claim_new"])

        result = await claim_processor._create_conflict_record("claim_new", mock_conflict)

        assert result == "conflict_123"
        mock_graph_operations.client.execute_query.assert_called()


class TestPublishConflictEvents:
    """Tests for _publish_conflict_events method."""

    @pytest.mark.asyncio
    async def test_publish_conflict_events(self, claim_processor_with_producer, mock_kafka_producer):
        """Test publishing conflict events."""
        conflicts = [MockConflict()]

        await claim_processor_with_producer._publish_conflict_events(conflicts)

        mock_kafka_producer.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_publish_conflict_events_no_producer(self, claim_processor):
        """Test publishing with no producer (should do nothing)."""
        conflicts = [MockConflict()]

        # Should not raise
        await claim_processor._publish_conflict_events(conflicts)

    @pytest.mark.asyncio
    async def test_publish_multiple_conflict_events(self, claim_processor_with_producer, mock_kafka_producer):
        """Test publishing multiple conflict events."""
        conflicts = [MockConflict(), MockConflict(conflict_id="conflict_456")]

        await claim_processor_with_producer._publish_conflict_events(conflicts)

        assert mock_kafka_producer.send.call_count == 2
