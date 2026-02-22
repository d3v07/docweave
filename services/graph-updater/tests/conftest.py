"""
Pytest Configuration and Fixtures
===================================

Shared fixtures and configuration for graph-updater tests.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

# Add parent directory to path for direct imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Mock shared modules before any imports that might need them
mock_claim_status = MagicMock()
mock_claim_status.CURRENT = MagicMock()
mock_claim_status.CURRENT.value = "current"
mock_claim_status.SUPERSEDED = MagicMock()
mock_claim_status.SUPERSEDED.value = "superseded"
mock_claim_status.CONFLICTING = MagicMock()
mock_claim_status.CONFLICTING.value = "conflicting"

sys.modules['shared'] = MagicMock()
sys.modules['shared.config'] = MagicMock()
sys.modules['shared.config'].get_settings = MagicMock()
sys.modules['shared.models'] = MagicMock()
sys.modules['shared.models.claim'] = MagicMock()
sys.modules['shared.models.claim'].ClaimStatus = mock_claim_status
sys.modules['shared.utils'] = MagicMock()
sys.modules['shared.utils.kafka_client'] = MagicMock()
sys.modules['shared.utils.kafka_client'].KafkaConsumer = MagicMock()
sys.modules['shared.utils.kafka_client'].KafkaProducer = MagicMock()
sys.modules['shared.utils.kafka_client'].KafkaTopics = MagicMock()
sys.modules['shared.utils.kafka_client'].KafkaTopics.CONFLICTS = MagicMock()
sys.modules['shared.utils.kafka_client'].KafkaTopics.CONFLICTS.value = "conflicts"
sys.modules['shared.utils.neo4j_client'] = MagicMock()

import pytest
import asyncio
from datetime import datetime
from typing import Any, AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

# Configure pytest-asyncio
pytest_plugins = ("pytest_asyncio",)


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


class MockQueryResult:
    """Mock Neo4j query result."""

    def __init__(self, records=None):
        self.records = records or []


class MockNeo4jClient:
    """Mock Neo4j client for testing."""

    def __init__(self):
        self.execute_query = AsyncMock(return_value=MockQueryResult([]))
        self._query_history = []

    async def init(self):
        pass

    async def close(self):
        pass

    async def health_check(self) -> bool:
        return True

    def set_query_response(self, records: list[dict[str, Any]]):
        """Set the response for the next query."""
        self.execute_query.return_value = MockQueryResult(records)

    def set_query_responses(self, responses: list[list[dict[str, Any]]]):
        """Set multiple responses for sequential queries."""
        self.execute_query.side_effect = [
            MockQueryResult(records) for records in responses
        ]


class MockKafkaProducer:
    """Mock Kafka producer for testing."""

    def __init__(self):
        self._started = False
        self._messages = []

    async def start(self):
        self._started = True

    async def stop(self):
        self._started = False

    async def send(self, topic: str, value: dict, key: str = None, **kwargs):
        self._messages.append({
            "topic": topic,
            "key": key,
            "value": value,
        })

    def get_messages(self) -> list[dict]:
        return self._messages

    def clear_messages(self):
        self._messages.clear()


class MockKafkaConsumer:
    """Mock Kafka consumer for testing."""

    def __init__(self, topics=None, **kwargs):
        self._topics = topics or []
        self._started = False
        self._messages = []

    async def start(self):
        self._started = True

    async def stop(self):
        self._started = False

    def add_message(self, message: dict):
        """Add a message to be consumed."""
        self._messages.append(message)

    async def consume(self):
        """Yield messages."""
        for msg in self._messages:
            yield MagicMock(value=msg)
        self._messages.clear()


@pytest.fixture
def mock_neo4j_client():
    """Create a mock Neo4j client."""
    return MockNeo4jClient()


@pytest.fixture
def mock_kafka_producer():
    """Create a mock Kafka producer."""
    return MockKafkaProducer()


@pytest.fixture
def mock_kafka_consumer():
    """Create a mock Kafka consumer."""
    return MockKafkaConsumer()


@pytest.fixture
def sample_entity_data():
    """Sample entity data for testing."""
    return {
        "id": f"ent_{uuid4().hex[:12]}",
        "name": "Acme Corporation",
        "type": "ORGANIZATION",
        "aliases": ["ACME", "Acme Inc", "Acme Corp"],
        "properties": {
            "industry": "Technology",
            "founded": "1990",
        },
    }


@pytest.fixture
def sample_claim_data():
    """Sample claim data for testing."""
    return {
        "id": f"claim_{uuid4().hex[:12]}",
        "subject_entity_id": "ent_acme_corp",
        "predicate": "employee_count",
        "object_value": "1500",
        "source_id": "src_annual_report_2024",
        "confidence": 0.95,
        "extracted_text": "The company employs approximately 1,500 people.",
        "timestamp": datetime.utcnow().isoformat(),
        "valid_from": datetime.utcnow().isoformat(),
    }


@pytest.fixture
def sample_source_data():
    """Sample source data for testing."""
    return {
        "id": f"src_{uuid4().hex[:12]}",
        "uri": "https://example.com/documents/annual_report_2024.pdf",
        "source_type": "DOCUMENT",
        "title": "Annual Report 2024",
        "reliability_score": 0.9,
        "date": "2024-01-15T00:00:00",
    }


@pytest.fixture
def sample_conflict_data():
    """Sample conflict data for testing."""
    return {
        "conflict_id": f"conflict_{uuid4().hex[:8]}",
        "conflict_type": "VALUE_MISMATCH",
        "claim1_id": "claim_old",
        "claim2_id": "claim_new",
        "predicate": "employee_count",
        "value1": "1200",
        "value2": "1500",
        "confidence1": 0.85,
        "confidence2": 0.95,
        "reliability1": 0.8,
        "reliability2": 0.9,
        "entity_id": "ent_acme_corp",
        "detected_at": datetime.utcnow().isoformat(),
        "status": "UNRESOLVED",
    }


@pytest.fixture
def sample_truth_data():
    """Sample truth data for testing."""
    return {
        "entity_id": "ent_acme_corp",
        "truths": {
            "ceo": {
                "value": "John Smith",
                "confidence": 0.95,
                "supporting_claim_ids": ["claim_ceo_1"],
                "source_count": 2,
            },
            "employee_count": {
                "value": "1500",
                "confidence": 0.9,
                "supporting_claim_ids": ["claim_emp_1"],
                "source_count": 1,
            },
            "headquarters": {
                "value": "San Francisco, CA",
                "confidence": 0.88,
                "supporting_claim_ids": ["claim_hq_1", "claim_hq_2"],
                "source_count": 3,
            },
        },
        "has_conflicts": False,
        "conflict_count": 0,
    }


@pytest.fixture
def mock_settings():
    """Mock settings for testing."""
    settings = MagicMock()
    settings.neo4j.uri = "bolt://localhost:7687"
    settings.neo4j.user = "neo4j"
    settings.neo4j.password = "test"
    settings.neo4j.database = "neo4j"
    settings.kafka.bootstrap_servers = "localhost:9092"
    settings.KAFKA_TOPIC_EXTRACTED_CLAIMS = "extracted-claims"
    settings.KAFKA_TOPIC_GRAPH_UPDATES = "graph-updates"
    settings.KAFKA_TOPIC_CONFLICTS = "conflicts"
    return settings


@pytest.fixture
def mock_get_settings(mock_settings):
    """Patch get_settings to return mock settings."""
    with patch("shared.config.get_settings", return_value=mock_settings):
        yield mock_settings


# Utility functions for tests

def create_mock_claim(
    claim_id: str = None,
    entity_id: str = "ent_test",
    predicate: str = "test_predicate",
    value: str = "test_value",
    confidence: float = 0.8,
    status: str = "current",
) -> dict:
    """Create a mock claim for testing."""
    return {
        "id": claim_id or f"claim_{uuid4().hex[:12]}",
        "subject_entity_id": entity_id,
        "predicate": predicate,
        "object_value": value,
        "confidence": confidence,
        "status": status,
        "source_id": f"src_{uuid4().hex[:8]}",
        "created_at": datetime.utcnow().isoformat(),
    }


def create_mock_conflict(
    conflict_id: str = None,
    claim1_id: str = "claim_1",
    claim2_id: str = "claim_2",
    conflict_type: str = "VALUE_MISMATCH",
) -> dict:
    """Create a mock conflict for testing."""
    return {
        "id": conflict_id or f"conflict_{uuid4().hex[:8]}",
        "conflict_type": conflict_type,
        "claim1_id": claim1_id,
        "claim2_id": claim2_id,
        "status": "UNRESOLVED",
        "detected_at": datetime.utcnow().isoformat(),
    }
