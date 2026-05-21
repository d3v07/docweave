"""Shared utility clients for DocWeave services."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from shared.utils.kafka_client import (
        KafkaClient,
        KafkaConsumer,
        KafkaMessage,
        KafkaProducer,
        KafkaTopics,
    )
    from shared.utils.neo4j_client import (
        Neo4jClient,
        Neo4jConnectionPool,
        Neo4jQueryBuilder,
    )

__all__ = [
    # Kafka utilities
    "KafkaClient",
    "KafkaProducer",
    "KafkaConsumer",
    "KafkaMessage",
    "KafkaTopics",
    # Neo4j utilities
    "Neo4jClient",
    "Neo4jConnectionPool",
    "Neo4jQueryBuilder",
]


def __getattr__(name: str) -> Any:
    if name in {"KafkaClient", "KafkaProducer", "KafkaConsumer", "KafkaMessage", "KafkaTopics"}:
        kafka_client = import_module("shared.utils.kafka_client")
        return getattr(kafka_client, name)

    if name in {"Neo4jClient", "Neo4jConnectionPool", "Neo4jQueryBuilder"}:
        neo4j_client = import_module("shared.utils.neo4j_client")
        return getattr(neo4j_client, name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
