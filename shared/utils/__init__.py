"""
DocWeave Utilities Module
=========================

This module provides utility functions and clients for common operations
across DocWeave services, including Kafka messaging and Neo4j graph operations.

Usage:
    from shared.utils import KafkaClient, Neo4jClient

    # Kafka operations
    kafka = KafkaClient()
    await kafka.produce("topic", {"key": "value"})

    # Neo4j operations
    neo4j = Neo4jClient()
    result = await neo4j.execute_query("MATCH (n) RETURN n LIMIT 10")
"""

from shared.utils.kafka_client import (
    KafkaClient,
    KafkaProducer,
    KafkaConsumer,
    KafkaMessage,
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
