"""
Kafka Client Utilities
======================

This module provides Kafka producer and consumer utilities for DocWeave services.
It wraps the aiokafka library with additional features like retry logic,
serialization, and connection management.

Topics:
    - document-events: Document lifecycle events
    - parsed-documents: Parsed document data
    - extracted-claims: Claims extracted from documents
    - graph-updates: Graph modification events
    - conflicts: Detected claim conflicts
"""

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, AsyncIterator, Callable, Optional
from uuid import UUID, uuid4

import orjson
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from shared.config import get_settings

logger = logging.getLogger(__name__)


class KafkaTopics(str, Enum):
    """
    Kafka topic definitions for DocWeave.

    Attributes:
        DOCUMENT_EVENTS: Document lifecycle events
        PARSED_DOCUMENTS: Parsed document data
        EXTRACTED_CLAIMS: Claims extracted from documents
        GRAPH_UPDATES: Graph modification events
        CONFLICTS: Detected claim conflicts
    """
    DOCUMENT_EVENTS = "document-events"
    PARSED_DOCUMENTS = "parsed-documents"
    EXTRACTED_CLAIMS = "extracted-claims"
    GRAPH_UPDATES = "graph-updates"
    CONFLICTS = "conflicts"


@dataclass
class KafkaMessage:
    """
    Represents a Kafka message for DocWeave.

    Attributes:
        topic: Target topic
        key: Message key (for partitioning)
        value: Message payload
        headers: Optional message headers
        timestamp: Message timestamp
        partition: Target partition (None for auto)
        offset: Message offset (set after consumption)
    """
    topic: str
    key: Optional[str]
    value: dict[str, Any]
    headers: dict[str, str] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    partition: Optional[int] = None
    offset: Optional[int] = None

    def to_bytes(self) -> tuple[Optional[bytes], bytes]:
        """
        Serialize message to bytes for Kafka.

        Returns:
            Tuple of (key_bytes, value_bytes)
        """
        key_bytes = self.key.encode("utf-8") if self.key else None
        value_bytes = orjson.dumps(self.value, default=self._json_serializer)
        return key_bytes, value_bytes

    @staticmethod
    def _json_serializer(obj: Any) -> Any:
        """Custom JSON serializer for special types."""
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Enum):
            return obj.value
        if isinstance(obj, bytes):
            return obj.decode("utf-8", errors="replace")
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    @classmethod
    def from_kafka(
        cls,
        topic: str,
        key: Optional[bytes],
        value: bytes,
        timestamp: int,
        partition: int,
        offset: int,
        headers: list[tuple[str, bytes]],
    ) -> "KafkaMessage":
        """
        Create KafkaMessage from raw Kafka data.

        Args:
            topic: Topic name
            key: Raw key bytes
            value: Raw value bytes
            timestamp: Kafka timestamp (milliseconds)
            partition: Partition number
            offset: Message offset
            headers: Raw headers

        Returns:
            KafkaMessage instance
        """
        return cls(
            topic=topic,
            key=key.decode("utf-8") if key else None,
            value=orjson.loads(value),
            headers={k: v.decode("utf-8") for k, v in headers} if headers else {},
            timestamp=datetime.fromtimestamp(timestamp / 1000),
            partition=partition,
            offset=offset,
        )


class KafkaProducer:
    """
    Async Kafka producer with retry logic and connection management.

    Example:
        async with KafkaProducer() as producer:
            await producer.send("topic", {"key": "value"})
    """

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        client_id: Optional[str] = None,
    ):
        """
        Initialize Kafka producer.

        Args:
            bootstrap_servers: Kafka broker addresses (comma-separated)
            client_id: Client identifier for logging
        """
        settings = get_settings()
        self._bootstrap_servers = bootstrap_servers or settings.kafka.bootstrap_servers
        self._client_id = client_id or f"docweave-producer-{uuid4().hex[:8]}"
        self._producer: Optional[AIOKafkaProducer] = None
        self._started = False

    async def start(self) -> None:
        """Start the producer connection."""
        if self._started:
            return

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            client_id=self._client_id,
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            value_serializer=lambda v: orjson.dumps(v, default=KafkaMessage._json_serializer),
            acks="all",
            retry_backoff_ms=100,
            request_timeout_ms=30000,
            max_request_size=10485760,  # 10MB
            compression_type="gzip",
        )

        await self._producer.start()
        self._started = True
        logger.info(f"Kafka producer started: {self._client_id}")

    async def stop(self) -> None:
        """Stop the producer connection."""
        if self._producer and self._started:
            await self._producer.stop()
            self._started = False
            logger.info(f"Kafka producer stopped: {self._client_id}")

    async def __aenter__(self) -> "KafkaProducer":
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.stop()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(KafkaError),
    )
    async def send(
        self,
        topic: str,
        value: dict[str, Any],
        key: Optional[str] = None,
        headers: Optional[dict[str, str]] = None,
        partition: Optional[int] = None,
    ) -> None:
        """
        Send a message to a Kafka topic.

        Args:
            topic: Target topic
            value: Message payload (will be JSON serialized)
            key: Optional message key for partitioning
            headers: Optional message headers
            partition: Optional target partition

        Raises:
            RuntimeError: If producer is not started
            KafkaError: If send fails after retries
        """
        if not self._started or not self._producer:
            raise RuntimeError("Producer not started. Call start() first or use context manager.")

        kafka_headers = [(k, v.encode("utf-8")) for k, v in (headers or {}).items()]

        await self._producer.send_and_wait(
            topic=topic,
            value=value,
            key=key,
            headers=kafka_headers if kafka_headers else None,
            partition=partition,
        )

        logger.debug(f"Message sent to {topic}: key={key}")

    async def send_message(self, message: KafkaMessage) -> None:
        """
        Send a KafkaMessage instance.

        Args:
            message: KafkaMessage to send
        """
        await self.send(
            topic=message.topic,
            value=message.value,
            key=message.key,
            headers=message.headers,
            partition=message.partition,
        )

    async def send_batch(
        self,
        topic: str,
        messages: list[dict[str, Any]],
        key_fn: Optional[Callable[[dict], str]] = None,
    ) -> None:
        """
        Send multiple messages to a topic.

        Args:
            topic: Target topic
            messages: List of message payloads
            key_fn: Optional function to extract key from each message
        """
        for msg in messages:
            key = key_fn(msg) if key_fn else None
            await self.send(topic, msg, key)


class KafkaConsumer:
    """
    Async Kafka consumer with connection management and message handling.

    Example:
        async with KafkaConsumer(["topic1", "topic2"]) as consumer:
            async for message in consumer.consume():
                process(message)
    """

    def __init__(
        self,
        topics: list[str],
        group_id: Optional[str] = None,
        bootstrap_servers: Optional[str] = None,
        auto_offset_reset: str = "earliest",
        enable_auto_commit: bool = True,
    ):
        """
        Initialize Kafka consumer.

        Args:
            topics: List of topics to subscribe to
            group_id: Consumer group identifier
            bootstrap_servers: Kafka broker addresses
            auto_offset_reset: Where to start consuming (earliest/latest)
            enable_auto_commit: Auto-commit offsets
        """
        settings = get_settings()
        self._topics = topics
        self._group_id = group_id or settings.kafka.consumer_group_id
        self._bootstrap_servers = bootstrap_servers or settings.kafka.bootstrap_servers
        self._auto_offset_reset = auto_offset_reset
        self._enable_auto_commit = enable_auto_commit
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._started = False

    async def start(self) -> None:
        """Start the consumer connection."""
        if self._started:
            return

        self._consumer = AIOKafkaConsumer(
            *self._topics,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            auto_offset_reset=self._auto_offset_reset,
            enable_auto_commit=self._enable_auto_commit,
            value_deserializer=lambda v: orjson.loads(v),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            max_poll_records=500,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
        )

        await self._consumer.start()
        self._started = True
        logger.info(f"Kafka consumer started: group={self._group_id}, topics={self._topics}")

    async def stop(self) -> None:
        """Stop the consumer connection."""
        if self._consumer and self._started:
            await self._consumer.stop()
            self._started = False
            logger.info(f"Kafka consumer stopped: group={self._group_id}")

    async def __aenter__(self) -> "KafkaConsumer":
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.stop()

    async def consume(self) -> AsyncIterator[KafkaMessage]:
        """
        Consume messages from subscribed topics.

        Yields:
            KafkaMessage instances

        Raises:
            RuntimeError: If consumer is not started
        """
        if not self._started or not self._consumer:
            raise RuntimeError("Consumer not started. Call start() first or use context manager.")

        async for msg in self._consumer:
            yield KafkaMessage.from_kafka(
                topic=msg.topic,
                key=msg.key.encode("utf-8") if isinstance(msg.key, str) else msg.key,
                value=orjson.dumps(msg.value),  # Re-serialize for from_kafka
                timestamp=msg.timestamp,
                partition=msg.partition,
                offset=msg.offset,
                headers=msg.headers or [],
            )

    async def consume_batch(
        self,
        max_records: int = 100,
        timeout_ms: int = 1000,
    ) -> list[KafkaMessage]:
        """
        Consume a batch of messages.

        Args:
            max_records: Maximum records to return
            timeout_ms: Timeout in milliseconds

        Returns:
            List of KafkaMessage instances
        """
        if not self._started or not self._consumer:
            raise RuntimeError("Consumer not started.")

        batch = await self._consumer.getmany(
            timeout_ms=timeout_ms,
            max_records=max_records,
        )

        messages = []
        for tp, records in batch.items():
            for msg in records:
                messages.append(KafkaMessage.from_kafka(
                    topic=msg.topic,
                    key=msg.key.encode("utf-8") if isinstance(msg.key, str) else msg.key,
                    value=orjson.dumps(msg.value),
                    timestamp=msg.timestamp,
                    partition=msg.partition,
                    offset=msg.offset,
                    headers=msg.headers or [],
                ))

        return messages

    async def commit(self) -> None:
        """Manually commit current offsets."""
        if self._consumer:
            await self._consumer.commit()


class KafkaClient:
    """
    Unified Kafka client providing both producer and consumer functionality.

    Example:
        client = KafkaClient()
        await client.start()

        # Produce
        await client.produce("topic", {"data": "value"})

        # Consume
        async for msg in client.consume(["topic"]):
            process(msg)

        await client.stop()
    """

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        group_id: Optional[str] = None,
    ):
        """
        Initialize Kafka client.

        Args:
            bootstrap_servers: Kafka broker addresses
            group_id: Consumer group identifier
        """
        settings = get_settings()
        self._bootstrap_servers = bootstrap_servers or settings.kafka.bootstrap_servers
        self._group_id = group_id or settings.kafka.consumer_group_id
        self._producer: Optional[KafkaProducer] = None
        self._consumers: dict[str, KafkaConsumer] = {}

    async def start(self) -> None:
        """Start the producer."""
        self._producer = KafkaProducer(self._bootstrap_servers)
        await self._producer.start()

    async def stop(self) -> None:
        """Stop all connections."""
        if self._producer:
            await self._producer.stop()

        for consumer in self._consumers.values():
            await consumer.stop()
        self._consumers.clear()

    async def __aenter__(self) -> "KafkaClient":
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.stop()

    async def produce(
        self,
        topic: str,
        value: dict[str, Any],
        key: Optional[str] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> None:
        """
        Produce a message to a topic.

        Args:
            topic: Target topic
            value: Message payload
            key: Optional message key
            headers: Optional message headers
        """
        if not self._producer:
            raise RuntimeError("Client not started.")
        await self._producer.send(topic, value, key, headers)

    async def consume(
        self,
        topics: list[str],
        group_id: Optional[str] = None,
    ) -> AsyncIterator[KafkaMessage]:
        """
        Consume messages from topics.

        Args:
            topics: Topics to subscribe to
            group_id: Optional consumer group override

        Yields:
            KafkaMessage instances
        """
        key = ",".join(sorted(topics))
        if key not in self._consumers:
            consumer = KafkaConsumer(
                topics,
                group_id=group_id or self._group_id,
                bootstrap_servers=self._bootstrap_servers,
            )
            await consumer.start()
            self._consumers[key] = consumer

        async for msg in self._consumers[key].consume():
            yield msg

    async def health_check(self) -> bool:
        """
        Check Kafka connectivity.

        Returns:
            True if connected, False otherwise
        """
        try:
            if not self._producer:
                self._producer = KafkaProducer(self._bootstrap_servers)
                await self._producer.start()
            return self._producer._started
        except Exception as e:
            logger.error(f"Kafka health check failed: {e}")
            return False
