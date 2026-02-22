"""
Kafka Consumer for Extracted Claims
===================================

This module provides a background Kafka consumer that listens to the
extracted-claims topic and processes claims automatically.

The consumer:
    - Receives claims from the extractor service
    - Detects conflicts with existing claims
    - Updates the knowledge graph
    - Publishes conflict events for resolution
"""

import asyncio
import logging
from datetime import datetime
from typing import Any, Optional
from uuid import uuid4

from shared.config import get_settings
from shared.utils.kafka_client import KafkaConsumer, KafkaProducer, KafkaTopics

logger = logging.getLogger(__name__)


class ClaimProcessor:
    """
    Processes incoming claims from the Kafka topic.

    Handles the full claim lifecycle:
    1. Receive claim from extractor
    2. Validate and normalize claim
    3. Check for conflicts
    4. Update graph
    5. Publish conflict events if needed
    """

    def __init__(
        self,
        graph_operations,
        conflict_detector,
        truth_layer,
        producer: Optional[KafkaProducer] = None,
    ):
        """
        Initialize the claim processor.

        Args:
            graph_operations: GraphOperations instance for graph updates
            conflict_detector: ConflictDetector for finding conflicts
            truth_layer: TruthLayer for truth updates
            producer: Optional Kafka producer for publishing events
        """
        self.graph_ops = graph_operations
        self.detector = conflict_detector
        self.truth_layer = truth_layer
        self.producer = producer
        self.settings = get_settings()

    async def process_claim(self, claim_data: dict[str, Any]) -> dict[str, Any]:
        """
        Process a single claim from the Kafka topic.

        Args:
            claim_data: The claim data from the extractor

        Returns:
            Processing result with claim ID and any conflicts
        """
        logger.info(f"Processing claim: {claim_data.get('id', 'unknown')}")

        try:
            # Validate required fields
            validation_result = self._validate_claim(claim_data)
            if not validation_result["valid"]:
                logger.warning(f"Invalid claim: {validation_result['errors']}")
                return {
                    "status": "rejected",
                    "claim_id": claim_data.get("id"),
                    "reason": "validation_failed",
                    "errors": validation_result["errors"],
                }

            # Normalize claim data
            normalized = self._normalize_claim(claim_data)

            # Detect conflicts
            conflicts = await self.detector.detect_conflicts_for_claim(normalized)

            # Add claim to graph
            claim_id = await self._add_claim_to_graph(normalized, conflicts)

            # Create conflict relationships if any
            conflict_ids = []
            if conflicts:
                for conflict in conflicts:
                    conflict_id = await self._create_conflict_record(claim_id, conflict)
                    conflict_ids.append(conflict_id)

                # Publish conflict events
                if self.producer:
                    await self._publish_conflict_events(conflicts)

            # Update truth layer
            await self.truth_layer.invalidate_cache(normalized["subject_entity_id"])

            result = {
                "status": "processed",
                "claim_id": claim_id,
                "conflicts_detected": len(conflicts),
                "conflict_ids": conflict_ids,
                "processed_at": datetime.utcnow().isoformat(),
            }

            logger.info(f"Processed claim {claim_id}: {len(conflicts)} conflicts detected")
            return result

        except Exception as e:
            logger.error(f"Error processing claim: {e}", exc_info=True)
            return {
                "status": "error",
                "claim_id": claim_data.get("id"),
                "reason": str(e),
            }

    def _validate_claim(self, claim_data: dict[str, Any]) -> dict[str, Any]:
        """
        Validate claim data for required fields.

        Args:
            claim_data: The claim to validate

        Returns:
            Validation result with valid flag and errors
        """
        errors = []

        required_fields = ["subject_entity_id", "predicate", "object_value", "source_id"]
        for field in required_fields:
            if not claim_data.get(field):
                errors.append(f"Missing required field: {field}")

        # Validate confidence is in range
        confidence = claim_data.get("confidence", 0.8)
        if not isinstance(confidence, (int, float)) or not 0.0 <= confidence <= 1.0:
            errors.append("Confidence must be a number between 0.0 and 1.0")

        return {"valid": len(errors) == 0, "errors": errors}

    def _normalize_claim(self, claim_data: dict[str, Any]) -> dict[str, Any]:
        """
        Normalize claim data for consistent processing.

        Args:
            claim_data: Raw claim data

        Returns:
            Normalized claim data
        """
        # Ensure claim has an ID
        if not claim_data.get("id"):
            claim_data["id"] = f"claim_{uuid4().hex[:12]}"

        # Normalize predicate (lowercase, underscores)
        predicate = claim_data.get("predicate", "")
        claim_data["predicate"] = predicate.lower().replace(" ", "_").replace("-", "_")

        # Ensure confidence is a float
        claim_data["confidence"] = float(claim_data.get("confidence", 0.8))

        # Add timestamps if missing
        if not claim_data.get("timestamp"):
            claim_data["timestamp"] = datetime.utcnow().isoformat()

        if not claim_data.get("valid_from"):
            claim_data["valid_from"] = datetime.utcnow().isoformat()

        return claim_data

    async def _add_claim_to_graph(
        self,
        claim_data: dict[str, Any],
        conflicts: list,
    ) -> str:
        """
        Add the claim to the Neo4j graph.

        Args:
            claim_data: Normalized claim data
            conflicts: Detected conflicts

        Returns:
            The claim ID
        """
        # Determine initial status based on conflicts
        status = "conflicting" if conflicts else "current"

        query = """
        MATCH (e:Entity {id: $subject_entity_id})
        MATCH (s:Source {id: $source_id})
        MERGE (c:Claim {id: $claim_id})
        ON CREATE SET
            c.predicate = $predicate,
            c.object_value = $object_value,
            c.confidence = $confidence,
            c.extracted_text = $extracted_text,
            c.status = $status,
            c.created_at = datetime(),
            c.valid_from = datetime($valid_from)
        ON MATCH SET
            c.updated_at = datetime()
        MERGE (e)-[:HAS_CLAIM]->(c)
        MERGE (c)-[:SOURCED_FROM]->(s)
        RETURN c.id as id
        """

        result = await self.graph_ops.client.execute_query(query, {
            "subject_entity_id": claim_data["subject_entity_id"],
            "source_id": claim_data["source_id"],
            "claim_id": claim_data["id"],
            "predicate": claim_data["predicate"],
            "object_value": str(claim_data["object_value"]),
            "confidence": claim_data["confidence"],
            "extracted_text": claim_data.get("extracted_text", ""),
            "status": status,
            "valid_from": claim_data.get("valid_from", datetime.utcnow().isoformat()),
        })

        return claim_data["id"]

    async def _create_conflict_record(
        self,
        new_claim_id: str,
        conflict,
    ) -> str:
        """
        Create a conflict relationship in the graph.

        Args:
            new_claim_id: ID of the new claim
            conflict: DetectedConflict object

        Returns:
            The conflict ID
        """
        conflict_id = conflict.id

        # Create bidirectional conflict relationship
        for existing_claim_id in conflict.claim_ids:
            if existing_claim_id != new_claim_id:
                query = """
                MATCH (c1:Claim {id: $claim1_id})
                MATCH (c2:Claim {id: $claim2_id})
                MERGE (c1)-[r:CONFLICTS_WITH]->(c2)
                ON CREATE SET
                    r.id = $conflict_id,
                    r.conflict_type = $conflict_type,
                    r.severity = $severity,
                    r.score = $score,
                    r.status = 'UNRESOLVED',
                    r.detected_at = datetime()
                RETURN r.id as id
                """

                await self.graph_ops.client.execute_query(query, {
                    "claim1_id": new_claim_id,
                    "claim2_id": existing_claim_id,
                    "conflict_id": conflict_id,
                    "conflict_type": conflict.conflict_type.value,
                    "severity": conflict.severity.value,
                    "score": conflict.score,
                })

        return conflict_id

    async def _publish_conflict_events(self, conflicts: list) -> None:
        """
        Publish conflict events to Kafka for processing.

        Args:
            conflicts: List of detected conflicts
        """
        if not self.producer:
            return

        for conflict in conflicts:
            event = {
                "event_type": "conflict_detected",
                "conflict_id": conflict.id,
                "conflict_type": conflict.conflict_type.value,
                "severity": conflict.severity.value,
                "claim_ids": conflict.claim_ids,
                "subject_entity_id": conflict.subject_entity_id,
                "predicate": conflict.predicate,
                "values": [str(v) for v in conflict.values],
                "detected_at": datetime.utcnow().isoformat(),
            }

            await self.producer.send(
                topic=KafkaTopics.CONFLICTS.value,
                value=event,
                key=conflict.subject_entity_id,
            )


class ClaimsConsumer:
    """
    Background Kafka consumer for extracted claims.

    Runs as a background task and continuously processes
    claims from the extracted-claims topic.

    Example:
        consumer = ClaimsConsumer(graph_ops, detector, truth_layer)
        await consumer.start()

        # Later...
        await consumer.stop()
    """

    # Consumer status constants
    STATUS_STOPPED = "stopped"
    STATUS_STARTING = "starting"
    STATUS_RUNNING = "running"
    STATUS_ERROR = "error"

    def __init__(
        self,
        graph_operations,
        conflict_detector,
        truth_layer,
        group_id: str = "graph-updater-claims",
    ):
        """
        Initialize the claims consumer.

        Args:
            graph_operations: GraphOperations instance
            conflict_detector: ConflictDetector instance
            truth_layer: TruthLayer instance
            group_id: Kafka consumer group ID
        """
        self.settings = get_settings()
        self.group_id = group_id
        self._consumer: Optional[KafkaConsumer] = None
        self._producer: Optional[KafkaProducer] = None
        self._processor: Optional[ClaimProcessor] = None
        self._running = False
        self._status = self.STATUS_STOPPED
        self._last_error: Optional[str] = None
        self._task: Optional[asyncio.Task] = None

        # Store dependencies for later initialization
        self._graph_ops = graph_operations
        self._detector = conflict_detector
        self._truth_layer = truth_layer

        # Metrics
        self._processed_count = 0
        self._error_count = 0
        self._conflict_count = 0

    async def start(self) -> None:
        """Start the consumer and begin processing claims."""
        if self._running:
            logger.warning("Consumer already running")
            return

        self._status = self.STATUS_STARTING
        self._last_error = None
        logger.info(f"Starting claims consumer (group: {self.group_id})")

        try:
            # Get bootstrap servers from settings
            bootstrap_servers = self.settings.kafka.bootstrap_servers
            logger.info(f"Connecting to Kafka at: {bootstrap_servers}")

            # Initialize Kafka consumer with retry logic
            self._consumer = KafkaConsumer(
                topics=[self.settings.KAFKA_TOPIC_EXTRACTED_CLAIMS],
                group_id=self.group_id,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )

            # Initialize Kafka producer
            self._producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
            )

            # Start consumer with retry
            await self._start_with_retry(self._consumer, "consumer")
            await self._start_with_retry(self._producer, "producer")

            # Initialize processor
            self._processor = ClaimProcessor(
                graph_operations=self._graph_ops,
                conflict_detector=self._detector,
                truth_layer=self._truth_layer,
                producer=self._producer,
            )

            self._running = True
            self._status = self.STATUS_RUNNING
            self._task = asyncio.create_task(self._consume_loop())

            logger.info("Claims consumer started successfully")

        except Exception as e:
            self._status = self.STATUS_ERROR
            self._last_error = str(e)
            logger.error(f"Failed to start claims consumer: {e}", exc_info=True)
            # Clean up any partially initialized resources
            await self._cleanup()
            raise

    async def _start_with_retry(
        self,
        client,
        client_name: str,
        max_retries: int = 5,
        base_delay: float = 1.0,
    ) -> None:
        """
        Start a Kafka client with exponential backoff retry.

        Args:
            client: The Kafka client (consumer or producer)
            client_name: Name for logging
            max_retries: Maximum number of retry attempts
            base_delay: Base delay between retries in seconds
        """
        last_error = None
        for attempt in range(max_retries):
            try:
                await client.start()
                logger.info(f"Kafka {client_name} started on attempt {attempt + 1}")
                return
            except Exception as e:
                last_error = e
                delay = base_delay * (2 ** attempt)
                logger.warning(
                    f"Failed to start Kafka {client_name} (attempt {attempt + 1}/{max_retries}): {e}. "
                    f"Retrying in {delay:.1f}s..."
                )
                if attempt < max_retries - 1:
                    await asyncio.sleep(delay)

        raise RuntimeError(
            f"Failed to start Kafka {client_name} after {max_retries} attempts: {last_error}"
        )

    async def _cleanup(self) -> None:
        """Clean up resources after a failed start or during stop."""
        if self._consumer:
            try:
                await self._consumer.stop()
            except Exception as e:
                logger.warning(f"Error stopping consumer during cleanup: {e}")
            self._consumer = None

        if self._producer:
            try:
                await self._producer.stop()
            except Exception as e:
                logger.warning(f"Error stopping producer during cleanup: {e}")
            self._producer = None

    async def stop(self) -> None:
        """Stop the consumer gracefully."""
        if not self._running and self._status != self.STATUS_STARTING:
            return

        logger.info("Stopping claims consumer...")
        self._running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        await self._cleanup()
        self._status = self.STATUS_STOPPED

        logger.info(f"Claims consumer stopped. Processed: {self._processed_count}, Errors: {self._error_count}")

    async def _consume_loop(self) -> None:
        """Main consumption loop."""
        logger.info("Starting consumption loop")

        try:
            async for message in self._consumer.consume():
                if not self._running:
                    break

                try:
                    claim_data = message.value
                    result = await self._processor.process_claim(claim_data)

                    if result["status"] == "processed":
                        self._processed_count += 1
                        self._conflict_count += result.get("conflicts_detected", 0)
                    else:
                        self._error_count += 1

                    # Periodic logging
                    if self._processed_count % 100 == 0:
                        logger.info(
                            f"Consumer stats: processed={self._processed_count}, "
                            f"errors={self._error_count}, conflicts={self._conflict_count}"
                        )

                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)
                    self._error_count += 1

        except asyncio.CancelledError:
            logger.info("Consumption loop cancelled")
        except Exception as e:
            logger.error(f"Fatal error in consumption loop: {e}", exc_info=True)
            self._running = False
            self._status = self.STATUS_ERROR
            self._last_error = str(e)

    def get_stats(self) -> dict[str, Any]:
        """
        Get consumer statistics.

        Returns:
            Dictionary with processing statistics
        """
        return {
            "running": self._running,
            "status": self._status,
            "group_id": self.group_id,
            "processed_count": self._processed_count,
            "error_count": self._error_count,
            "conflict_count": self._conflict_count,
            "last_error": self._last_error,
        }


class GraphUpdateConsumer:
    """
    Consumer for graph update events.

    Handles events that require graph modifications such as
    entity merges, claim updates, and resolution propagations.
    """

    # Consumer status constants
    STATUS_STOPPED = "stopped"
    STATUS_STARTING = "starting"
    STATUS_RUNNING = "running"
    STATUS_ERROR = "error"

    def __init__(
        self,
        graph_operations,
        truth_layer,
        group_id: str = "graph-updater-updates",
    ):
        """
        Initialize the graph update consumer.

        Args:
            graph_operations: GraphOperations instance
            truth_layer: TruthLayer instance
            group_id: Kafka consumer group ID
        """
        self.settings = get_settings()
        self.group_id = group_id
        self._graph_ops = graph_operations
        self._truth_layer = truth_layer
        self._consumer: Optional[KafkaConsumer] = None
        self._running = False
        self._status = self.STATUS_STOPPED
        self._last_error: Optional[str] = None
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Start the update consumer."""
        if self._running:
            return

        self._status = self.STATUS_STARTING
        self._last_error = None
        logger.info(f"Starting graph update consumer (group: {self.group_id})")

        try:
            # Get bootstrap servers from settings
            bootstrap_servers = self.settings.kafka.bootstrap_servers
            logger.info(f"Connecting to Kafka at: {bootstrap_servers}")

            self._consumer = KafkaConsumer(
                topics=[self.settings.KAFKA_TOPIC_GRAPH_UPDATES],
                group_id=self.group_id,
                bootstrap_servers=bootstrap_servers,
            )

            # Start with retry
            await self._start_with_retry(self._consumer, "consumer")

            self._running = True
            self._status = self.STATUS_RUNNING
            self._task = asyncio.create_task(self._consume_loop())

            logger.info("Graph update consumer started successfully")

        except Exception as e:
            self._status = self.STATUS_ERROR
            self._last_error = str(e)
            logger.error(f"Failed to start graph update consumer: {e}", exc_info=True)
            # Clean up
            if self._consumer:
                try:
                    await self._consumer.stop()
                except Exception:
                    pass
                self._consumer = None
            raise

    async def _start_with_retry(
        self,
        client,
        client_name: str,
        max_retries: int = 5,
        base_delay: float = 1.0,
    ) -> None:
        """
        Start a Kafka client with exponential backoff retry.

        Args:
            client: The Kafka client (consumer or producer)
            client_name: Name for logging
            max_retries: Maximum number of retry attempts
            base_delay: Base delay between retries in seconds
        """
        last_error = None
        for attempt in range(max_retries):
            try:
                await client.start()
                logger.info(f"Kafka {client_name} started on attempt {attempt + 1}")
                return
            except Exception as e:
                last_error = e
                delay = base_delay * (2 ** attempt)
                logger.warning(
                    f"Failed to start Kafka {client_name} (attempt {attempt + 1}/{max_retries}): {e}. "
                    f"Retrying in {delay:.1f}s..."
                )
                if attempt < max_retries - 1:
                    await asyncio.sleep(delay)

        raise RuntimeError(
            f"Failed to start Kafka {client_name} after {max_retries} attempts: {last_error}"
        )

    async def stop(self) -> None:
        """Stop the update consumer."""
        if not self._running and self._status != self.STATUS_STARTING:
            return

        logger.info("Stopping graph update consumer...")
        self._running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        if self._consumer:
            try:
                await self._consumer.stop()
            except Exception as e:
                logger.warning(f"Error stopping consumer: {e}")
            self._consumer = None

        self._status = self.STATUS_STOPPED
        logger.info("Graph update consumer stopped")

    async def _consume_loop(self) -> None:
        """Main consumption loop for graph updates."""
        logger.info("Starting graph update consumption loop")

        try:
            async for message in self._consumer.consume():
                if not self._running:
                    break

                try:
                    await self._process_update(message.value)
                except Exception as e:
                    logger.error(f"Error processing update: {e}", exc_info=True)

        except asyncio.CancelledError:
            logger.info("Graph update consumption loop cancelled")
        except Exception as e:
            logger.error(f"Fatal error in graph update consumption loop: {e}", exc_info=True)
            self._running = False
            self._status = self.STATUS_ERROR
            self._last_error = str(e)

    async def _process_update(self, event: dict[str, Any]) -> None:
        """
        Process a graph update event.

        Args:
            event: The update event data
        """
        event_type = event.get("event_type")

        if event_type == "entity_merged":
            await self._handle_entity_merge(event)
        elif event_type == "claim_superseded":
            await self._handle_claim_superseded(event)
        elif event_type == "conflict_resolved":
            await self._handle_conflict_resolved(event)
        else:
            logger.warning(f"Unknown event type: {event_type}")

    async def _handle_entity_merge(self, event: dict[str, Any]) -> None:
        """Handle entity merge event."""
        target_id = event.get("target_entity_id")
        source_ids = event.get("source_entity_ids", [])

        logger.info(f"Processing entity merge: {source_ids} -> {target_id}")

        # Invalidate truth cache for all involved entities
        for entity_id in [target_id] + source_ids:
            self._truth_layer.invalidate_cache(entity_id)

    async def _handle_claim_superseded(self, event: dict[str, Any]) -> None:
        """Handle claim superseded event."""
        entity_id = event.get("entity_id")
        self._truth_layer.invalidate_cache(entity_id)

    async def _handle_conflict_resolved(self, event: dict[str, Any]) -> None:
        """Handle conflict resolution event."""
        entity_id = event.get("entity_id")
        predicate = event.get("predicate")
        winning_claim_id = event.get("winning_claim_id")

        if entity_id and predicate and winning_claim_id:
            await self._truth_layer.propagate_resolution(
                entity_id, predicate, winning_claim_id
            )

    def get_stats(self) -> dict[str, Any]:
        """
        Get consumer statistics.

        Returns:
            Dictionary with consumer status
        """
        return {
            "running": self._running,
            "status": self._status,
            "group_id": self.group_id,
            "last_error": self._last_error,
        }
