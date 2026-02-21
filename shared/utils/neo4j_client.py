"""
Neo4j Client Utilities
======================

This module provides Neo4j connection pool management and query utilities
for DocWeave services. It wraps the official Neo4j Python driver with
additional features like connection pooling, retry logic, and query building.

The knowledge graph uses the following node labels:
    - Entity: Real-world entities (people, companies, products)
    - Claim: Factual assertions about entities
    - Source: Document sources for claims
    - Document: Ingested documents
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, AsyncIterator, Optional
from uuid import UUID

from neo4j import AsyncGraphDatabase, AsyncDriver, AsyncSession
from neo4j.exceptions import (
    Neo4jError,
    ServiceUnavailable,
    SessionExpired,
    TransientError,
)
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from shared.config import get_settings
from shared.models.graph import GraphNode, GraphRelationship, GraphQueryResult

logger = logging.getLogger(__name__)


class Neo4jConnectionPool:
    """
    Neo4j async connection pool manager.

    Manages a pool of connections to Neo4j and provides
    session management with automatic cleanup.

    Example:
        pool = Neo4jConnectionPool()
        await pool.init()

        async with pool.session() as session:
            result = await session.run("MATCH (n) RETURN n LIMIT 10")
            data = await result.data()

        await pool.close()
    """

    def __init__(
        self,
        uri: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        max_connection_pool_size: int = 50,
    ):
        """
        Initialize connection pool.

        Args:
            uri: Neo4j connection URI
            user: Database username
            password: Database password
            database: Database name
            max_connection_pool_size: Maximum connections in pool
        """
        settings = get_settings()
        self._uri = uri or settings.neo4j.uri
        self._user = user or settings.neo4j.user
        self._password = password or settings.neo4j.password
        self._database = database or settings.neo4j.database
        self._max_pool_size = max_connection_pool_size
        self._driver: Optional[AsyncDriver] = None
        self._initialized = False

    async def init(self) -> None:
        """Initialize the connection pool."""
        if self._initialized:
            return

        self._driver = AsyncGraphDatabase.driver(
            self._uri,
            auth=(self._user, self._password),
            max_connection_pool_size=self._max_pool_size,
        )

        # Verify connectivity
        await self._driver.verify_connectivity()
        self._initialized = True
        logger.info(f"Neo4j connection pool initialized: {self._uri}")

    async def close(self) -> None:
        """Close all connections in the pool."""
        if self._driver:
            await self._driver.close()
            self._initialized = False
            logger.info("Neo4j connection pool closed")

    async def __aenter__(self) -> "Neo4jConnectionPool":
        """Async context manager entry."""
        await self.init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()

    @asynccontextmanager
    async def session(self, database: Optional[str] = None) -> AsyncIterator[AsyncSession]:
        """
        Get a session from the pool.

        Args:
            database: Optional database override

        Yields:
            AsyncSession for database operations
        """
        if not self._initialized or not self._driver:
            raise RuntimeError("Connection pool not initialized. Call init() first.")

        session = self._driver.session(database=database or self._database)
        try:
            yield session
        finally:
            await session.close()

    async def health_check(self) -> bool:
        """
        Check Neo4j connectivity.

        Returns:
            True if connected, False otherwise
        """
        try:
            if not self._initialized:
                await self.init()

            async with self.session() as session:
                result = await session.run("RETURN 1 as n")
                await result.consume()
            return True
        except Exception as e:
            logger.error(f"Neo4j health check failed: {e}")
            return False


class Neo4jQueryBuilder:
    """
    Helper class for building Cypher queries.

    Provides methods for common graph operations with
    parameterized queries to prevent injection.

    Example:
        builder = Neo4jQueryBuilder()

        # Create entity
        query, params = builder.create_entity(
            "Entity",
            {"id": "123", "name": "Test"}
        )

        # Find by property
        query, params = builder.find_by_property(
            "Entity", "name", "Test"
        )
    """

    @staticmethod
    def create_node(
        label: str,
        properties: dict[str, Any],
        additional_labels: Optional[list[str]] = None,
    ) -> tuple[str, dict[str, Any]]:
        """
        Build a CREATE node query.

        Args:
            label: Primary node label
            properties: Node properties
            additional_labels: Additional labels to add

        Returns:
            Tuple of (query, parameters)
        """
        labels = [label] + (additional_labels or [])
        label_str = ":".join(labels)

        query = f"""
        CREATE (n:{label_str} $props)
        RETURN n
        """
        return query, {"props": properties}

    @staticmethod
    def merge_node(
        label: str,
        match_properties: dict[str, Any],
        set_properties: Optional[dict[str, Any]] = None,
    ) -> tuple[str, dict[str, Any]]:
        """
        Build a MERGE node query (create if not exists).

        Args:
            label: Node label
            match_properties: Properties to match on
            set_properties: Additional properties to set

        Returns:
            Tuple of (query, parameters)
        """
        query = f"""
        MERGE (n:{label} {{id: $match_props.id}})
        ON CREATE SET n = $match_props
        ON MATCH SET n += $set_props
        RETURN n
        """
        return query, {
            "match_props": match_properties,
            "set_props": set_properties or {},
        }

    @staticmethod
    def find_by_id(label: str, node_id: str) -> tuple[str, dict[str, Any]]:
        """
        Build a query to find node by ID.

        Args:
            label: Node label
            node_id: Node ID

        Returns:
            Tuple of (query, parameters)
        """
        query = f"""
        MATCH (n:{label} {{id: $id}})
        RETURN n
        """
        return query, {"id": node_id}

    @staticmethod
    def find_by_property(
        label: str,
        property_name: str,
        property_value: Any,
        limit: int = 100,
    ) -> tuple[str, dict[str, Any]]:
        """
        Build a query to find nodes by property.

        Args:
            label: Node label
            property_name: Property to match
            property_value: Value to match
            limit: Maximum results

        Returns:
            Tuple of (query, parameters)
        """
        query = f"""
        MATCH (n:{label})
        WHERE n.{property_name} = $value
        RETURN n
        LIMIT $limit
        """
        return query, {"value": property_value, "limit": limit}

    @staticmethod
    def create_relationship(
        source_label: str,
        source_id: str,
        target_label: str,
        target_id: str,
        relationship_type: str,
        properties: Optional[dict[str, Any]] = None,
    ) -> tuple[str, dict[str, Any]]:
        """
        Build a query to create a relationship.

        Args:
            source_label: Source node label
            source_id: Source node ID
            target_label: Target node label
            target_id: Target node ID
            relationship_type: Relationship type
            properties: Relationship properties

        Returns:
            Tuple of (query, parameters)
        """
        query = f"""
        MATCH (source:{source_label} {{id: $source_id}})
        MATCH (target:{target_label} {{id: $target_id}})
        CREATE (source)-[r:{relationship_type} $props]->(target)
        RETURN source, r, target
        """
        return query, {
            "source_id": source_id,
            "target_id": target_id,
            "props": properties or {},
        }

    @staticmethod
    def delete_node(label: str, node_id: str) -> tuple[str, dict[str, Any]]:
        """
        Build a query to delete a node and its relationships.

        Args:
            label: Node label
            node_id: Node ID

        Returns:
            Tuple of (query, parameters)
        """
        query = f"""
        MATCH (n:{label} {{id: $id}})
        DETACH DELETE n
        """
        return query, {"id": node_id}

    @staticmethod
    def update_node(
        label: str,
        node_id: str,
        properties: dict[str, Any],
    ) -> tuple[str, dict[str, Any]]:
        """
        Build a query to update node properties.

        Args:
            label: Node label
            node_id: Node ID
            properties: Properties to update

        Returns:
            Tuple of (query, parameters)
        """
        query = f"""
        MATCH (n:{label} {{id: $id}})
        SET n += $props
        RETURN n
        """
        return query, {"id": node_id, "props": properties}

    @staticmethod
    def find_claims_for_entity(
        entity_id: str,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> tuple[str, dict[str, Any]]:
        """
        Build a query to find claims for an entity.

        Args:
            entity_id: Entity ID
            status: Optional claim status filter
            limit: Maximum results

        Returns:
            Tuple of (query, parameters)
        """
        status_filter = "AND c.status = $status" if status else ""
        query = f"""
        MATCH (e:Entity {{id: $entity_id}})-[:HAS_CLAIM]->(c:Claim)
        WHERE true {status_filter}
        OPTIONAL MATCH (c)-[:FROM_SOURCE]->(s:Source)
        RETURN c, s
        ORDER BY c.timestamp DESC
        LIMIT $limit
        """
        params = {"entity_id": entity_id, "limit": limit}
        if status:
            params["status"] = status
        return query, params

    @staticmethod
    def find_conflicting_claims(
        entity_id: str,
        predicate: str,
    ) -> tuple[str, dict[str, Any]]:
        """
        Build a query to find conflicting claims.

        Args:
            entity_id: Entity ID
            predicate: Claim predicate to check

        Returns:
            Tuple of (query, parameters)
        """
        query = """
        MATCH (e:Entity {id: $entity_id})-[:HAS_CLAIM]->(c:Claim)
        WHERE c.predicate = $predicate AND c.status = 'current'
        WITH e, c
        ORDER BY c.timestamp DESC
        WITH e, collect(c) as claims
        WHERE size(claims) > 1
        UNWIND claims as claim
        MATCH (claim)-[:FROM_SOURCE]->(s:Source)
        RETURN claim, s
        """
        return query, {"entity_id": entity_id, "predicate": predicate}


class Neo4jClient:
    """
    High-level Neo4j client for DocWeave operations.

    Provides async methods for common graph operations
    with built-in retry logic and error handling.

    Example:
        client = Neo4jClient()
        await client.init()

        # Execute raw query
        result = await client.execute_query(
            "MATCH (n) RETURN n LIMIT 10"
        )

        # Create entity
        entity = await client.create_entity({
            "id": "123",
            "name": "Test Entity",
            "entity_type": "person"
        })

        await client.close()
    """

    def __init__(
        self,
        uri: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
    ):
        """
        Initialize Neo4j client.

        Args:
            uri: Neo4j connection URI
            user: Database username
            password: Database password
            database: Database name
        """
        self._pool = Neo4jConnectionPool(uri, user, password, database)
        self._builder = Neo4jQueryBuilder()

    async def init(self) -> None:
        """Initialize the client."""
        await self._pool.init()

    async def close(self) -> None:
        """Close the client."""
        await self._pool.close()

    async def __aenter__(self) -> "Neo4jClient":
        """Async context manager entry."""
        await self.init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()

    async def health_check(self) -> bool:
        """Check Neo4j connectivity."""
        return await self._pool.health_check()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((TransientError, ServiceUnavailable, SessionExpired)),
    )
    async def execute_query(
        self,
        query: str,
        parameters: Optional[dict[str, Any]] = None,
    ) -> GraphQueryResult:
        """
        Execute a Cypher query.

        Args:
            query: Cypher query string
            parameters: Query parameters

        Returns:
            GraphQueryResult with nodes, relationships, and records
        """
        start_time = datetime.utcnow()

        async with self._pool.session() as session:
            result = await session.run(query, parameters or {})
            records = await result.data()

        execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000

        # Parse results into nodes and relationships
        nodes = []
        relationships = []

        for record in records:
            for value in record.values():
                if hasattr(value, "labels"):  # Node
                    try:
                        nodes.append(GraphNode.from_neo4j_node(value))
                    except (ValueError, KeyError):
                        pass
                elif hasattr(value, "type"):  # Relationship
                    pass  # Handle relationship parsing if needed

        return GraphQueryResult(
            nodes=nodes,
            relationships=relationships,
            records=records,
            execution_time_ms=execution_time,
            query=query,
        )

    async def create_entity(self, properties: dict[str, Any]) -> Optional[dict[str, Any]]:
        """
        Create an Entity node.

        Args:
            properties: Entity properties (must include 'id')

        Returns:
            Created entity data or None
        """
        query, params = self._builder.create_node("Entity", properties)
        result = await self.execute_query(query, params)
        return result.records[0] if result.records else None

    async def create_claim(self, properties: dict[str, Any]) -> Optional[dict[str, Any]]:
        """
        Create a Claim node.

        Args:
            properties: Claim properties (must include 'id')

        Returns:
            Created claim data or None
        """
        query, params = self._builder.create_node("Claim", properties)
        result = await self.execute_query(query, params)
        return result.records[0] if result.records else None

    async def create_source(self, properties: dict[str, Any]) -> Optional[dict[str, Any]]:
        """
        Create a Source node.

        Args:
            properties: Source properties (must include 'id')

        Returns:
            Created source data or None
        """
        query, params = self._builder.create_node("Source", properties)
        result = await self.execute_query(query, params)
        return result.records[0] if result.records else None

    async def find_entity(self, entity_id: str) -> Optional[dict[str, Any]]:
        """
        Find an Entity by ID.

        Args:
            entity_id: Entity ID

        Returns:
            Entity data or None
        """
        query, params = self._builder.find_by_id("Entity", entity_id)
        result = await self.execute_query(query, params)
        return result.records[0] if result.records else None

    async def find_claim(self, claim_id: str) -> Optional[dict[str, Any]]:
        """
        Find a Claim by ID.

        Args:
            claim_id: Claim ID

        Returns:
            Claim data or None
        """
        query, params = self._builder.find_by_id("Claim", claim_id)
        result = await self.execute_query(query, params)
        return result.records[0] if result.records else None

    async def link_claim_to_entity(
        self,
        entity_id: str,
        claim_id: str,
    ) -> bool:
        """
        Create HAS_CLAIM relationship between Entity and Claim.

        Args:
            entity_id: Entity ID
            claim_id: Claim ID

        Returns:
            True if successful
        """
        query, params = self._builder.create_relationship(
            "Entity", entity_id,
            "Claim", claim_id,
            "HAS_CLAIM",
        )
        result = await self.execute_query(query, params)
        return len(result.records) > 0

    async def link_claim_to_source(
        self,
        claim_id: str,
        source_id: str,
    ) -> bool:
        """
        Create FROM_SOURCE relationship between Claim and Source.

        Args:
            claim_id: Claim ID
            source_id: Source ID

        Returns:
            True if successful
        """
        query, params = self._builder.create_relationship(
            "Claim", claim_id,
            "Source", source_id,
            "FROM_SOURCE",
        )
        result = await self.execute_query(query, params)
        return len(result.records) > 0

    async def find_claims_for_entity(
        self,
        entity_id: str,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """
        Find all claims for an entity.

        Args:
            entity_id: Entity ID
            status: Optional status filter
            limit: Maximum results

        Returns:
            List of claim records
        """
        query, params = self._builder.find_claims_for_entity(entity_id, status, limit)
        result = await self.execute_query(query, params)
        return result.records

    async def find_conflicting_claims(
        self,
        entity_id: str,
        predicate: str,
    ) -> list[dict[str, Any]]:
        """
        Find conflicting claims for an entity and predicate.

        Args:
            entity_id: Entity ID
            predicate: Claim predicate

        Returns:
            List of conflicting claim records
        """
        query, params = self._builder.find_conflicting_claims(entity_id, predicate)
        result = await self.execute_query(query, params)
        return result.records

    async def update_claim_status(
        self,
        claim_id: str,
        status: str,
        superseded_by: Optional[str] = None,
    ) -> bool:
        """
        Update a claim's status.

        Args:
            claim_id: Claim ID
            status: New status
            superseded_by: ID of superseding claim

        Returns:
            True if successful
        """
        properties = {"status": status, "updated_at": datetime.utcnow().isoformat()}
        if superseded_by:
            properties["superseded_by"] = superseded_by

        query, params = self._builder.update_node("Claim", claim_id, properties)
        result = await self.execute_query(query, params)
        return len(result.records) > 0

    async def search_entities(
        self,
        search_term: str,
        entity_type: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """
        Search for entities by name.

        Args:
            search_term: Search term (partial match)
            entity_type: Optional entity type filter
            limit: Maximum results

        Returns:
            List of matching entities
        """
        type_filter = "AND e.entity_type = $entity_type" if entity_type else ""
        query = f"""
        MATCH (e:Entity)
        WHERE toLower(e.name) CONTAINS toLower($search)
        {type_filter}
        RETURN e
        ORDER BY e.name
        LIMIT $limit
        """
        params = {"search": search_term, "limit": limit}
        if entity_type:
            params["entity_type"] = entity_type

        result = await self.execute_query(query, params)
        return result.records
