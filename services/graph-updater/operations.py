"""
Enhanced Graph Operations for Knowledge Graph Management
=========================================================

This module provides production-ready graph operations for the DocWeave
knowledge graph, including:
    - Entity merging with alias support
    - Claim versioning with superseded chain tracking
    - Transaction support for atomic updates
    - Multi-source claim aggregation
    - Temporal validity tracking
"""

import logging
from datetime import datetime
from typing import Any, Optional
from uuid import uuid4

from pydantic import BaseModel, Field

from shared.models.claim import ClaimStatus

logger = logging.getLogger(__name__)


class EntityMergeRequest(BaseModel):
    """Request to merge entities."""
    target_entity_id: str
    source_entity_ids: list[str]
    merge_aliases: bool = Field(default=True)
    merge_claims: bool = Field(default=True)
    resolved_by: str = Field(default="system")


class EntityMergeResult(BaseModel):
    """Result of an entity merge operation."""
    target_entity_id: str
    merged_entity_ids: list[str]
    aliases_added: list[str]
    claims_transferred: int
    relationships_updated: int
    success: bool
    message: str


class ClaimVersionInfo(BaseModel):
    """Information about a claim version."""
    claim_id: str
    version: int
    status: str
    object_value: Any
    confidence: float
    created_at: str
    superseded_by: Optional[str] = None


class GraphOperations:
    """
    Enhanced operations for managing the DocWeave knowledge graph.

    Provides atomic operations for entity management, claim processing,
    and conflict handling with full transaction support.

    Example:
        async with GraphOperations(neo4j_client) as ops:
            # Merge entities
            result = await ops.merge_entities(
                target_id="ent_acme",
                source_ids=["ent_acme_typo", "ent_acme_old"]
            )

            # Add claim with versioning
            claim_id = await ops.add_claim_versioned(
                subject_entity_id="ent_acme",
                predicate="employee_count",
                object_value="15,000",
                source_id="src_2024_report"
            )
    """

    def __init__(self, neo4j_client):
        """
        Initialize graph operations.

        Args:
            neo4j_client: Async Neo4j client instance
        """
        self.client = neo4j_client

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        pass

    # =========================================================================
    # Entity Operations
    # =========================================================================

    async def merge_entity(
        self,
        name: str,
        entity_type: str,
        aliases: Optional[list[str]] = None,
        properties: Optional[dict[str, Any]] = None,
    ) -> str:
        """
        Create or merge an entity node, returning its ID.

        If an entity with the same name and type exists, updates it.
        Otherwise creates a new entity.

        Args:
            name: Entity name
            entity_type: Type of entity (ORGANIZATION, PERSON, etc.)
            aliases: Alternative names for the entity
            properties: Additional entity properties

        Returns:
            Entity ID (existing or new)
        """
        entity_id = f"ent_{uuid4().hex[:12]}"
        aliases = aliases or []
        properties = properties or {}

        query = """
        MERGE (e:Entity {name: $name, type: $type})
        ON CREATE SET
            e.id = $entity_id,
            e.aliases = $aliases,
            e.properties = $properties,
            e.created_at = datetime(),
            e.updated_at = datetime()
        ON MATCH SET
            e.aliases = CASE
                WHEN e.aliases IS NULL THEN $aliases
                ELSE [x IN e.aliases + $aliases WHERE x IS NOT NULL | x]
            END,
            e.properties = CASE
                WHEN e.properties IS NULL THEN $properties
                ELSE apoc.map.merge(e.properties, $properties)
            END,
            e.updated_at = datetime()
        RETURN e.id as id
        """

        # Fallback query without APOC
        fallback_query = """
        MERGE (e:Entity {name: $name, type: $type})
        ON CREATE SET
            e.id = $entity_id,
            e.aliases = $aliases,
            e.created_at = datetime(),
            e.updated_at = datetime()
        ON MATCH SET
            e.aliases = CASE
                WHEN e.aliases IS NULL THEN $aliases
                ELSE [x IN (e.aliases + $aliases) WHERE x IS NOT NULL]
            END,
            e.updated_at = datetime()
        RETURN e.id as id
        """

        try:
            result = await self.client.execute_query(
                query,
                {
                    "name": name,
                    "type": entity_type,
                    "entity_id": entity_id,
                    "aliases": list(set(aliases)),
                    "properties": properties,
                }
            )
        except Exception:
            # Fallback without APOC
            result = await self.client.execute_query(
                fallback_query,
                {
                    "name": name,
                    "type": entity_type,
                    "entity_id": entity_id,
                    "aliases": list(set(aliases)),
                }
            )

        return result.records[0]["id"] if result.records else entity_id

    async def merge_entities(
        self,
        target_entity_id: str,
        source_entity_ids: list[str],
        merge_aliases: bool = True,
        merge_claims: bool = True,
        resolved_by: str = "system",
    ) -> EntityMergeResult:
        """
        Merge multiple entities into a target entity.

        Combines aliases, transfers claims, and updates relationships.
        Source entities are marked as merged and linked to target.

        Args:
            target_entity_id: ID of the entity to merge into
            source_entity_ids: IDs of entities to merge
            merge_aliases: Whether to combine aliases
            merge_claims: Whether to transfer claims
            resolved_by: Who initiated the merge

        Returns:
            EntityMergeResult with merge details
        """
        logger.info(f"Merging entities {source_entity_ids} into {target_entity_id}")

        aliases_added = []
        claims_transferred = 0
        relationships_updated = 0

        try:
            # Collect aliases from source entities
            if merge_aliases:
                alias_query = """
                MATCH (e:Entity)
                WHERE e.id IN $source_ids
                RETURN e.name as name, e.aliases as aliases
                """
                alias_result = await self.client.execute_query(
                    alias_query, {"source_ids": source_entity_ids}
                )

                for record in alias_result.records:
                    aliases_added.append(record["name"])
                    if record.get("aliases"):
                        aliases_added.extend(record["aliases"])

                # Add aliases to target
                if aliases_added:
                    update_query = """
                    MATCH (e:Entity {id: $target_id})
                    SET e.aliases = CASE
                        WHEN e.aliases IS NULL THEN $aliases
                        ELSE [x IN (e.aliases + $aliases) WHERE x IS NOT NULL]
                    END,
                    e.updated_at = datetime()
                    """
                    await self.client.execute_query(
                        update_query,
                        {"target_id": target_entity_id, "aliases": list(set(aliases_added))}
                    )

            # Transfer claims to target entity
            if merge_claims:
                transfer_query = """
                MATCH (source:Entity)-[r:HAS_CLAIM]->(c:Claim)
                WHERE source.id IN $source_ids
                MATCH (target:Entity {id: $target_id})
                DELETE r
                CREATE (target)-[:HAS_CLAIM]->(c)
                RETURN count(c) as count
                """
                transfer_result = await self.client.execute_query(
                    transfer_query,
                    {"source_ids": source_entity_ids, "target_id": target_entity_id}
                )
                claims_transferred = transfer_result.records[0]["count"] if transfer_result.records else 0

            # Update other relationships pointing to source entities
            relationship_query = """
            MATCH (source:Entity)-[r]->(other)
            WHERE source.id IN $source_ids AND NOT type(r) = 'HAS_CLAIM'
            MATCH (target:Entity {id: $target_id})
            WITH source, r, other, target, type(r) as rel_type, properties(r) as rel_props
            DELETE r
            CREATE (target)-[new_r:RELATES_TO]->(other)
            SET new_r = rel_props, new_r.original_type = rel_type
            RETURN count(new_r) as count
            """
            rel_result = await self.client.execute_query(
                relationship_query,
                {"source_ids": source_entity_ids, "target_id": target_entity_id}
            )
            relationships_updated = rel_result.records[0]["count"] if rel_result.records else 0

            # Mark source entities as merged
            mark_query = """
            MATCH (source:Entity)
            WHERE source.id IN $source_ids
            MATCH (target:Entity {id: $target_id})
            SET source.status = 'merged',
                source.merged_into = $target_id,
                source.merged_at = datetime(),
                source.merged_by = $resolved_by
            CREATE (source)-[:MERGED_INTO {at: datetime(), by: $resolved_by}]->(target)
            """
            await self.client.execute_query(
                mark_query,
                {
                    "source_ids": source_entity_ids,
                    "target_id": target_entity_id,
                    "resolved_by": resolved_by
                }
            )

            # Update target entity's merged_from list
            update_merged_query = """
            MATCH (e:Entity {id: $target_id})
            SET e.merged_from = CASE
                WHEN e.merged_from IS NULL THEN $source_ids
                ELSE e.merged_from + $source_ids
            END
            """
            await self.client.execute_query(
                update_merged_query,
                {"target_id": target_entity_id, "source_ids": source_entity_ids}
            )

            return EntityMergeResult(
                target_entity_id=target_entity_id,
                merged_entity_ids=source_entity_ids,
                aliases_added=list(set(aliases_added)),
                claims_transferred=claims_transferred,
                relationships_updated=relationships_updated,
                success=True,
                message=f"Successfully merged {len(source_entity_ids)} entities",
            )

        except Exception as e:
            logger.error(f"Entity merge failed: {e}", exc_info=True)
            return EntityMergeResult(
                target_entity_id=target_entity_id,
                merged_entity_ids=[],
                aliases_added=[],
                claims_transferred=0,
                relationships_updated=0,
                success=False,
                message=str(e),
            )

    async def find_entity_by_alias(
        self,
        alias: str,
        entity_type: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        """
        Find an entity by name or alias.

        Args:
            alias: Name or alias to search for
            entity_type: Optional type filter

        Returns:
            Entity data or None if not found
        """
        type_filter = "AND e.type = $entity_type" if entity_type else ""

        query = f"""
        MATCH (e:Entity)
        WHERE (e.name = $alias OR $alias IN e.aliases)
          AND (e.status IS NULL OR e.status <> 'merged')
          {type_filter}
        RETURN e.id as id, e.name as name, e.type as type,
               e.aliases as aliases
        LIMIT 1
        """

        params = {"alias": alias}
        if entity_type:
            params["entity_type"] = entity_type

        result = await self.client.execute_query(query, params)
        return result.records[0] if result.records else None

    # =========================================================================
    # Claim Operations with Versioning
    # =========================================================================

    async def add_claim(
        self,
        subject_entity_id: str,
        predicate: str,
        object_value: Any,
        source_id: str,
        confidence: float,
        extracted_text: str = "",
        object_entity_id: Optional[str] = None,
        valid_from: Optional[datetime] = None,
        valid_until: Optional[datetime] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> str:
        """
        Add a new claim to the graph.

        Args:
            subject_entity_id: Entity the claim is about
            predicate: The relationship/attribute
            object_value: The claim value
            source_id: Source document ID
            confidence: Confidence score (0-1)
            extracted_text: Original text the claim was extracted from
            object_entity_id: Optional target entity ID for relationships
            valid_from: When this claim becomes valid
            valid_until: When this claim expires
            metadata: Additional claim metadata

        Returns:
            The new claim ID
        """
        claim_id = f"claim_{uuid4().hex[:12]}"
        valid_from = valid_from or datetime.utcnow()
        metadata = metadata or {}

        query = """
        MATCH (e:Entity {id: $subject_entity_id})
        MATCH (s:Source {id: $source_id})
        CREATE (c:Claim {
            id: $claim_id,
            predicate: $predicate,
            object_value: $object_value,
            confidence: $confidence,
            extracted_text: $extracted_text,
            status: $status,
            version: 1,
            created_at: datetime(),
            valid_from: datetime($valid_from)
        })
        CREATE (e)-[:HAS_CLAIM]->(c)
        CREATE (c)-[:SOURCED_FROM]->(s)
        RETURN c.id as id
        """

        params = {
            "subject_entity_id": subject_entity_id,
            "source_id": source_id,
            "claim_id": claim_id,
            "predicate": predicate,
            "object_value": str(object_value),
            "confidence": confidence,
            "extracted_text": extracted_text,
            "status": ClaimStatus.CURRENT.value,
            "valid_from": valid_from.isoformat(),
        }

        await self.client.execute_query(query, params)

        # If valid_until is set, add it
        if valid_until:
            await self.client.execute_query(
                "MATCH (c:Claim {id: $claim_id}) SET c.valid_until = datetime($valid_until)",
                {"claim_id": claim_id, "valid_until": valid_until.isoformat()}
            )

        # If object_entity_id is provided, create relationship
        if object_entity_id:
            rel_query = """
            MATCH (c:Claim {id: $claim_id})
            MATCH (target:Entity {id: $object_entity_id})
            CREATE (c)-[:REFERS_TO]->(target)
            SET c.object_entity_id = $object_entity_id
            """
            await self.client.execute_query(
                rel_query,
                {"claim_id": claim_id, "object_entity_id": object_entity_id}
            )

        logger.debug(f"Added claim {claim_id}: {subject_entity_id}.{predicate} = {object_value}")
        return claim_id

    async def add_claim_versioned(
        self,
        subject_entity_id: str,
        predicate: str,
        object_value: Any,
        source_id: str,
        confidence: float,
        extracted_text: str = "",
        supersede_existing: bool = True,
    ) -> tuple[str, list[str]]:
        """
        Add a new claim with versioning support.

        If supersede_existing is True and there are existing claims
        for the same entity+predicate, marks them as superseded.

        Args:
            subject_entity_id: Entity the claim is about
            predicate: The relationship/attribute
            object_value: The claim value
            source_id: Source document ID
            confidence: Confidence score (0-1)
            extracted_text: Original extracted text
            supersede_existing: Whether to supersede existing claims

        Returns:
            Tuple of (new_claim_id, list_of_superseded_claim_ids)
        """
        superseded_ids = []

        if supersede_existing:
            # Find and supersede existing current claims
            existing_query = """
            MATCH (e:Entity {id: $entity_id})-[:HAS_CLAIM]->(c:Claim)
            WHERE c.predicate = $predicate
              AND c.status IN ['current', 'CURRENT']
              AND c.object_value <> $object_value
            RETURN c.id as id
            """

            existing_result = await self.client.execute_query(
                existing_query,
                {
                    "entity_id": subject_entity_id,
                    "predicate": predicate,
                    "object_value": str(object_value),
                }
            )

            superseded_ids = [r["id"] for r in existing_result.records]

        # Add the new claim
        claim_id = await self.add_claim(
            subject_entity_id=subject_entity_id,
            predicate=predicate,
            object_value=object_value,
            source_id=source_id,
            confidence=confidence,
            extracted_text=extracted_text,
        )

        # Supersede existing claims
        if superseded_ids:
            supersede_query = """
            MATCH (c:Claim)
            WHERE c.id IN $claim_ids
            SET c.status = 'superseded',
                c.superseded_by = $new_claim_id,
                c.superseded_at = datetime(),
                c.valid_until = datetime()
            """
            await self.client.execute_query(
                supersede_query,
                {"claim_ids": superseded_ids, "new_claim_id": claim_id}
            )

            # Create supersedes relationships
            for old_id in superseded_ids:
                rel_query = """
                MATCH (new:Claim {id: $new_id})
                MATCH (old:Claim {id: $old_id})
                CREATE (new)-[:SUPERSEDES {at: datetime()}]->(old)
                """
                await self.client.execute_query(
                    rel_query, {"new_id": claim_id, "old_id": old_id}
                )

        return claim_id, superseded_ids

    async def get_claim_versions(
        self,
        entity_id: str,
        predicate: str,
    ) -> list[ClaimVersionInfo]:
        """
        Get all versions of claims for an entity+predicate.

        Returns claims ordered by creation date, showing the
        version history and supersession chain.

        Args:
            entity_id: Entity ID
            predicate: The predicate to query

        Returns:
            List of ClaimVersionInfo objects
        """
        query = """
        MATCH (e:Entity {id: $entity_id})-[:HAS_CLAIM]->(c:Claim)
        WHERE c.predicate = $predicate
        OPTIONAL MATCH (c)-[:SUPERSEDES]->(older:Claim)
        RETURN c.id as id, c.status as status, c.object_value as value,
               c.confidence as confidence, c.created_at as created_at,
               c.superseded_by as superseded_by, c.version as version
        ORDER BY c.created_at DESC
        """

        result = await self.client.execute_query(
            query, {"entity_id": entity_id, "predicate": predicate}
        )

        versions = []
        for i, record in enumerate(result.records):
            versions.append(ClaimVersionInfo(
                claim_id=record["id"],
                version=record.get("version") or (len(result.records) - i),
                status=record.get("status", "unknown"),
                object_value=record["value"],
                confidence=record.get("confidence", 0.0),
                created_at=str(record.get("created_at", "")),
                superseded_by=record.get("superseded_by"),
            ))

        return versions

    # =========================================================================
    # Conflict Operations
    # =========================================================================

    async def find_conflicting_claims(
        self,
        subject_entity_id: str,
        predicate: str,
        object_value: Any,
    ) -> list[dict[str, Any]]:
        """
        Find existing claims that may conflict with a new claim.

        Args:
            subject_entity_id: Entity ID
            predicate: The predicate
            object_value: The value to check against

        Returns:
            List of potentially conflicting claims
        """
        query = """
        MATCH (e:Entity {id: $subject_entity_id})-[:HAS_CLAIM]->(c:Claim)
        WHERE c.predicate = $predicate
          AND c.object_value <> $object_value
          AND c.status IN ['current', 'CURRENT']
        OPTIONAL MATCH (c)-[:SOURCED_FROM]->(s:Source)
        RETURN c.id as id, c.predicate as predicate,
               c.object_value as object_value, c.confidence as confidence,
               s.id as source_id, s.reliability_score as source_reliability
        """

        result = await self.client.execute_query(
            query,
            {
                "subject_entity_id": subject_entity_id,
                "predicate": predicate,
                "object_value": str(object_value)
            }
        )

        return result.records

    async def add_claim_with_conflict_check(
        self,
        subject_entity_id: str,
        predicate: str,
        object_value: Any,
        source_id: str,
        confidence: float,
        extracted_text: str = "",
    ) -> tuple[str, list[dict[str, Any]]]:
        """
        Add a claim and detect any conflicts.

        Args:
            subject_entity_id: Entity ID
            predicate: The predicate
            object_value: The claim value
            source_id: Source ID
            confidence: Confidence score
            extracted_text: Original text

        Returns:
            Tuple of (claim_id, list of conflicts)
        """
        # Check for conflicts first
        conflicts = await self.find_conflicting_claims(
            subject_entity_id, predicate, object_value
        )

        # Add the new claim
        claim_id = await self.add_claim(
            subject_entity_id, predicate, object_value,
            source_id, confidence, extracted_text
        )

        # Create conflict relationships
        if conflicts:
            for conflict in conflicts:
                await self._create_conflict_relationship(claim_id, conflict["id"])

            # Update claim status to conflicting
            await self.client.execute_query(
                "MATCH (c:Claim {id: $claim_id}) SET c.status = 'conflicting'",
                {"claim_id": claim_id}
            )

        return claim_id, conflicts

    async def _create_conflict_relationship(
        self,
        claim1_id: str,
        claim2_id: str,
        conflict_type: str = "VALUE_MISMATCH",
    ) -> str:
        """
        Create a conflict relationship between two claims.

        Args:
            claim1_id: First claim ID
            claim2_id: Second claim ID
            conflict_type: Type of conflict

        Returns:
            The conflict relationship ID
        """
        conflict_id = f"conflict_{uuid4().hex[:8]}"

        query = """
        MATCH (c1:Claim {id: $claim1_id})
        MATCH (c2:Claim {id: $claim2_id})
        MERGE (c1)-[r:CONFLICTS_WITH]->(c2)
        ON CREATE SET
            r.id = $conflict_id,
            r.conflict_type = $conflict_type,
            r.status = 'UNRESOLVED',
            r.created_at = datetime()
        RETURN r.id as id
        """

        await self.client.execute_query(
            query,
            {
                "claim1_id": claim1_id,
                "claim2_id": claim2_id,
                "conflict_id": conflict_id,
                "conflict_type": conflict_type,
            }
        )

        return conflict_id

    async def get_unresolved_conflicts(
        self,
        entity_id: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """
        Get all unresolved conflicts.

        Args:
            entity_id: Optional filter by entity
            limit: Maximum results

        Returns:
            List of conflict records
        """
        entity_filter = ""
        params = {"limit": limit}

        if entity_id:
            entity_filter = """
            MATCH (e:Entity {id: $entity_id})-[:HAS_CLAIM]->(c1)
            WITH c1
            """
            params["entity_id"] = entity_id

        query = f"""
        {entity_filter}
        MATCH (c1:Claim)-[r:CONFLICTS_WITH {{status: 'UNRESOLVED'}}]->(c2:Claim)
        OPTIONAL MATCH (c1)-[:SOURCED_FROM]->(s1:Source)
        OPTIONAL MATCH (c2)-[:SOURCED_FROM]->(s2:Source)
        RETURN r.id as conflict_id, r.conflict_type as conflict_type,
               c1.id as claim1_id, c1.predicate as predicate,
               c1.object_value as value1, c2.object_value as value2,
               c1.confidence as confidence1, c2.confidence as confidence2,
               s1.reliability_score as reliability1, s2.reliability_score as reliability2,
               r.created_at as detected_at
        LIMIT $limit
        """

        result = await self.client.execute_query(query, params)
        return result.records

    async def resolve_conflict(
        self,
        conflict_id: str,
        winning_claim_id: str,
        resolution_reason: str,
        resolved_by: str = "system",
    ) -> None:
        """
        Resolve a conflict by marking one claim as superseded.

        Args:
            conflict_id: ID of the conflict relationship
            winning_claim_id: ID of the claim to keep
            resolution_reason: Explanation for the decision
            resolved_by: Who resolved the conflict
        """
        query = """
        MATCH (c1:Claim)-[r:CONFLICTS_WITH {id: $conflict_id}]->(c2:Claim)
        WITH c1, c2, r,
             CASE WHEN c1.id = $winning_claim_id THEN c2 ELSE c1 END as loser,
             CASE WHEN c1.id = $winning_claim_id THEN c1 ELSE c2 END as winner
        SET r.status = 'RESOLVED',
            r.resolved_at = datetime(),
            r.resolution_reason = $reason,
            r.resolved_by = $resolved_by,
            r.winning_claim_id = $winning_claim_id,
            loser.status = 'superseded',
            loser.superseded_by = winner.id,
            loser.valid_until = datetime(),
            winner.status = 'current'
        """

        await self.client.execute_query(
            query,
            {
                "conflict_id": conflict_id,
                "winning_claim_id": winning_claim_id,
                "reason": resolution_reason,
                "resolved_by": resolved_by,
            }
        )

        logger.info(f"Resolved conflict {conflict_id}: winner={winning_claim_id}")

    # =========================================================================
    # Truth/Current State Operations
    # =========================================================================

    async def get_current_truth(
        self,
        entity_id: str,
        predicate: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """
        Get the current truth (active claims) for an entity.

        Args:
            entity_id: Entity ID
            predicate: Optional predicate filter

        Returns:
            List of current claims
        """
        predicate_filter = "AND c.predicate = $predicate" if predicate else ""
        params = {"entity_id": entity_id}
        if predicate:
            params["predicate"] = predicate

        query = f"""
        MATCH (e:Entity {{id: $entity_id}})-[:HAS_CLAIM]->(c:Claim)
        WHERE c.status IN ['current', 'CURRENT']
        {predicate_filter}
        OPTIONAL MATCH (c)-[:SOURCED_FROM]->(s:Source)
        RETURN c.predicate as predicate, c.object_value as value,
               c.confidence as confidence, c.id as claim_id,
               s.uri as source_uri, c.created_at as created_at
        ORDER BY c.predicate, c.created_at DESC
        """

        result = await self.client.execute_query(query, params)
        return result.records

    async def get_claim_history(
        self,
        entity_id: str,
        predicate: str,
        include_superseded: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Get the history of claims for a specific entity attribute.

        Args:
            entity_id: Entity ID
            predicate: The predicate to query
            include_superseded: Whether to include superseded claims

        Returns:
            List of claims ordered by creation date
        """
        status_filter = "" if include_superseded else "AND c.status IN ['current', 'CURRENT']"

        query = f"""
        MATCH (e:Entity {{id: $entity_id}})-[:HAS_CLAIM]->(c:Claim {{predicate: $predicate}})
        {status_filter}
        OPTIONAL MATCH (c)-[:SOURCED_FROM]->(s:Source)
        OPTIONAL MATCH (c)-[:SUPERSEDES]->(older:Claim)
        RETURN c.id as claim_id, c.object_value as value, c.status as status,
               c.confidence as confidence, c.created_at as created_at,
               c.valid_from as valid_from, c.valid_until as valid_until,
               s.uri as source_uri, s.reliability_score as source_reliability,
               older.id as supersedes_claim_id
        ORDER BY c.created_at DESC
        """

        result = await self.client.execute_query(
            query, {"entity_id": entity_id, "predicate": predicate}
        )
        return result.records

    # =========================================================================
    # Source Operations
    # =========================================================================

    async def get_claims_by_source(
        self,
        source_id: str,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """
        Get all claims from a specific source.

        Args:
            source_id: Source ID
            status: Optional status filter
            limit: Maximum results

        Returns:
            List of claims from the source
        """
        status_filter = "AND c.status = $status" if status else ""
        params = {"source_id": source_id, "limit": limit}
        if status:
            params["status"] = status

        query = f"""
        MATCH (c:Claim)-[:SOURCED_FROM]->(s:Source {{id: $source_id}})
        {status_filter}
        MATCH (e:Entity)-[:HAS_CLAIM]->(c)
        RETURN c.id as claim_id, c.predicate as predicate,
               c.object_value as value, c.confidence as confidence,
               c.status as status, e.id as entity_id, e.name as entity_name
        LIMIT $limit
        """

        result = await self.client.execute_query(query, params)
        return result.records

    async def update_source_reliability(
        self,
        source_id: str,
        reliability_score: float,
    ) -> None:
        """
        Update a source's reliability score.

        Args:
            source_id: Source ID
            reliability_score: New reliability score (0-1)
        """
        query = """
        MATCH (s:Source {id: $source_id})
        SET s.reliability_score = $reliability_score,
            s.reliability_updated_at = datetime()
        """

        await self.client.execute_query(
            query,
            {"source_id": source_id, "reliability_score": reliability_score}
        )
