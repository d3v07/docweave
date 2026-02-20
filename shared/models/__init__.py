"""
DocWeave Data Models
====================

This module exports all core data models used throughout the DocWeave platform.

Models:
    - Claim: Represents a factual assertion extracted from documents
    - ClaimStatus: Enum for claim lifecycle states
    - Entity: Represents a real-world entity (person, company, product, etc.)
    - EntityType: Enum for categorizing entities
    - Source: Represents the origin of information
    - SourceType: Enum for source categories
    - Document: Represents an ingested document
    - DocumentEvent: Event emitted during document processing
    - DocumentEventType: Enum for document event types
    - Graph relationship models for Neo4j
"""

from shared.models.claim import (
    Claim,
    ClaimStatus,
    ClaimCreate,
    ClaimUpdate,
    Entity,
    EntityType,
    EntityCreate,
    EntityUpdate,
    Source,
    SourceType,
    SourceCreate,
    SourceUpdate,
)

from shared.models.document import (
    Document,
    DocumentCreate,
    DocumentUpdate,
    DocumentEvent,
    DocumentEventType,
    ParsedElement,
    ElementType,
    DocumentMetadata,
)

from shared.models.graph import (
    GraphNode,
    GraphRelationship,
    RelationshipType,
    GraphPath,
    GraphQueryResult,
    ConflictResolution,
    ConflictStatus,
)

__all__ = [
    # Claim models
    "Claim",
    "ClaimStatus",
    "ClaimCreate",
    "ClaimUpdate",
    "Entity",
    "EntityType",
    "EntityCreate",
    "EntityUpdate",
    "Source",
    "SourceType",
    "SourceCreate",
    "SourceUpdate",
    # Document models
    "Document",
    "DocumentCreate",
    "DocumentUpdate",
    "DocumentEvent",
    "DocumentEventType",
    "ParsedElement",
    "ElementType",
    "DocumentMetadata",
    # Graph models
    "GraphNode",
    "GraphRelationship",
    "RelationshipType",
    "GraphPath",
    "GraphQueryResult",
    "ConflictResolution",
    "ConflictStatus",
]
