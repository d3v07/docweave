"""
Claim, Entity, and Source Models
================================

This module defines the core data models for representing knowledge claims,
entities, and their sources within the DocWeave knowledge graph.

A Claim represents a factual assertion about an entity, extracted from a source
document. Claims can be in various states (CURRENT, SUPERSEDED, CONFLICTING)
and form the backbone of the knowledge graph.

An Entity represents a real-world object such as a person, company, product,
or concept that claims can reference.

A Source represents the origin document from which claims are extracted,
including metadata about reliability and provenance.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator


class ClaimStatus(str, Enum):
    """
    Lifecycle states for claims in the knowledge graph.

    Attributes:
        CURRENT: The claim is the most recent valid assertion
        SUPERSEDED: The claim has been replaced by a newer claim
        CONFLICTING: The claim conflicts with another claim and requires resolution
        PENDING: The claim is awaiting validation or processing
        REJECTED: The claim has been rejected due to low confidence or errors
    """
    CURRENT = "current"
    SUPERSEDED = "superseded"
    CONFLICTING = "conflicting"
    PENDING = "pending"
    REJECTED = "rejected"


class EntityType(str, Enum):
    """
    Categories of entities that can be referenced in claims.

    Attributes:
        PERSON: A human individual
        ORGANIZATION: A company, institution, or group
        PRODUCT: A product, service, or offering
        LOCATION: A geographical location or address
        EVENT: A dated occurrence or happening
        CONCEPT: An abstract idea or category
        TECHNOLOGY: A technology, framework, or tool
        DOCUMENT: A reference to another document
        OTHER: Uncategorized entity type
    """
    PERSON = "person"
    ORGANIZATION = "organization"
    PRODUCT = "product"
    LOCATION = "location"
    EVENT = "event"
    CONCEPT = "concept"
    TECHNOLOGY = "technology"
    DOCUMENT = "document"
    OTHER = "other"


class SourceType(str, Enum):
    """
    Categories of information sources.

    Attributes:
        DOCUMENT: A document file (PDF, DOCX, etc.)
        WEBPAGE: A web page or online resource
        API: Data from an external API
        DATABASE: Data from a database
        USER_INPUT: Information provided directly by a user
        SYSTEM: System-generated information
    """
    DOCUMENT = "document"
    WEBPAGE = "webpage"
    API = "api"
    DATABASE = "database"
    USER_INPUT = "user_input"
    SYSTEM = "system"


@dataclass
class Entity:
    """
    Represents a real-world entity in the knowledge graph.

    Entities are the nodes in the knowledge graph that claims reference.
    They can represent people, organizations, products, locations, and
    other real-world objects or concepts.

    Attributes:
        id: Unique identifier for the entity
        name: Primary name of the entity
        entity_type: Category of the entity
        aliases: Alternative names or spellings
        description: Optional description of the entity
        properties: Additional key-value properties
        created_at: Timestamp of entity creation
        updated_at: Timestamp of last update
        merged_from: IDs of entities merged into this one
    """
    id: UUID
    name: str
    entity_type: EntityType
    aliases: list[str] = field(default_factory=list)
    description: Optional[str] = None
    properties: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    merged_from: list[UUID] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert entity to dictionary for serialization."""
        return {
            "id": str(self.id),
            "name": self.name,
            "entity_type": self.entity_type.value,
            "aliases": self.aliases,
            "description": self.description,
            "properties": self.properties,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "merged_from": [str(uid) for uid in self.merged_from],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Entity":
        """Create entity from dictionary."""
        return cls(
            id=UUID(data["id"]) if isinstance(data["id"], str) else data["id"],
            name=data["name"],
            entity_type=EntityType(data["entity_type"]),
            aliases=data.get("aliases", []),
            description=data.get("description"),
            properties=data.get("properties", {}),
            created_at=datetime.fromisoformat(data["created_at"]) if isinstance(data.get("created_at"), str) else data.get("created_at", datetime.utcnow()),
            updated_at=datetime.fromisoformat(data["updated_at"]) if isinstance(data.get("updated_at"), str) else data.get("updated_at", datetime.utcnow()),
            merged_from=[UUID(uid) if isinstance(uid, str) else uid for uid in data.get("merged_from", [])],
        )


@dataclass
class Source:
    """
    Represents the origin of information in the knowledge graph.

    Sources track where claims come from, including provenance metadata
    and reliability scoring. This enables traceability and helps with
    conflict resolution.

    Attributes:
        id: Unique identifier for the source
        source_type: Category of the source
        uri: Location or identifier for the source (file path, URL, etc.)
        title: Human-readable title of the source
        date: Date of the source (publication date, access date, etc.)
        reliability_score: Score from 0.0 to 1.0 indicating source reliability
        access_level: Access control level (public, internal, confidential)
        raw_content_hash: Hash of the raw content for deduplication
        metadata: Additional source-specific metadata
        created_at: Timestamp of source creation
        updated_at: Timestamp of last update
    """
    id: UUID
    source_type: SourceType
    uri: str
    title: Optional[str] = None
    date: Optional[datetime] = None
    reliability_score: float = 0.5
    access_level: str = "internal"
    raw_content_hash: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    def __post_init__(self):
        """Validate reliability score is within bounds."""
        if not 0.0 <= self.reliability_score <= 1.0:
            raise ValueError("reliability_score must be between 0.0 and 1.0")

    def to_dict(self) -> dict[str, Any]:
        """Convert source to dictionary for serialization."""
        return {
            "id": str(self.id),
            "source_type": self.source_type.value,
            "uri": self.uri,
            "title": self.title,
            "date": self.date.isoformat() if self.date else None,
            "reliability_score": self.reliability_score,
            "access_level": self.access_level,
            "raw_content_hash": self.raw_content_hash,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Source":
        """Create source from dictionary."""
        return cls(
            id=UUID(data["id"]) if isinstance(data["id"], str) else data["id"],
            source_type=SourceType(data["source_type"]),
            uri=data["uri"],
            title=data.get("title"),
            date=datetime.fromisoformat(data["date"]) if data.get("date") else None,
            reliability_score=data.get("reliability_score", 0.5),
            access_level=data.get("access_level", "internal"),
            raw_content_hash=data.get("raw_content_hash"),
            metadata=data.get("metadata", {}),
            created_at=datetime.fromisoformat(data["created_at"]) if isinstance(data.get("created_at"), str) else data.get("created_at", datetime.utcnow()),
            updated_at=datetime.fromisoformat(data["updated_at"]) if isinstance(data.get("updated_at"), str) else data.get("updated_at", datetime.utcnow()),
        )


@dataclass
class Claim:
    """
    Represents a factual assertion extracted from a source document.

    Claims are the edges in the knowledge graph, connecting entities
    through predicates. They include provenance information, confidence
    scores, and lifecycle state tracking.

    A claim follows the structure: subject_entity -> predicate -> object_value
    For example: "Apple Inc." -> "founded_by" -> "Steve Jobs"

    Attributes:
        id: Unique identifier for the claim
        subject_entity_id: ID of the entity this claim is about
        predicate: The relationship or property type
        object_value: The value or target of the claim
        object_entity_id: Optional ID if object is an entity reference
        source_id: ID of the source this claim was extracted from
        confidence: Confidence score from 0.0 to 1.0
        extracted_text: Original text from which the claim was extracted
        timestamp: When the claim was extracted
        status: Current lifecycle state of the claim
        superseded_by: ID of claim that superseded this one
        conflicting_claims: IDs of claims this one conflicts with
        metadata: Additional claim-specific metadata
    """
    id: UUID
    subject_entity_id: UUID
    predicate: str
    object_value: Any
    source_id: UUID
    confidence: float = 0.8
    object_entity_id: Optional[UUID] = None
    extracted_text: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    status: ClaimStatus = ClaimStatus.CURRENT
    superseded_by: Optional[UUID] = None
    conflicting_claims: list[UUID] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate confidence score is within bounds."""
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError("confidence must be between 0.0 and 1.0")

    def to_dict(self) -> dict[str, Any]:
        """Convert claim to dictionary for serialization."""
        return {
            "id": str(self.id),
            "subject_entity_id": str(self.subject_entity_id),
            "predicate": self.predicate,
            "object_value": self.object_value,
            "object_entity_id": str(self.object_entity_id) if self.object_entity_id else None,
            "source_id": str(self.source_id),
            "confidence": self.confidence,
            "extracted_text": self.extracted_text,
            "timestamp": self.timestamp.isoformat(),
            "status": self.status.value,
            "superseded_by": str(self.superseded_by) if self.superseded_by else None,
            "conflicting_claims": [str(uid) for uid in self.conflicting_claims],
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Claim":
        """Create claim from dictionary."""
        return cls(
            id=UUID(data["id"]) if isinstance(data["id"], str) else data["id"],
            subject_entity_id=UUID(data["subject_entity_id"]) if isinstance(data["subject_entity_id"], str) else data["subject_entity_id"],
            predicate=data["predicate"],
            object_value=data["object_value"],
            object_entity_id=UUID(data["object_entity_id"]) if data.get("object_entity_id") else None,
            source_id=UUID(data["source_id"]) if isinstance(data["source_id"], str) else data["source_id"],
            confidence=data.get("confidence", 0.8),
            extracted_text=data.get("extracted_text"),
            timestamp=datetime.fromisoformat(data["timestamp"]) if isinstance(data.get("timestamp"), str) else data.get("timestamp", datetime.utcnow()),
            status=ClaimStatus(data.get("status", "current")),
            superseded_by=UUID(data["superseded_by"]) if data.get("superseded_by") else None,
            conflicting_claims=[UUID(uid) if isinstance(uid, str) else uid for uid in data.get("conflicting_claims", [])],
            metadata=data.get("metadata", {}),
        )

    def is_active(self) -> bool:
        """Check if the claim is in an active state."""
        return self.status == ClaimStatus.CURRENT

    def mark_superseded(self, new_claim_id: UUID) -> None:
        """Mark this claim as superseded by another claim."""
        self.status = ClaimStatus.SUPERSEDED
        self.superseded_by = new_claim_id

    def add_conflict(self, conflicting_claim_id: UUID) -> None:
        """Add a conflicting claim reference."""
        if conflicting_claim_id not in self.conflicting_claims:
            self.conflicting_claims.append(conflicting_claim_id)
            self.status = ClaimStatus.CONFLICTING


# Pydantic models for API request/response validation

class EntityCreate(BaseModel):
    """Request model for creating a new entity."""
    name: str = Field(..., min_length=1, max_length=500)
    entity_type: EntityType
    aliases: list[str] = Field(default_factory=list)
    description: Optional[str] = Field(None, max_length=5000)
    properties: dict[str, Any] = Field(default_factory=dict)

    @field_validator("aliases")
    @classmethod
    def validate_aliases(cls, v: list[str]) -> list[str]:
        return [alias.strip() for alias in v if alias.strip()]


class EntityUpdate(BaseModel):
    """Request model for updating an entity."""
    name: Optional[str] = Field(None, min_length=1, max_length=500)
    entity_type: Optional[EntityType] = None
    aliases: Optional[list[str]] = None
    description: Optional[str] = Field(None, max_length=5000)
    properties: Optional[dict[str, Any]] = None


class SourceCreate(BaseModel):
    """Request model for creating a new source."""
    source_type: SourceType
    uri: str = Field(..., min_length=1, max_length=2000)
    title: Optional[str] = Field(None, max_length=500)
    date: Optional[datetime] = None
    reliability_score: float = Field(0.5, ge=0.0, le=1.0)
    access_level: str = Field("internal", max_length=50)
    raw_content_hash: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class SourceUpdate(BaseModel):
    """Request model for updating a source."""
    title: Optional[str] = Field(None, max_length=500)
    date: Optional[datetime] = None
    reliability_score: Optional[float] = Field(None, ge=0.0, le=1.0)
    access_level: Optional[str] = Field(None, max_length=50)
    metadata: Optional[dict[str, Any]] = None


class ClaimCreate(BaseModel):
    """Request model for creating a new claim."""
    subject_entity_id: UUID
    predicate: str = Field(..., min_length=1, max_length=200)
    object_value: Any
    object_entity_id: Optional[UUID] = None
    source_id: UUID
    confidence: float = Field(0.8, ge=0.0, le=1.0)
    extracted_text: Optional[str] = Field(None, max_length=10000)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ClaimUpdate(BaseModel):
    """Request model for updating a claim."""
    predicate: Optional[str] = Field(None, min_length=1, max_length=200)
    object_value: Optional[Any] = None
    confidence: Optional[float] = Field(None, ge=0.0, le=1.0)
    status: Optional[ClaimStatus] = None
    metadata: Optional[dict[str, Any]] = None
