"""
Document and Document Event Models
==================================

This module defines the data models for documents processed by DocWeave,
including the document structure, parsed elements, metadata, and events
emitted during the document processing pipeline.

Documents flow through the system:
1. Ingestion: Raw document received
2. Parsing: Document parsed into structured elements
3. Extraction: Claims extracted from parsed elements
4. Graph Update: Claims integrated into knowledge graph
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class EventType(str, Enum):
    """Simple event types for connectors."""
    CREATED = "created"
    MODIFIED = "modified"
    DELETED = "deleted"


class DocumentEventType(str, Enum):
    """
    Types of events that can occur during document processing.

    Attributes:
        INGESTED: Document has been received and stored
        PARSING_STARTED: Document parsing has begun
        PARSING_COMPLETED: Document parsing finished successfully
        PARSING_FAILED: Document parsing encountered an error
        EXTRACTION_STARTED: Claim extraction has begun
        EXTRACTION_COMPLETED: Claim extraction finished successfully
        EXTRACTION_FAILED: Claim extraction encountered an error
        GRAPH_UPDATE_STARTED: Graph update has begun
        GRAPH_UPDATE_COMPLETED: Graph update finished successfully
        GRAPH_UPDATE_FAILED: Graph update encountered an error
        DELETED: Document has been deleted
        REPROCESSING_REQUESTED: Document reprocessing has been requested
    """
    INGESTED = "ingested"
    PARSING_STARTED = "parsing_started"
    PARSING_COMPLETED = "parsing_completed"
    PARSING_FAILED = "parsing_failed"
    EXTRACTION_STARTED = "extraction_started"
    EXTRACTION_COMPLETED = "extraction_completed"
    EXTRACTION_FAILED = "extraction_failed"
    GRAPH_UPDATE_STARTED = "graph_update_started"
    GRAPH_UPDATE_COMPLETED = "graph_update_completed"
    GRAPH_UPDATE_FAILED = "graph_update_failed"
    DELETED = "deleted"
    REPROCESSING_REQUESTED = "reprocessing_requested"


class ElementType(str, Enum):
    """
    Types of structural elements parsed from documents.

    Attributes:
        TITLE: Document title
        HEADING: Section heading
        PARAGRAPH: Text paragraph
        LIST: Bulleted or numbered list
        LIST_ITEM: Individual list item
        TABLE: Data table
        TABLE_ROW: Table row
        TABLE_CELL: Table cell
        CODE_BLOCK: Code or preformatted text
        QUOTE: Block quote
        IMAGE: Image with optional caption
        LINK: Hyperlink
        FOOTNOTE: Footnote or endnote
        METADATA: Document metadata
        UNKNOWN: Unrecognized element type
    """
    TITLE = "title"
    HEADING = "heading"
    PARAGRAPH = "paragraph"
    LIST = "list"
    LIST_ITEM = "list_item"
    TABLE = "table"
    TABLE_ROW = "table_row"
    TABLE_CELL = "table_cell"
    CODE_BLOCK = "code_block"
    QUOTE = "quote"
    IMAGE = "image"
    LINK = "link"
    FOOTNOTE = "footnote"
    METADATA = "metadata"
    UNKNOWN = "unknown"


@dataclass
class ParsedElement:
    """
    Represents a structural element parsed from a document.

    Parsed elements provide structure to the raw document content,
    enabling more accurate claim extraction by understanding the
    document's organization.

    Attributes:
        id: Unique identifier for the element
        element_type: Type of structural element
        content: Text content of the element
        level: Nesting level (for headings, lists)
        position: Position in document (character offset)
        parent_id: ID of parent element for nested structures
        children_ids: IDs of child elements
        attributes: Element-specific attributes (e.g., heading level)
        confidence: Parsing confidence score
    """
    id: UUID
    element_type: ElementType
    content: str
    level: int = 0
    position: int = 0
    parent_id: Optional[UUID] = None
    children_ids: list[UUID] = field(default_factory=list)
    attributes: dict[str, Any] = field(default_factory=dict)
    confidence: float = 1.0

    def to_dict(self) -> dict[str, Any]:
        """Convert element to dictionary for serialization."""
        return {
            "id": str(self.id),
            "element_type": self.element_type.value,
            "content": self.content,
            "level": self.level,
            "position": self.position,
            "parent_id": str(self.parent_id) if self.parent_id else None,
            "children_ids": [str(uid) for uid in self.children_ids],
            "attributes": self.attributes,
            "confidence": self.confidence,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ParsedElement":
        """Create element from dictionary."""
        return cls(
            id=UUID(data["id"]) if isinstance(data["id"], str) else data["id"],
            element_type=ElementType(data["element_type"]),
            content=data["content"],
            level=data.get("level", 0),
            position=data.get("position", 0),
            parent_id=UUID(data["parent_id"]) if data.get("parent_id") else None,
            children_ids=[UUID(uid) if isinstance(uid, str) else uid for uid in data.get("children_ids", [])],
            attributes=data.get("attributes", {}),
            confidence=data.get("confidence", 1.0),
        )


@dataclass
class DocumentMetadata:
    """
    Metadata associated with a document.

    Attributes:
        title: Document title
        author: Document author
        created_date: Document creation date
        modified_date: Document last modified date
        page_count: Number of pages (if applicable)
        word_count: Word count
        language: Detected language code
        encoding: Character encoding
        custom: Custom metadata fields
    """
    title: Optional[str] = None
    author: Optional[str] = None
    created_date: Optional[datetime] = None
    modified_date: Optional[datetime] = None
    page_count: Optional[int] = None
    word_count: Optional[int] = None
    language: Optional[str] = None
    encoding: str = "utf-8"
    custom: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert metadata to dictionary for serialization."""
        return {
            "title": self.title,
            "author": self.author,
            "created_date": self.created_date.isoformat() if self.created_date else None,
            "modified_date": self.modified_date.isoformat() if self.modified_date else None,
            "page_count": self.page_count,
            "word_count": self.word_count,
            "language": self.language,
            "encoding": self.encoding,
            "custom": self.custom,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DocumentMetadata":
        """Create metadata from dictionary."""
        return cls(
            title=data.get("title"),
            author=data.get("author"),
            created_date=datetime.fromisoformat(data["created_date"]) if data.get("created_date") else None,
            modified_date=datetime.fromisoformat(data["modified_date"]) if data.get("modified_date") else None,
            page_count=data.get("page_count"),
            word_count=data.get("word_count"),
            language=data.get("language"),
            encoding=data.get("encoding", "utf-8"),
            custom=data.get("custom", {}),
        )


@dataclass
class Document:
    """
    Represents a document in the DocWeave processing pipeline.

    Documents are the primary input to the system. They contain raw content
    that is parsed into structured elements, from which claims are extracted
    and added to the knowledge graph.

    Attributes:
        id: Unique identifier for the document
        filename: Original filename
        content_type: MIME type of the document
        raw_text: Extracted plain text content
        raw_content: Binary content (for non-text formats)
        parsed_elements: Structured elements parsed from the document
        metadata: Document metadata
        source_connector: Connector that ingested this document
        source_uri: Original location of the document
        content_hash: Hash of the raw content for deduplication
        processing_status: Current processing state
        error_message: Error message if processing failed
        claims_extracted: Number of claims extracted
        created_at: Timestamp of document creation
        updated_at: Timestamp of last update
        processed_at: Timestamp of last processing completion
    """
    id: UUID
    filename: str
    content_type: str
    raw_text: Optional[str] = None
    raw_content: Optional[bytes] = None
    parsed_elements: list[ParsedElement] = field(default_factory=list)
    metadata: DocumentMetadata = field(default_factory=DocumentMetadata)
    source_connector: str = "filesystem"
    source_uri: Optional[str] = None
    content_hash: Optional[str] = None
    processing_status: str = "pending"
    error_message: Optional[str] = None
    claims_extracted: int = 0
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    processed_at: Optional[datetime] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert document to dictionary for serialization."""
        return {
            "id": str(self.id),
            "filename": self.filename,
            "content_type": self.content_type,
            "raw_text": self.raw_text,
            # Note: raw_content is excluded as it's binary
            "parsed_elements": [elem.to_dict() for elem in self.parsed_elements],
            "metadata": self.metadata.to_dict(),
            "source_connector": self.source_connector,
            "source_uri": self.source_uri,
            "content_hash": self.content_hash,
            "processing_status": self.processing_status,
            "error_message": self.error_message,
            "claims_extracted": self.claims_extracted,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "processed_at": self.processed_at.isoformat() if self.processed_at else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Document":
        """Create document from dictionary."""
        return cls(
            id=UUID(data["id"]) if isinstance(data["id"], str) else data["id"],
            filename=data["filename"],
            content_type=data["content_type"],
            raw_text=data.get("raw_text"),
            raw_content=data.get("raw_content"),
            parsed_elements=[ParsedElement.from_dict(elem) for elem in data.get("parsed_elements", [])],
            metadata=DocumentMetadata.from_dict(data["metadata"]) if data.get("metadata") else DocumentMetadata(),
            source_connector=data.get("source_connector", "filesystem"),
            source_uri=data.get("source_uri"),
            content_hash=data.get("content_hash"),
            processing_status=data.get("processing_status", "pending"),
            error_message=data.get("error_message"),
            claims_extracted=data.get("claims_extracted", 0),
            created_at=datetime.fromisoformat(data["created_at"]) if isinstance(data.get("created_at"), str) else data.get("created_at", datetime.utcnow()),
            updated_at=datetime.fromisoformat(data["updated_at"]) if isinstance(data.get("updated_at"), str) else data.get("updated_at", datetime.utcnow()),
            processed_at=datetime.fromisoformat(data["processed_at"]) if data.get("processed_at") else None,
        )


@dataclass
class DocumentEvent:
    """
    Represents an event in the document processing lifecycle.

    Document events are published to Kafka topics to enable
    asynchronous processing by downstream services.

    Attributes:
        id: Unique identifier for the event
        event_type: Type of event
        document_id: ID of the document this event is about
        timestamp: When the event occurred
        source_connector: Connector that triggered the event
        payload: Event-specific data
        correlation_id: ID for tracing related events
        error_details: Error information if event represents a failure
    """
    id: UUID
    event_type: DocumentEventType
    document_id: UUID
    timestamp: datetime = field(default_factory=datetime.utcnow)
    source_connector: Optional[str] = None
    payload: dict[str, Any] = field(default_factory=dict)
    correlation_id: Optional[UUID] = None
    error_details: Optional[dict[str, Any]] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert event to dictionary for serialization."""
        return {
            "id": str(self.id),
            "event_type": self.event_type.value,
            "document_id": str(self.document_id),
            "timestamp": self.timestamp.isoformat(),
            "source_connector": self.source_connector,
            "payload": self.payload,
            "correlation_id": str(self.correlation_id) if self.correlation_id else None,
            "error_details": self.error_details,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DocumentEvent":
        """Create event from dictionary."""
        return cls(
            id=UUID(data["id"]) if isinstance(data["id"], str) else data["id"],
            event_type=DocumentEventType(data["event_type"]),
            document_id=UUID(data["document_id"]) if isinstance(data["document_id"], str) else data["document_id"],
            timestamp=datetime.fromisoformat(data["timestamp"]) if isinstance(data.get("timestamp"), str) else data.get("timestamp", datetime.utcnow()),
            source_connector=data.get("source_connector"),
            payload=data.get("payload", {}),
            correlation_id=UUID(data["correlation_id"]) if data.get("correlation_id") else None,
            error_details=data.get("error_details"),
        )

    def is_success(self) -> bool:
        """Check if this is a success event."""
        return self.event_type in {
            DocumentEventType.INGESTED,
            DocumentEventType.PARSING_COMPLETED,
            DocumentEventType.EXTRACTION_COMPLETED,
            DocumentEventType.GRAPH_UPDATE_COMPLETED,
        }

    def is_failure(self) -> bool:
        """Check if this is a failure event."""
        return self.event_type in {
            DocumentEventType.PARSING_FAILED,
            DocumentEventType.EXTRACTION_FAILED,
            DocumentEventType.GRAPH_UPDATE_FAILED,
        }


# Pydantic models for API request/response validation

class DocumentEventModel(BaseModel):
    """Pydantic model for document events (used by connectors and services)."""
    event_type: EventType
    document_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    source_connector: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    def model_dump_json(self, **kwargs) -> str:
        """Serialize to JSON string."""
        import json
        data = self.model_dump()
        data["timestamp"] = data["timestamp"].isoformat()
        data["event_type"] = data["event_type"].value
        return json.dumps(data)


class DocumentCreate(BaseModel):
    """Request model for creating a new document."""
    filename: str = Field(..., min_length=1, max_length=500)
    content_type: str = Field(..., min_length=1, max_length=100)
    raw_text: Optional[str] = None
    source_connector: str = Field("filesystem", max_length=100)
    source_uri: Optional[str] = Field(None, max_length=2000)
    metadata: dict[str, Any] = Field(default_factory=dict)


class DocumentUpdate(BaseModel):
    """Request model for updating a document."""
    filename: Optional[str] = Field(None, min_length=1, max_length=500)
    processing_status: Optional[str] = Field(None, max_length=50)
    error_message: Optional[str] = Field(None, max_length=5000)
    metadata: Optional[dict[str, Any]] = None
