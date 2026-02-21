"""Document processing pipeline for ingestion service."""
import asyncio
from datetime import datetime
from typing import Optional, Callable, Awaitable
from dataclasses import dataclass

from shared.config.settings import settings
from shared.utils.kafka_client import KafkaProducer
from shared.models.document import DocumentEventModel, EventType
from services.ingestion.storage import DocumentStorage, StoredDocument


@dataclass
class ProcessingResult:
    """Result of document processing."""
    document_id: str
    status: str  # accepted, duplicate, rejected, error
    message: str
    is_new: bool = True


class DocumentProcessor:
    """
    Orchestrates document ingestion pipeline.

    Handles:
    - Document validation
    - Storage with deduplication
    - Event publishing to Kafka
    - Processing status tracking
    """

    ALLOWED_CONTENT_TYPES = {
        "application/pdf",
        "text/plain",
        "text/markdown",
        "text/html",
        "application/json",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "message/rfc822",  # email
    }

    MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB

    def __init__(
        self,
        storage: DocumentStorage,
        kafka_producer: Optional[KafkaProducer] = None
    ):
        self.storage = storage
        self.kafka_producer = kafka_producer
        self._processing_callbacks: list[Callable[[StoredDocument], Awaitable[None]]] = []

    def add_callback(self, callback: Callable[[StoredDocument], Awaitable[None]]) -> None:
        """Add a callback to be invoked after successful processing."""
        self._processing_callbacks.append(callback)

    def validate_content_type(self, content_type: str) -> bool:
        """Check if content type is supported."""
        # Normalize content type (remove parameters like charset)
        base_type = content_type.split(";")[0].strip().lower()
        return base_type in self.ALLOWED_CONTENT_TYPES

    def validate_size(self, size: int) -> bool:
        """Check if file size is within limits."""
        return size <= self.MAX_FILE_SIZE

    def detect_content_type(self, filename: str, content: bytes) -> str:
        """Detect content type from filename and content."""
        ext_mapping = {
            ".pdf": "application/pdf",
            ".txt": "text/plain",
            ".md": "text/markdown",
            ".html": "text/html",
            ".htm": "text/html",
            ".json": "application/json",
            ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            ".eml": "message/rfc822",
        }

        ext = "." + filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

        if ext in ext_mapping:
            return ext_mapping[ext]

        # Magic number detection for common formats
        if content[:4] == b"%PDF":
            return "application/pdf"
        if content[:2] in (b"PK", b"\x50\x4b"):  # ZIP-based formats like DOCX
            return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"

        return "application/octet-stream"

    async def process(
        self,
        content: bytes,
        filename: str,
        content_type: Optional[str] = None,
        source_connector: str = "api",
        metadata: Optional[dict] = None
    ) -> ProcessingResult:
        """
        Process and store a document.

        Args:
            content: Raw document content
            filename: Original filename
            content_type: MIME type (auto-detected if not provided)
            source_connector: Source identifier
            metadata: Additional metadata

        Returns:
            ProcessingResult with status and document ID
        """
        # Auto-detect content type if not provided
        if not content_type or content_type == "application/octet-stream":
            content_type = self.detect_content_type(filename, content)

        # Validate
        if not self.validate_size(len(content)):
            return ProcessingResult(
                document_id="",
                status="rejected",
                message=f"File too large. Maximum size is {self.MAX_FILE_SIZE // (1024*1024)}MB",
                is_new=False
            )

        if not self.validate_content_type(content_type):
            return ProcessingResult(
                document_id="",
                status="rejected",
                message=f"Unsupported content type: {content_type}",
                is_new=False
            )

        # Store with deduplication
        try:
            stored_doc, is_new = self.storage.store(
                content=content,
                filename=filename,
                content_type=content_type,
                source_connector=source_connector,
                metadata=metadata
            )
        except Exception as e:
            return ProcessingResult(
                document_id="",
                status="error",
                message=f"Storage error: {str(e)}",
                is_new=False
            )

        # Publish event to Kafka
        if is_new and self.kafka_producer:
            await self._publish_event(stored_doc)

        # Run callbacks
        if is_new:
            for callback in self._processing_callbacks:
                try:
                    await callback(stored_doc)
                except Exception:
                    pass  # Don't fail processing on callback errors

        return ProcessingResult(
            document_id=stored_doc.id,
            status="accepted" if is_new else "duplicate",
            message="Document queued for processing" if is_new else "Duplicate document detected",
            is_new=is_new
        )

    async def _publish_event(self, doc: StoredDocument) -> None:
        """Publish document event to Kafka."""
        event = DocumentEventModel(
            event_type=EventType.CREATED,
            document_id=doc.id,
            timestamp=datetime.utcnow(),
            source_connector=doc.source_connector,
            metadata={
                "filename": doc.filename,
                "content_type": doc.content_type,
                "size_bytes": doc.size_bytes,
                "content_hash": doc.content_hash
            }
        )

        await self.kafka_producer.send(
            topic=settings.KAFKA_TOPIC_DOCUMENT_EVENTS,
            value=event.model_dump_json(),
            key=doc.id
        )

    async def reprocess(self, doc_id: str) -> ProcessingResult:
        """Requeue an existing document for processing."""
        result = self.storage.get(doc_id)
        if not result:
            return ProcessingResult(
                document_id=doc_id,
                status="error",
                message="Document not found",
                is_new=False
            )

        stored_doc, _ = result

        if self.kafka_producer:
            await self._publish_event(stored_doc)

        return ProcessingResult(
            document_id=doc_id,
            status="accepted",
            message="Document requeued for processing",
            is_new=False
        )
