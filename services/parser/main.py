"""Parser service - extracts structured content from raw documents."""
import asyncio
import base64
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from shared.config.settings import settings
from shared.utils.kafka_client import KafkaProducer, KafkaConsumer
from services.parser.parsers import (
    BaseParser, ParsedContent, ContentBlock, BlockType,
    TextParser, JSONParser, HTMLParser, MarkdownParser
)
from services.ingestion.storage import DocumentStorage


logger = logging.getLogger(__name__)


class HealthResponse(BaseModel):
    status: str
    service: str
    timestamp: str


class ParsedBlockResponse(BaseModel):
    type: str
    content: str
    level: int = 0
    metadata: Dict[str, Any] = {}
    children: List["ParsedBlockResponse"] = []

ParsedBlockResponse.model_rebuild()


class ParseResponse(BaseModel):
    document_id: str
    blocks: List[ParsedBlockResponse]
    metadata: Dict[str, Any]
    raw_text: str
    word_count: int
    parse_time_ms: float
    errors: List[str] = []


class ParseRequest(BaseModel):
    document_id: str
    content: str  # Base64 encoded or plain text
    content_type: str = "text/plain"
    filename: str = ""
    encoding: str = "utf-8"


kafka_producer: Optional[KafkaProducer] = None
kafka_consumer: Optional[KafkaConsumer] = None
consumer_task: Optional[asyncio.Task] = None
storage: Optional[DocumentStorage] = None

# Initialize parsers
PARSERS: List[BaseParser] = [
    TextParser(),
    JSONParser(),
    HTMLParser(),
    MarkdownParser(),
]


def get_parser(content_type: str) -> Optional[BaseParser]:
    """Get appropriate parser for content type."""
    normalized = content_type.split(";")[0].strip().lower()
    for parser in PARSERS:
        if parser.can_parse(normalized):
            return parser
    return None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global kafka_producer, kafka_consumer, consumer_task, storage
    storage_path = os.environ.get("STORAGE_PATH", "/app/data/documents")
    storage = DocumentStorage(storage_path)
    kafka_producer = KafkaProducer()
    kafka_consumer = KafkaConsumer(
        topics=[settings.KAFKA_TOPIC_DOCUMENT_EVENTS],
        group_id="parser-service"
    )
    await kafka_producer.start()
    await kafka_consumer.start()
    consumer_task = asyncio.create_task(consume_document_events())
    try:
        yield
    finally:
        if consumer_task:
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass
        await kafka_producer.stop()
        await kafka_consumer.stop()


app = FastAPI(
    title="DocWeave Parser Service",
    description="Extracts structured content from documents (text, JSON, HTML, Markdown)",
    version="0.2.0",
    lifespan=lifespan
)


@app.get("/health", response_model=HealthResponse)
async def health_check():
    return HealthResponse(
        status="healthy",
        service="parser",
        timestamp=datetime.utcnow().isoformat()
    )


@app.get("/ready")
async def readiness_check():
    return {
        "ready": True,
        "checks": {"kafka": kafka_producer is not None},
        "supported_formats": [ct for p in PARSERS for ct in p.supported_content_types],
        "timestamp": datetime.utcnow().isoformat()
    }


def block_to_response(block: ContentBlock) -> ParsedBlockResponse:
    """Convert ContentBlock to response model."""
    return ParsedBlockResponse(
        type=block.type.value,
        content=block.content,
        level=block.level,
        metadata=block.metadata,
        children=[block_to_response(c) for c in block.children]
    )


def decode_content(request: ParseRequest) -> bytes:
    """Decode request content from base64 or plain text."""
    try:
        try:
            return base64.b64decode(request.content, validate=True)
        except Exception:
            return request.content.encode(request.encoding)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Content decode error: {e}") from e


async def parse_bytes(
    document_id: str,
    content: bytes,
    content_type: str,
    filename: str = "",
    source_id: Optional[str] = None,
    publish: bool = True,
) -> ParseResponse:
    """Parse bytes and optionally publish the parsed document event."""
    import time
    start = time.time()

    parser = get_parser(content_type)
    if not parser:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported content type: {content_type}"
        )

    try:
        result: ParsedContent = parser.parse(content, filename)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Parse error: {e}") from e

    parse_time = (time.time() - start) * 1000

    if publish and kafka_producer and not result.parse_errors:
        event = {
            "document_id": document_id,
            "source_id": source_id or document_id,
            "status": "parsed",
            "content_type": content_type,
            "filename": filename,
            "block_count": len(result.blocks),
            "word_count": result.word_count,
            "raw_text": result.raw_text,
            "metadata": result.metadata,
            "timestamp": datetime.utcnow().isoformat()
        }
        await kafka_producer.send(
            topic=settings.KAFKA_TOPIC_PARSED_DOCUMENTS,
            value=event,
            key=document_id
        )

    return ParseResponse(
        document_id=document_id,
        blocks=[block_to_response(b) for b in result.blocks],
        metadata=result.metadata,
        raw_text=result.raw_text,
        word_count=result.word_count,
        parse_time_ms=parse_time,
        errors=result.parse_errors
    )


@app.post("/parse", response_model=ParseResponse)
async def parse_document(request: ParseRequest):
    """
    Parse document content into structured blocks.

    Supports:
    - text/plain: Plain text with paragraph detection
    - application/json: JSON with key-value extraction
    - text/html: HTML with tag-based parsing
    - text/markdown: Markdown with syntax parsing
    """
    return await parse_bytes(
        document_id=request.document_id,
        content=decode_content(request),
        content_type=request.content_type,
        filename=request.filename,
        source_id=request.document_id,
    )


async def consume_document_events() -> None:
    """Consume ingested document events and publish parsed document events."""
    if not kafka_consumer or not storage:
        return

    async for message in kafka_consumer.consume():
        try:
            event = message.value
            if isinstance(event, str):
                event = json.loads(event)

            document_id = event.get("document_id")
            if not document_id:
                logger.warning("Skipping document event without document_id")
                continue

            stored = storage.get(document_id)
            if not stored:
                logger.warning("Stored document not found: %s", document_id)
                continue

            doc, content = stored
            await parse_bytes(
                document_id=doc.id,
                content=content,
                content_type=doc.content_type,
                filename=doc.filename,
                source_id=doc.id,
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception("Failed to parse document event: %s", e)


@app.get("/formats")
async def list_supported_formats():
    """List all supported document formats."""
    formats = []
    for parser in PARSERS:
        formats.append({
            "parser": parser.__class__.__name__,
            "content_types": parser.supported_content_types
        })
    return {"formats": formats}


@app.post("/parse/text", response_model=ParseResponse)
async def parse_text_simple(document_id: str, content: str):
    """Simple endpoint to parse plain text."""
    import time
    start = time.time()

    parser = TextParser()
    result = parser.parse(content.encode("utf-8"), "")

    return ParseResponse(
        document_id=document_id,
        blocks=[block_to_response(b) for b in result.blocks],
        metadata=result.metadata,
        raw_text=result.raw_text,
        word_count=result.word_count,
        parse_time_ms=(time.time() - start) * 1000
    )
