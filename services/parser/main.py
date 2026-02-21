"""Parser service - extracts structured content from raw documents."""
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
    global kafka_producer, kafka_consumer
    kafka_producer = KafkaProducer()
    kafka_consumer = KafkaConsumer(
        topics=[settings.KAFKA_TOPIC_DOCUMENT_EVENTS],
        group_id="parser-service"
    )
    await kafka_producer.start()
    await kafka_consumer.start()
    yield
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
    import time
    import base64
    start = time.time()

    # Get appropriate parser
    parser = get_parser(request.content_type)
    if not parser:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported content type: {request.content_type}"
        )

    # Decode content
    try:
        # Try base64 first
        try:
            content = base64.b64decode(request.content)
        except Exception:
            # Fall back to UTF-8 encoding
            content = request.content.encode(request.encoding)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Content decode error: {e}")

    # Parse document
    try:
        result: ParsedContent = parser.parse(content, request.filename)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Parse error: {e}")

    parse_time = (time.time() - start) * 1000

    # Publish to Kafka
    if kafka_producer and not result.parse_errors:
        import json
        event = {
            "document_id": request.document_id,
            "status": "parsed",
            "block_count": len(result.blocks),
            "word_count": result.word_count,
            "timestamp": datetime.utcnow().isoformat()
        }
        await kafka_producer.send(
            topic=settings.KAFKA_TOPIC_PARSED_DOCUMENTS,
            value=json.dumps(event),
            key=request.document_id
        )

    return ParseResponse(
        document_id=request.document_id,
        blocks=[block_to_response(b) for b in result.blocks],
        metadata=result.metadata,
        raw_text=result.raw_text,
        word_count=result.word_count,
        parse_time_ms=parse_time,
        errors=result.parse_errors
    )


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
