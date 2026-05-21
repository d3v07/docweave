"""Extractor service - NLP extraction pipeline for entities, relations, and claims."""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from shared.config.settings import settings
from shared.utils.kafka_client import KafkaConsumer, KafkaProducer

logger = logging.getLogger(__name__)


class HealthResponse(BaseModel):
    status: str
    service: str
    timestamp: str


class EntityResponse(BaseModel):
    text: str
    normalized_text: str
    entity_type: str
    confidence: float
    aliases: List[str] = []


class RelationResponse(BaseModel):
    subject: str
    predicate: str
    object_value: str
    confidence: float
    source_text: str


class ClaimResponse(BaseModel):
    id: str
    subject_entity_id: str
    subject_name: str
    predicate: str
    object_value: str
    source_id: str
    confidence: float
    source_text: str


class EmbeddingResponse(BaseModel):
    text: str
    embedding: List[float]
    dimension: int


class ExtractionRequest(BaseModel):
    document_id: str
    text: str
    source_id: str = "unknown"


class ExtractionResponse(BaseModel):
    document_id: str
    entities: List[EntityResponse]
    relations: List[RelationResponse]
    claims: List[ClaimResponse]
    extraction_time_ms: float


class EmbedRequest(BaseModel):
    texts: List[str] = Field(..., max_length=100)


# Global components
kafka_producer: Optional[KafkaProducer] = None
kafka_consumer: Optional[KafkaConsumer] = None
consumer_task: Optional[asyncio.Task] = None
entity_extractor = None
relation_extractor = None
claim_generator = None
embedder = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize extraction components."""
    global kafka_producer, kafka_consumer, consumer_task
    global entity_extractor, relation_extractor, claim_generator, embedder

    # Import here to avoid loading models at module import
    from services.extractor.embedding import Embedder
    from services.extractor.extraction import (
        ClaimGenerator,
        EntityExtractor,
        RelationExtractor,
    )

    # Initialize extractors
    entity_extractor = EntityExtractor()
    relation_extractor = RelationExtractor()
    claim_generator = ClaimGenerator()

    # Initialize embedder (lazy loaded)
    embedder = Embedder()

    # Initialize Kafka
    kafka_producer = KafkaProducer()
    kafka_consumer = KafkaConsumer(
        topics=[settings.KAFKA_TOPIC_PARSED_DOCUMENTS], group_id="extractor-service"
    )
    await kafka_producer.start()
    await kafka_consumer.start()
    consumer_task = asyncio.create_task(consume_parsed_documents())

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
    title="DocWeave Extractor Service",
    description="NLP extraction pipeline for entities, relations, and claims",
    version="0.2.0",
    lifespan=lifespan,
)


@app.get("/health", response_model=HealthResponse)
async def health_check():
    return HealthResponse(
        status="healthy", service="extractor", timestamp=datetime.utcnow().isoformat()
    )


@app.get("/ready")
async def readiness_check():
    return {
        "ready": entity_extractor is not None,
        "checks": {
            "entity_extractor": entity_extractor is not None,
            "relation_extractor": relation_extractor is not None,
            "claim_generator": claim_generator is not None,
            "kafka": kafka_producer is not None,
        },
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.post("/extract", response_model=ExtractionResponse)
async def extract_all(request: ExtractionRequest):
    """
    Full extraction pipeline: entities -> relations -> claims.

    Extracts named entities, relations between them, and generates
    structured claims ready for the knowledge graph.
    """
    return await run_extraction(request)


async def run_extraction(
    request: ExtractionRequest,
    publish: bool = True,
) -> ExtractionResponse:
    """Run extraction and optionally publish standard claim events."""
    import time

    start = time.time()

    if not entity_extractor or not relation_extractor or not claim_generator:
        raise HTTPException(status_code=503, detail="Extractors not initialized")

    entities = entity_extractor.extract(request.text)
    relations = relation_extractor.extract(request.text, entities)
    claims = claim_generator.generate(relations, request.source_id)

    extraction_time = (time.time() - start) * 1000

    if publish and kafka_producer and claims:
        for claim in claims:
            event = {
                "id": claim.id,
                "document_id": request.document_id,
                "subject_entity_id": claim.subject_entity_id,
                "subject_name": claim.subject_name,
                "predicate": claim.predicate,
                "object_value": claim.object_value,
                "source_id": request.source_id,
                "confidence": claim.confidence,
                "object_entity_id": claim.object_entity_id,
                "extracted_text": claim.extracted_text,
                "valid_from": (
                    claim.valid_from.isoformat() if claim.valid_from else None
                ),
                "valid_until": (
                    claim.valid_until.isoformat() if claim.valid_until else None
                ),
                "timestamp": datetime.utcnow().isoformat(),
            }
            await kafka_producer.send(
                topic=settings.KAFKA_TOPIC_EXTRACTED_CLAIMS,
                value=event,
                key=request.document_id,
            )

    return ExtractionResponse(
        document_id=request.document_id,
        entities=[
            EntityResponse(
                text=e.text,
                normalized_text=e.normalized_text,
                entity_type=e.entity_type,
                confidence=e.confidence,
                aliases=e.aliases,
            )
            for e in entities
        ],
        relations=[
            RelationResponse(
                subject=r.subject.normalized_text,
                predicate=r.normalized_predicate,
                object_value=r.object_value,
                confidence=r.confidence,
                source_text=r.source_text,
            )
            for r in relations
        ],
        claims=[
            ClaimResponse(
                id=c.id,
                subject_entity_id=c.subject_entity_id,
                subject_name=c.subject_name,
                predicate=c.predicate,
                object_value=c.object_value,
                source_id=request.source_id,
                confidence=c.confidence,
                source_text=c.source_text,
            )
            for c in claims
        ],
        extraction_time_ms=extraction_time,
    )


async def consume_parsed_documents() -> None:
    """Consume parsed documents and publish extracted claim events."""
    if not kafka_consumer:
        return

    async for message in kafka_consumer.consume():
        try:
            event = message.value
            if isinstance(event, str):
                event = json.loads(event)

            document_id = event.get("document_id")
            raw_text = event.get("raw_text")
            if not document_id or not raw_text:
                logger.warning(
                    "Skipping parsed document event without document_id/raw_text"
                )
                continue

            await run_extraction(
                ExtractionRequest(
                    document_id=document_id,
                    text=raw_text,
                    source_id=event.get("source_id") or document_id,
                )
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception("Failed to extract parsed document event: %s", e)


@app.post("/extract/entities", response_model=List[EntityResponse])
async def extract_entities(text: str):
    """Extract named entities only."""
    if not entity_extractor:
        raise HTTPException(status_code=503, detail="Entity extractor not initialized")

    entities = entity_extractor.extract(text)

    return [
        EntityResponse(
            text=e.text,
            normalized_text=e.normalized_text,
            entity_type=e.entity_type,
            confidence=e.confidence,
            aliases=e.aliases,
        )
        for e in entities
    ]


@app.post("/extract/relations", response_model=List[RelationResponse])
async def extract_relations(text: str):
    """Extract relations from text."""
    if not entity_extractor or not relation_extractor:
        raise HTTPException(status_code=503, detail="Extractors not initialized")

    entities = entity_extractor.extract(text)
    relations = relation_extractor.extract(text, entities)

    return [
        RelationResponse(
            subject=r.subject.normalized_text,
            predicate=r.normalized_predicate,
            object_value=r.object_value,
            confidence=r.confidence,
            source_text=r.source_text,
        )
        for r in relations
    ]


@app.post("/embed", response_model=List[EmbeddingResponse])
async def generate_embeddings(request: EmbedRequest):
    """Generate embeddings for texts."""
    if not embedder:
        raise HTTPException(status_code=503, detail="Embedder not initialized")

    try:
        results = embedder.embed_batch(request.texts)
    except ImportError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return [
        EmbeddingResponse(text=r.text, embedding=r.embedding, dimension=r.dimension)
        for r in results
    ]


@app.post("/embed/claim")
async def embed_claim(subject: str, predicate: str, object_value: str):
    """Generate embedding for a claim triple."""
    if not embedder:
        raise HTTPException(status_code=503, detail="Embedder not initialized")

    try:
        result = embedder.embed_claim(subject, predicate, object_value)
    except ImportError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return EmbeddingResponse(
        text=result.text, embedding=result.embedding, dimension=result.dimension
    )


@app.get("/vocabulary")
async def get_vocabulary():
    """Get the standard predicate vocabulary."""
    from services.extractor.vocabulary import (
        PREDICATE_VOCABULARY,
        PredicateCategory,
        get_predicates_by_category,
    )

    return {
        "predicates": list(PREDICATE_VOCABULARY.keys()),
        "by_category": {
            cat.value: get_predicates_by_category(cat) for cat in PredicateCategory
        },
    }
