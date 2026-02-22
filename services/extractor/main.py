"""Extractor service - NLP extraction pipeline for entities, relations, and claims."""
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from shared.config.settings import settings
from shared.utils.kafka_client import KafkaProducer, KafkaConsumer


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
entity_extractor = None
relation_extractor = None
claim_generator = None
embedder = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize extraction components."""
    global kafka_producer, kafka_consumer
    global entity_extractor, relation_extractor, claim_generator, embedder

    # Import here to avoid loading models at module import
    from services.extractor.extraction import EntityExtractor, RelationExtractor, ClaimGenerator
    from services.extractor.embedding import Embedder

    # Initialize extractors
    entity_extractor = EntityExtractor()
    relation_extractor = RelationExtractor()
    claim_generator = ClaimGenerator()

    # Initialize embedder (lazy loaded)
    embedder = Embedder()

    # Initialize Kafka
    kafka_producer = KafkaProducer()
    kafka_consumer = KafkaConsumer(
        topics=[settings.KAFKA_TOPIC_PARSED_DOCUMENTS],
        group_id="extractor-service"
    )
    await kafka_producer.start()
    await kafka_consumer.start()

    yield

    await kafka_producer.stop()
    await kafka_consumer.stop()


app = FastAPI(
    title="DocWeave Extractor Service",
    description="NLP extraction pipeline for entities, relations, and claims",
    version="0.2.0",
    lifespan=lifespan
)


@app.get("/health", response_model=HealthResponse)
async def health_check():
    return HealthResponse(
        status="healthy",
        service="extractor",
        timestamp=datetime.utcnow().isoformat()
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
        "timestamp": datetime.utcnow().isoformat()
    }


@app.post("/extract", response_model=ExtractionResponse)
async def extract_all(request: ExtractionRequest):
    """
    Full extraction pipeline: entities -> relations -> claims.

    Extracts named entities, relations between them, and generates
    structured claims ready for the knowledge graph.
    """
    import time
    start = time.time()

    if not entity_extractor or not relation_extractor or not claim_generator:
        raise HTTPException(status_code=503, detail="Extractors not initialized")

    # Step 1: Extract entities
    entities = entity_extractor.extract(request.text)

    # Step 2: Extract relations
    relations = relation_extractor.extract(request.text, entities)

    # Step 3: Generate claims
    claims = claim_generator.generate(relations, request.source_id)

    extraction_time = (time.time() - start) * 1000

    # Publish claims to Kafka
    if kafka_producer and claims:
        import json
        for claim in claims:
            event = {
                "document_id": request.document_id,
                "claim_id": claim.id,
                "subject_entity_id": claim.subject_entity_id,
                "predicate": claim.predicate,
                "object_value": claim.object_value,
                "confidence": claim.confidence,
                "timestamp": datetime.utcnow().isoformat()
            }
            await kafka_producer.send(
                topic=settings.KAFKA_TOPIC_EXTRACTED_CLAIMS,
                value=json.dumps(event),
                key=request.document_id
            )

    return ExtractionResponse(
        document_id=request.document_id,
        entities=[
            EntityResponse(
                text=e.text,
                normalized_text=e.normalized_text,
                entity_type=e.entity_type,
                confidence=e.confidence,
                aliases=e.aliases
            )
            for e in entities
        ],
        relations=[
            RelationResponse(
                subject=r.subject.normalized_text,
                predicate=r.normalized_predicate,
                object_value=r.object_value,
                confidence=r.confidence,
                source_text=r.source_text
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
                confidence=c.confidence,
                source_text=c.source_text
            )
            for c in claims
        ],
        extraction_time_ms=extraction_time
    )


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
            aliases=e.aliases
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
            source_text=r.source_text
        )
        for r in relations
    ]


@app.post("/embed", response_model=List[EmbeddingResponse])
async def generate_embeddings(request: EmbedRequest):
    """Generate embeddings for texts."""
    if not embedder:
        raise HTTPException(status_code=503, detail="Embedder not initialized")

    results = embedder.embed_batch(request.texts)

    return [
        EmbeddingResponse(
            text=r.text,
            embedding=r.embedding,
            dimension=r.dimension
        )
        for r in results
    ]


@app.post("/embed/claim")
async def embed_claim(subject: str, predicate: str, object_value: str):
    """Generate embedding for a claim triple."""
    if not embedder:
        raise HTTPException(status_code=503, detail="Embedder not initialized")

    result = embedder.embed_claim(subject, predicate, object_value)

    return EmbeddingResponse(
        text=result.text,
        embedding=result.embedding,
        dimension=result.dimension
    )


@app.get("/vocabulary")
async def get_vocabulary():
    """Get the standard predicate vocabulary."""
    from services.extractor.vocabulary import PREDICATE_VOCABULARY, get_predicates_by_category, PredicateCategory

    return {
        "predicates": list(PREDICATE_VOCABULARY.keys()),
        "by_category": {
            cat.value: get_predicates_by_category(cat)
            for cat in PredicateCategory
        }
    }
