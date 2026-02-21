"""Ingestion service - handles document intake and routing to processing pipeline."""
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional, List

from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from shared.config.settings import settings
from shared.utils.kafka_client import KafkaProducer
from shared.utils.neo4j_client import Neo4jClient
from services.ingestion.storage import DocumentStorage
from services.ingestion.processor import DocumentProcessor


class HealthResponse(BaseModel):
    status: str
    service: str
    timestamp: str


class IngestResponse(BaseModel):
    document_id: str
    status: str
    message: str
    is_new: bool = True


class DocumentInfo(BaseModel):
    id: str
    filename: str
    content_type: str
    size_bytes: int
    created_at: str
    source_connector: str


kafka_producer: Optional[KafkaProducer] = None
neo4j_client: Optional[Neo4jClient] = None
storage: Optional[DocumentStorage] = None
processor: Optional[DocumentProcessor] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage service lifecycle."""
    global kafka_producer, neo4j_client, storage, processor

    # Initialize storage
    storage_path = os.environ.get("STORAGE_PATH", "/tmp/docweave/documents")
    storage = DocumentStorage(storage_path)

    # Initialize Kafka producer
    kafka_producer = KafkaProducer()
    await kafka_producer.start()

    # Initialize processor
    processor = DocumentProcessor(storage, kafka_producer)

    # Initialize Neo4j (optional, for metadata queries)
    try:
        neo4j_client = Neo4jClient()
    except Exception:
        neo4j_client = None

    yield

    # Cleanup
    await kafka_producer.stop()
    if neo4j_client:
        neo4j_client.close()


app = FastAPI(
    title="DocWeave Ingestion Service",
    description="Handles document intake, deduplication, and routing",
    version="0.2.0",
    lifespan=lifespan
)


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Basic health check endpoint."""
    return HealthResponse(
        status="healthy",
        service="ingestion",
        timestamp=datetime.utcnow().isoformat()
    )


@app.get("/ready")
async def readiness_check():
    """Check if service dependencies are ready."""
    checks = {"storage": False, "kafka": False}

    try:
        checks["storage"] = storage is not None and storage.base_path.exists()
    except Exception:
        pass

    try:
        checks["kafka"] = kafka_producer is not None
    except Exception:
        pass

    all_ready = all(checks.values())
    return {
        "ready": all_ready,
        "checks": checks,
        "timestamp": datetime.utcnow().isoformat()
    }


@app.post("/ingest", response_model=IngestResponse)
async def ingest_document(
    file: UploadFile = File(...),
    source: str = Query("api_upload", description="Source identifier")
):
    """
    Ingest a document into the processing pipeline.

    The document will be:
    1. Validated for size and content type
    2. Stored with deduplication (duplicates are detected by content hash)
    3. Queued for processing via Kafka

    Returns document ID and processing status.
    """
    if not processor:
        raise HTTPException(status_code=503, detail="Service not ready")

    if not file.filename:
        raise HTTPException(status_code=400, detail="Filename required")

    content = await file.read()
    if len(content) == 0:
        raise HTTPException(status_code=400, detail="Empty file")

    result = await processor.process(
        content=content,
        filename=file.filename,
        content_type=file.content_type,
        source_connector=source,
        metadata={"original_filename": file.filename}
    )

    if result.status == "rejected":
        raise HTTPException(status_code=400, detail=result.message)
    if result.status == "error":
        raise HTTPException(status_code=500, detail=result.message)

    return IngestResponse(
        document_id=result.document_id,
        status=result.status,
        message=result.message,
        is_new=result.is_new
    )


@app.post("/ingest/batch", response_model=List[IngestResponse])
async def ingest_batch(files: List[UploadFile] = File(...)):
    """Ingest multiple documents in a single request."""
    if not processor:
        raise HTTPException(status_code=503, detail="Service not ready")

    results = []
    for file in files:
        if not file.filename:
            results.append(IngestResponse(
                document_id="",
                status="rejected",
                message="Filename required",
                is_new=False
            ))
            continue

        content = await file.read()
        result = await processor.process(
            content=content,
            filename=file.filename,
            content_type=file.content_type,
            source_connector="api_batch"
        )

        results.append(IngestResponse(
            document_id=result.document_id,
            status=result.status,
            message=result.message,
            is_new=result.is_new
        ))

    return results


@app.get("/documents", response_model=List[DocumentInfo])
async def list_documents(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0)
):
    """List ingested documents with pagination."""
    if not storage:
        raise HTTPException(status_code=503, detail="Service not ready")

    docs = storage.list_documents(limit=limit, offset=offset)
    return [
        DocumentInfo(
            id=doc.id,
            filename=doc.filename,
            content_type=doc.content_type,
            size_bytes=doc.size_bytes,
            created_at=doc.created_at,
            source_connector=doc.source_connector
        )
        for doc in docs
    ]


@app.get("/documents/{doc_id}")
async def get_document(doc_id: str):
    """Get document metadata."""
    if not storage:
        raise HTTPException(status_code=503, detail="Service not ready")

    result = storage.get(doc_id)
    if not result:
        raise HTTPException(status_code=404, detail="Document not found")

    doc, _ = result
    return {
        "id": doc.id,
        "filename": doc.filename,
        "content_type": doc.content_type,
        "size_bytes": doc.size_bytes,
        "content_hash": doc.content_hash,
        "created_at": doc.created_at,
        "source_connector": doc.source_connector,
        "metadata": doc.metadata
    }


@app.get("/documents/{doc_id}/content")
async def get_document_content(doc_id: str):
    """Download document content."""
    if not storage:
        raise HTTPException(status_code=503, detail="Service not ready")

    result = storage.get(doc_id)
    if not result:
        raise HTTPException(status_code=404, detail="Document not found")

    doc, content = result

    return StreamingResponse(
        iter([content]),
        media_type=doc.content_type,
        headers={
            "Content-Disposition": f'attachment; filename="{doc.filename}"'
        }
    )


@app.post("/documents/{doc_id}/reprocess")
async def reprocess_document(doc_id: str):
    """Requeue a document for processing."""
    if not processor:
        raise HTTPException(status_code=503, detail="Service not ready")

    result = await processor.reprocess(doc_id)
    if result.status == "error":
        raise HTTPException(status_code=404, detail=result.message)

    return {"status": result.status, "message": result.message}


@app.delete("/documents/{doc_id}")
async def delete_document(doc_id: str):
    """Delete a document from storage."""
    if not storage:
        raise HTTPException(status_code=503, detail="Service not ready")

    deleted = storage.delete(doc_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Document not found")

    return {"status": "deleted", "document_id": doc_id}
