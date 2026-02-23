"""Query service - handles search and natural language queries."""
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional, List, Any

from fastapi import FastAPI, Query
from pydantic import BaseModel

from shared.utils.neo4j_client import Neo4jClient


class HealthResponse(BaseModel):
    status: str
    service: str
    timestamp: str


class EntityResult(BaseModel):
    id: str
    name: str
    type: str
    claims: List[dict] = []


class SearchResult(BaseModel):
    entities: List[EntityResult]
    total: int
    query_time_ms: float


class AnswerResult(BaseModel):
    answer: str
    confidence: float
    sources: List[dict]
    supporting_claims: List[dict]


neo4j_client: Optional[Neo4jClient] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global neo4j_client
    neo4j_client = Neo4jClient()
    yield
    neo4j_client.close()


app = FastAPI(
    title="DocWeave Query Service",
    description="Search and natural language query interface",
    version="0.1.0",
    lifespan=lifespan
)


@app.get("/health", response_model=HealthResponse)
async def health_check():
    return HealthResponse(
        status="healthy",
        service="query-service",
        timestamp=datetime.utcnow().isoformat()
    )


@app.get("/ready")
async def readiness_check():
    neo4j_ok = neo4j_client.health_check() if neo4j_client else False
    return {
        "ready": neo4j_ok,
        "checks": {"neo4j": neo4j_ok},
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/search", response_model=SearchResult)
async def search_entities(
    q: str = Query(..., min_length=1, description="Search query"),
    entity_type: Optional[str] = None,
    limit: int = Query(20, ge=1, le=100)
):
    """Search entities by name or alias."""
    import time
    start = time.time()

    query = """
    MATCH (e:Entity)
    WHERE e.name CONTAINS $search_term
       OR ANY(alias IN e.aliases WHERE alias CONTAINS $search_term)
    """
    if entity_type:
        query += " AND e.type = $entity_type"
    query += """
    OPTIONAL MATCH (e)-[:HAS_CLAIM]->(c:Claim {status: 'CURRENT'})
    RETURN e.id as id, e.name as name, e.type as type,
           collect({predicate: c.predicate, value: c.object_value}) as claims
    LIMIT $limit
    """

    params = {"search_term": q, "limit": limit}
    if entity_type:
        params["entity_type"] = entity_type

    results = neo4j_client.execute_query(query, **params) if neo4j_client else []

    entities = [
        EntityResult(
            id=r["id"],
            name=r["name"],
            type=r["type"],
            claims=[c for c in r["claims"] if c["predicate"]]
        )
        for r in results
    ]

    query_time = (time.time() - start) * 1000

    return SearchResult(
        entities=entities,
        total=len(entities),
        query_time_ms=query_time
    )


@app.get("/entity/{entity_id}")
async def get_entity(entity_id: str):
    """Get entity details with all current claims."""
    query = """
    MATCH (e:Entity {id: $entity_id})
    OPTIONAL MATCH (e)-[:HAS_CLAIM]->(c:Claim {status: 'CURRENT'})
    OPTIONAL MATCH (c)-[:SOURCED_FROM]->(s:Source)
    RETURN e.id as id, e.name as name, e.type as type, e.aliases as aliases,
           collect({
               claim_id: c.id,
               predicate: c.predicate,
               value: c.object_value,
               confidence: c.confidence,
               source_uri: s.uri
           }) as claims
    """

    results = neo4j_client.execute_query(query, entity_id=entity_id) if neo4j_client else []

    if not results:
        return {"error": "Entity not found"}

    r = results[0]
    return {
        "id": r["id"],
        "name": r["name"],
        "type": r["type"],
        "aliases": r["aliases"] or [],
        "claims": [c for c in r["claims"] if c["claim_id"]]
    }


@app.get("/ask", response_model=AnswerResult)
async def ask_question(q: str = Query(..., description="Natural language question")):
    """Answer a natural language question using the knowledge graph."""
    # Simple pattern matching for now
    # Full implementation would use NLP to parse the question

    # Extract potential entity names from the question
    words = q.replace("?", "").replace("'s", " ").split()

    # Search for entities
    entity_results = []
    for word in words:
        if len(word) > 2:
            query = """
            MATCH (e:Entity)
            WHERE toLower(e.name) CONTAINS toLower($term)
            OPTIONAL MATCH (e)-[:HAS_CLAIM]->(c:Claim {status: 'CURRENT'})
            OPTIONAL MATCH (c)-[:SOURCED_FROM]->(s:Source)
            RETURN e.name as entity, c.predicate as predicate,
                   c.object_value as value, c.confidence as confidence,
                   s.uri as source
            LIMIT 5
            """
            results = neo4j_client.execute_query(query, term=word) if neo4j_client else []
            entity_results.extend(results)

    if not entity_results:
        return AnswerResult(
            answer="I couldn't find relevant information to answer your question.",
            confidence=0.0,
            sources=[],
            supporting_claims=[]
        )

    # Build answer from claims
    claims = [r for r in entity_results if r.get("predicate")]
    sources = list({r["source"] for r in entity_results if r.get("source")})

    answer_parts = []
    for claim in claims[:3]:
        answer_parts.append(f"{claim['entity']} {claim['predicate']} {claim['value']}")

    return AnswerResult(
        answer=". ".join(answer_parts) + "." if answer_parts else "No specific answer found.",
        confidence=sum(c.get("confidence", 0) for c in claims) / max(len(claims), 1),
        sources=[{"uri": s} for s in sources],
        supporting_claims=claims
    )
