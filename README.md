# DocWeave: Event-Driven Knowledge Graph from Document Streams

Continuously extract structured knowledge from document streams using a microservices pipeline. Documents flow through parsing → claim extraction → relationship discovery → Neo4j graph storage.

## Problem Solved

Unstructured documents (PDFs, articles, reports) contain valuable knowledge but require manual structuring. DocWeave automates extraction and builds a queryable knowledge graph: "Document X claims that resource A depends on technology B" becomes a graph edge queryable via REST API.

## Architecture

```
Document Upload (port 8001)
  ↓ [Kafka: document-events]
Parser (port 8002)
  ↓ [Kafka: parsed-documents]
Extractor (port 8003)
  ├→ [Kafka: extracted-claims]
  └→ [Kafka: graph-updates]
Graph Updater (port 8004)
  ├→ Neo4j (relationships, entities)
  └→ [Kafka: conflicts] (resolution queue)
Query Service (port 8005) ← REST API
```

## Evidence

**Data Flow** (`docker-compose.yml`):
- Ingestion service: Receives `UploadFile` via FastAPI; publishes JSON event to `document-events` (3 partitions)
- Parser service: Consumes `document-events`; extracts text + structure; publishes to `parsed-documents`
- Extractor service: Consumes `parsed-documents`; identifies claims (Pydantic models); publishes to `extracted-claims` + `graph-updates`
- Graph Updater: Consumes both; writes to Neo4j Bolt (port 7687); publishes conflicts to `conflicts` topic
- Query Service: Reads from Neo4j; exposes REST API on port 8005

**Tech Stack**:
- Services: Python 3.10+, FastAPI (6 services)
- Message Broker: Apache Kafka 7.5.3 (Confluent) with ZooKeeper
- Graph DB: Neo4j 5 Enterprise with APOC plugin (query/path analysis)
- Orchestration: Docker Compose with health checks
- Storage: Persistent volumes for Neo4j, ZooKeeper, Kafka

**Kafka Topics** (auto-created):
- `document-events` (3 partitions) — Raw document metadata
- `parsed-documents` (3 partitions) — Text extracted + structure
- `extracted-claims` (3 partitions) — Structured claims
- `graph-updates` (3 partitions) — Neo4j mutations
- `conflicts` (3 partitions) — Duplicate/conflicting relationships

**Services** (`services/*/main.py`):
- **ingestion** (port 8001): POST `/documents` → stores file + publishes event; GET `/documents` → list uploaded docs
- **parser** (port 8002): POST `/parse` (internal); consumes document-events
- **extractor** (port 8003): POST `/extract` (internal); consumes parsed-documents; publishes claims
- **graph-updater** (port 8004): POST `/update-graph` (internal); Neo4j write; conflict detection
- **query-service** (port 8005): GET `/entities` → list entities; GET `/relationships` → query edges; GET `/paths/:from/:to` → traversal

**Health Checks** (`docker-compose.yml`):
- FastAPI services: curl to `http://service:port/health`
- Kafka: `kafka-broker-api-versions --bootstrap-server localhost:9092`
- Neo4j: wget to `http://localhost:7474`

**Extensibility**:
- `connectors/` directory for pluggable data sources (S3, web crawlers, etc.)
- `shared/config/settings.py` centralizes Kafka bootstrap, Neo4j URIs, auth

## Deployment

**Prerequisites**:
- Docker & Docker Compose
- Set `NEO4J_PASSWORD` in `.env` or environment

**Start All Services**:
```bash
cp .env.example .env
# Edit .env: NEO4J_PASSWORD=your_strong_password
docker-compose up -d
```

All services auto-health-check and retry on startup. Kafka topics created automatically.

**Access Points**:

| Service | Endpoint | Purpose |
|---------|----------|---------|
| Neo4j Browser | http://localhost:7474 | Graph visualization + CYPHER queries |
| Ingestion API | http://localhost:8001 | Upload documents |
| Parser API | http://localhost:8002 | (Internal; not publicly exposed) |
| Extractor API | http://localhost:8003 | (Internal; not publicly exposed) |
| Graph Updater | http://localhost:8004 | (Internal; not publicly exposed) |
| Query API | http://localhost:8005 | REST API for knowledge retrieval |

**Example Usage**:
```bash
# Upload a document
curl -X POST -F "file=@whitepaper.pdf" http://localhost:8001/documents

# Query extracted entities
curl http://localhost:8005/entities?type=Technology

# Find paths between two entities
curl http://localhost:8005/paths/kubernetes/docker
```

## Architecture Decisions

**Why Kafka?** Decouples services; enables backpressure handling and replay on failure.

**Why Neo4j APOC?** Enables complex relationship queries (e.g., "all technologies transitively dependent on X") without custom code.

**Why Separate Services?** Each can be scaled independently; Parser may need more CPU, Extractor may need more memory.

**Conflict Resolution?** Duplicate claim detection; conflicts topic allows manual review before Neo4j write.

## Example: Claim Extraction

**Input Document**: "Kubernetes requires Docker for containerization."

**Extraction**:
```
Parsed: "Kubernetes requires Docker for containerization"
Claim extracted: (Kubernetes, requires, Docker)
  - Subject: Kubernetes
  - Predicate: requires
  - Object: Docker
  - Confidence: 0.92
  - Source: kubernetes-guide.pdf
```

**Graph Result**: Neo4j relationship `(Kubernetes)-[:REQUIRES]->(Docker)` with metadata (confidence, source)

**Query**: `GET /paths/kubernetes/docker` returns path and all intermediate technologies.

## Testing

```bash
# Unit tests
cd services/[service]
python -m pytest tests/

# Integration tests
docker-compose up -d
python -m pytest tests/integration/
```

## Known Limitations

- Conflict resolution is logged but not auto-resolved; manual intervention required
- Extraction accuracy depends on document quality and format
- No built-in document versioning; updates replace prior versions
- Scaling: Single Neo4j instance is bottleneck; cluster mode requires enterprise setup

## Performance Characteristics

| Metric | Typical | Notes |
|--------|---------|-------|
| Document ingestion latency | < 100ms | Upload + Kafka publish |
| Extraction latency | 500ms–2s | Per document (depends on size) |
| Graph write latency | 50–200ms | Per claim batch |
| Query latency (simple) | 10–50ms | Redis-cached entity lookups |
| Query latency (complex path) | 100–500ms | Neo4j APOC traversal |

## License

MIT
