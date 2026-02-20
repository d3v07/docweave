# DocWeave

Microservices platform for ingesting documents, extracting structured knowledge, and building a continuously updated knowledge graph. Documents flow through a parsing and extraction pipeline via Kafka, with all relationships stored in Neo4j.

## Architecture

```
Document Upload
      │
 Ingestion Service (port 8001)
      │ [Kafka: document-events]
 Parser Service (port 8002)
      │ [Kafka: parsed-documents]
 Extractor Service (port 8003)
      │ [Kafka: extracted-claims + graph-updates]
 Graph Updater Service (port 8004)
      │
    Neo4j
      │
 Query Service (port 8005) ← REST/GraphQL API
```

## Services

| Service | Port | Responsibility |
|---------|------|---------------|
| `ingestion` | 8001 | Accepts document uploads, publishes to `document-events` topic |
| `parser` | 8002 | Consumes raw documents, extracts text and structure |
| `extractor` | 8003 | Identifies claims, entities, and relationships from parsed content |
| `graph-updater` | 8004 | Writes extracted knowledge into Neo4j |
| `query-service` | 8005 | Exposes REST API for querying the knowledge graph |

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Services | Python, FastAPI |
| Message Broker | Apache Kafka (Confluent) |
| Graph Database | Neo4j 5 Enterprise (with APOC) |
| Orchestration | Docker Compose |
| Coordination | Apache ZooKeeper |

## Getting Started

### Prerequisites

- Docker and Docker Compose
- `NEO4J_PASSWORD` set in your environment or `.env`

### Run All Services

```bash
cp .env.example .env
# fill in NEO4J_PASSWORD

docker-compose up
```

Kafka topics are created automatically by the `kafka-init` service on first startup.

### Access

| Endpoint | URL |
|----------|-----|
| Neo4j Browser | http://localhost:7474 |
| Ingestion API | http://localhost:8001 |
| Parser API | http://localhost:8002 |
| Extractor API | http://localhost:8003 |
| Graph Updater API | http://localhost:8004 |
| Query API | http://localhost:8005 |

## Kafka Topics

| Topic | Producer | Consumer |
|-------|----------|---------|
| `document-events` | ingestion | parser |
| `parsed-documents` | parser | extractor |
| `extracted-claims` | extractor | graph-updater |
| `graph-updates` | extractor | graph-updater |
| `conflicts` | graph-updater | — |

## Environment Variables

```
NEO4J_PASSWORD=your_password
```

See `.env.example` for the full list.

## License

MIT
