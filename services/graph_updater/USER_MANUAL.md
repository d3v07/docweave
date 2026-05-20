# DocWeave Graph Updater User Manual

Graph Updater is the DocWeave service that stores extracted claims, detects contradictions, and exposes the current truth for an entity.

It does not read documents by itself. In the full DocWeave flow, ingestion, parser, and extractor services prepare claims, then graph-updater writes those claims into Neo4j.

## Basic Vocabulary

| Word | Meaning | Example |
| --- | --- | --- |
| Entity | A thing the graph knows about | `Acme Corporation` |
| Claim | A fact about an entity | `Acme headquartered_in San Francisco` |
| Predicate | The claim's relationship name | `headquartered_in` |
| Source | Where the claim came from | `annual_report_2024` |
| Confidence | How sure the extractor is | `0.95` |
| Conflict | Two claims disagree | `employee_count 15000` vs `14500` |
| Truth | The best current value after conflict handling | `employee_count 15000` |
| Dossier | Entity truth plus evidence, lineage, quality, and next checks | `GET /entity/{id}/dossier` |

## Run Locally

From the repo root:

```bash
make dev
make health
```

Graph Updater runs on:

```text
http://localhost:8004
```

Open the interactive API docs:

```text
http://localhost:8004/docs
```

## Use The Whole Pipeline

Upload a sample document:

```bash
curl -X POST \
  -F "file=@data/samples/acme_press_release.txt;type=text/plain" \
  http://localhost:8001/ingest
```

Wait a few seconds, then search for entities:

```bash
curl "http://localhost:8004/entity/search?query=Acme&limit=5"
```

## Use Graph Updater Directly

Create an entity:

```bash
curl -X POST \
  "http://localhost:8004/entity?name=Acme%20Corporation&entity_type=ORGANIZATION"
```

Add a claim:

```bash
curl -X POST http://localhost:8004/claim \
  -H "Content-Type: application/json" \
  -d '{
    "subject_entity_id": "ent_acme",
    "predicate": "headquartered_in",
    "object_value": "San Francisco",
    "source_id": "src_demo",
    "confidence": 0.95
  }'
```

Read current truth:

```bash
curl http://localhost:8004/entity/ent_acme/truth
```

Read the evidence dossier:

```bash
curl http://localhost:8004/entity/ent_acme/dossier
```

Use the dossier when you need to explain an answer. It includes current truth, supporting claims, source lineage, conflicts, a timeline, quality signals, and suggested follow-up checks.

Check source impact:

```bash
curl http://localhost:8004/source/src_demo/impact
```

Use source impact when you need to know what would be affected if a source is corrected or removed.

Create a conflict by adding a different value for the same entity and predicate:

```bash
curl -X POST http://localhost:8004/claim \
  -H "Content-Type: application/json" \
  -d '{
    "subject_entity_id": "ent_acme",
    "predicate": "headquartered_in",
    "object_value": "Oakland",
    "source_id": "src_other",
    "confidence": 0.70
  }'
```

List unresolved conflicts:

```bash
curl http://localhost:8004/conflicts
```

## Admin Endpoints

Admin endpoints use `X-API-Key` when `GRAPH_UPDATER_ADMIN_API_KEY` is configured.

Example:

```bash
curl http://localhost:8004/admin/stats \
  -H "X-API-Key: $GRAPH_UPDATER_ADMIN_API_KEY"
```

## Container Image

Pull the public image:

```bash
docker pull ghcr.io/d3v07/docweave/graph-updater:latest
```

Build locally:

```bash
make graph-updater-image
```

## Troubleshooting

Check health:

```bash
curl http://localhost:8004/health
curl http://localhost:8004/ready
```

Check logs:

```bash
docker compose logs -f graph-updater-service
```

Restart local services:

```bash
docker compose restart graph-updater-service parser-service extractor-service
```

Stop without deleting data:

```bash
make down
```
