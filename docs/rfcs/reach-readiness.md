# RFC: Reach Readiness

## What

Make the first DocWeave path reliable for new users:

```text
fresh clone -> make dev -> upload sample document -> claims reach graph-updater -> graph is queryable
```

## Why

The graph-updater package has public usage, but the repo had avoidable adoption blockers: an empty dashboard gitlink, mismatched docs, a non-importable graph-updater folder name, and Kafka consumers that started without processing messages.

## Decisions

- Use `services/graph_updater` as the Python package path while keeping external names as `graph-updater`.
- Keep v1 API-first; do not document a dashboard until dashboard source exists in the repo.
- Use dictionaries as Kafka payloads end to end.
- Let graph-updater upsert source/entity records when claims arrive, so claims do not disappear because setup data is missing.
- Add `make dev`, `make health`, `make demo`, and `make test` as the beginner and developer command surface.

## Alternatives Rejected

- Recreating the dashboard now: this would expand scope and still leave the pipeline unreliable.
- Keeping `services/graph-updater` and adding import shims: that preserves the import problem and adds compatibility code.
- Documentation-only fixes: users would still hit broken event flow after upload.

## Test Cases

- Python import/syntax checks pass after the package rename.
- Graph-updater tests run from `services/graph_updater/tests`.
- Docker Compose health endpoints respond on ports 8001-8005.
- `make demo` uploads the Acme sample and graph-updater search returns entities.
