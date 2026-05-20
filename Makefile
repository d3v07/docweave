SHELL := /bin/sh
PYTHON ?= python3

.PHONY: dev health demo test graph-updater-image down

dev:
	@test -f .env || cp .env.example .env
	docker compose up -d --build

health:
	curl -fsS http://localhost:8001/health
	curl -fsS http://localhost:8002/health
	curl -fsS http://localhost:8003/health
	curl -fsS http://localhost:8004/health
	curl -fsS http://localhost:8005/health

demo:
	response=$$(curl -fsS -X POST -F "file=@data/samples/acme_press_release.txt;type=text/plain" http://localhost:8001/ingest); \
	echo "$$response"; \
	doc_id=$$(printf '%s' "$$response" | $(PYTHON) -c 'import json, sys; print(json.load(sys.stdin)["document_id"])'); \
	curl -fsS -X POST "http://localhost:8001/documents/$$doc_id/reprocess"
	sleep 12
	curl -fsS "http://localhost:8004/entity/search?query=Acme&limit=5"
	curl -fsS "http://localhost:8004/entity/ent_organization_acme_corporation/truth"

test:
	$(PYTHON) -m compileall -q shared services scripts
	$(PYTHON) -m pytest services/graph_updater/tests

graph-updater-image:
	docker build -f services/graph_updater/Dockerfile -t ghcr.io/d3v07/docweave/graph-updater:local .

down:
	docker compose down
