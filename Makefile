SHELL := /bin/sh
PYTHON ?= python3
ENTITY ?= ent_organization_acme_corporation
SOURCE ?=

.PHONY: dev health demo dossier story source-impact test graph-updater-image down

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

dossier:
	curl -fsS "http://localhost:8004/entity/$(ENTITY)/dossier" | $(PYTHON) -m json.tool

story:
	@set -e; \
	impact_doc_id=""; \
	for sample in data/samples/acme_press_release.txt data/samples/techstart_acquisition.txt data/samples/product_specs.json; do \
		mime="text/plain"; \
		case "$$sample" in *.json) mime="application/json" ;; esac; \
		response=$$(curl -fsS -X POST -F "file=@$$sample;type=$$mime" http://localhost:8001/ingest); \
		echo "$$sample -> $$response"; \
		doc_id=$$(printf '%s' "$$response" | $(PYTHON) -c 'import json, sys; print(json.load(sys.stdin)["document_id"])'); \
		if [ -z "$$impact_doc_id" ]; then impact_doc_id="$$doc_id"; fi; \
		curl -fsS -X POST "http://localhost:8001/documents/$$doc_id/reprocess" >/dev/null; \
	done; \
	sleep 15; \
	echo "== Entity dossier =="; \
	curl -fsS "http://localhost:8004/entity/$(ENTITY)/dossier" | $(PYTHON) -m json.tool; \
	echo "== Source impact =="; \
	curl -fsS "http://localhost:8004/source/$$impact_doc_id/impact" | $(PYTHON) -m json.tool

source-impact:
	@test -n "$(SOURCE)" || (echo "Usage: make source-impact SOURCE=doc_..." >&2; exit 2)
	curl -fsS "http://localhost:8004/source/$(SOURCE)/impact" | $(PYTHON) -m json.tool

test:
	$(PYTHON) -m compileall -q shared services scripts
	$(PYTHON) -m pytest services/graph_updater/tests

graph-updater-image:
	docker build -f services/graph_updater/Dockerfile -t ghcr.io/d3v07/docweave/graph-updater:local .

down:
	docker compose down
