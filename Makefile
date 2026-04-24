UV ?= uv
MANAGEMENT_TOKEN ?= example-management-token

.PHONY: install sync test run mock up up-server down logs health lock

install:
	$(UV) sync --all-groups

sync:
	$(UV) sync --all-groups

lock:
	$(UV) lock

test:
	$(UV) run pytest

run:
	OPC_CONFIG_FILE=examples/config.edge.yaml $(UV) run uvicorn src.main:app --host 0.0.0.0 --port 8080

mock:
	$(UV) run python examples/mock_opcua_server.py

up:
	docker compose -f docker-compose.yaml up --build -d

up-server:
	docker compose -f docker-compose.server.yml up --build -d

down:
	docker compose -f docker-compose.yaml down --remove-orphans

logs:
	docker compose -f docker-compose.yaml logs -f opcua-client

health:
	curl -s -H 'Authorization: Bearer $(MANAGEMENT_TOKEN)' http://127.0.0.1:8080/health
