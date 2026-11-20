.PHONY: install lint typecheck test integration check up down logs psql clean

install:
	pip install -e ".[dev]"

lint:
	ruff check .
	ruff format --check .

typecheck:
	mypy

test:
	pytest -m "not integration"

integration:
	pytest -m integration --no-cov

check: lint typecheck test

# simulated (six simulated stations) or meteofrance (the real stations, via the bridge).
STATION_SOURCE ?= simulated

up:
	STATION_SOURCE=$(STATION_SOURCE) docker compose --profile $(STATION_SOURCE) up -d --build

down:
	docker compose --profile simulated --profile meteofrance down

logs:
	docker compose logs -f ingest simulator bridge

psql:
	docker compose exec db psql -U sensors

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage
