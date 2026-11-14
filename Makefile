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

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f ingest simulator

psql:
	docker compose exec db psql -U sensors

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage
