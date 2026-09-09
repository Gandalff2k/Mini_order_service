.PHONY: up down logs ps build test lint format typecheck migrate revision

up:
	docker compose up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f api outbox-worker consumer

ps:
	docker compose ps

build:
	docker compose build

test:
	pytest

lint:
	ruff check . && ruff format --check .

format:
	ruff format . && ruff check --fix .

typecheck:
	mypy app tests

migrate:
	docker compose run --rm migrator

revision:
	docker compose run --rm migrator alembic revision --autogenerate -m "$(m)"
