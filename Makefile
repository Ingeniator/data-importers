.PHONY: dev test e2e lint

dev:
	uv run uvicorn dataimporter.main:app --reload --port 5001

test:
	uv run pytest tests/ --ignore=tests/e2e -v

e2e:
	uv run pytest tests/e2e/ -v

lint:
	uv run ruff check src/ tests/
