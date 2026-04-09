.PHONY: dev test lint

dev:
	uv run uvicorn dataimporter.main:app --reload --port 5001

test:
	uv run pytest -v

lint:
	uv run ruff check src/ tests/
