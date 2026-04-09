FROM python:3.13-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src/ src/
RUN uv sync --frozen --no-dev --no-editable

FROM python:3.13-slim

WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY config.yaml .
COPY entrypoint.py .

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 5001

CMD ["python", "-m", "entrypoint"]
