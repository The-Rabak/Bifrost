FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.9.4 /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev
COPY bifrost ./bifrost
RUN useradd --uid 10001 --create-home app
USER app
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
CMD ["uvicorn", "bifrost.api:app", "--host", "0.0.0.0", "--port", "8000", "--timeout-graceful-shutdown", "210"]
